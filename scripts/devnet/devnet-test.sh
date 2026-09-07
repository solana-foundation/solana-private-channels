#!/usr/bin/env bash
# End-to-end devnet test for Solana Private Channels escrow system
#
# Required env vars:
#   DEVNET_RPC_URL          - Solana devnet RPC URL (e.g. https://api.devnet.solana.com)
#   ADMIN_PRIVATE_KEY       - Channel admin key, read from the .env sourced below
#
# Two separate admin keys, which must never be the same keypair:
#
#   ESCROW_ADMIN_KEYPAIR  Instance.admin on Solana. Signs CreateInstance, AddOperator
#                         and AllowMint here, plus SetNewAdmin in production. Needs SOL
#                         to pay for those three transactions. Never loaded by a service.
#   ADMIN_PRIVATE_KEY     The channel admin the containers run as: SPL mint+freeze
#                         authority on receipt mints, operator fee payer, and the wallet
#                         registered as the escrow Operator by step 2. Read from the same
#                         .env the containers load, so the Operator registered here is
#                         always the key they sign with.
#
# Sharing one keypair across both roles means SetNewAdmin does not revoke anything:
# the rotated-out key keeps its receipt-mint authority and its Operator PDA, and can
# still drain escrow custody afterwards.
#
# Optional env vars:
#   PRIVATE_CHANNEL_GATEWAY_URL      - Solana Private Channels gateway URL (default: http://localhost:8899)
#   ESCROW_ADMIN_KEYPAIR    - Path to escrow admin keypair (default: ./keypairs/escrow-admin.json)
#   MINT_KEYPAIR            - Path to mint keypair (default: ./keypairs/mint.json)
#   USER_KEYPAIR            - Path to user keypair (default: ./keypairs/user.json)

set -eo pipefail

# Source .env if it exists
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

RPC_URL="${DEVNET_RPC_URL:?DEVNET_RPC_URL is required}"
PRIVATE_CHANNEL_GATEWAY_URL="${PRIVATE_CHANNEL_GATEWAY_URL:-http://localhost:8899}"
ESCROW_ADMIN_KEYPAIR="${ESCROW_ADMIN_KEYPAIR:-./keypairs/escrow-admin.json}"
MINT_KEYPAIR="${MINT_KEYPAIR:-./keypairs/mint.json}"
USER_KEYPAIR="${USER_KEYPAIR:-./keypairs/user.json}"
: "${ADMIN_PRIVATE_KEY:?ADMIN_PRIVATE_KEY is required; \`make build-devnet\` writes it to .env}"

if [ ! -f "$ESCROW_ADMIN_KEYPAIR" ]; then
  echo "ERROR: escrow admin keypair not found: $ESCROW_ADMIN_KEYPAIR" >&2
  echo "       It must be a different keypair from the channel admin in ADMIN_PRIVATE_KEY." >&2
  echo "       Create one: solana-keygen new --no-bip39-passphrase -o $ESCROW_ADMIN_KEYPAIR" >&2
  echo "       Then fund it on devnet so it can pay for CreateInstance/AddOperator/AllowMint." >&2
  exit 1
fi

# Portable sed -i (macOS vs Linux)
sedi() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "$@"
  else
    sed -i "$@"
  fi
}

MINT=$(solana-keygen pubkey "$MINT_KEYPAIR")
ESCROW_ADMIN=$(solana-keygen pubkey "$ESCROW_ADMIN_KEYPAIR")
USER=$(solana-keygen pubkey "$USER_KEYPAIR")

# Derived from the secret itself, not from a keypair path: a path could name a key the
# containers never load, which would both defeat the check below and register an Operator
# that cannot sign ReleaseFunds.
if ! OPERATOR=$(printf '%s\n' "$ADMIN_PRIVATE_KEY" | solana-keygen pubkey -); then
  echo "ERROR: could not derive a pubkey from ADMIN_PRIVATE_KEY." >&2
  echo "       Expected the JSON byte array that \`make build-devnet\` writes to .env." >&2
  exit 1
fi

# Fail closed on the collapsed-key configuration this script used to ship with.
if [ "$ESCROW_ADMIN" = "$OPERATOR" ]; then
  echo "ERROR: the escrow admin and the channel admin are the same key ($ESCROW_ADMIN)." >&2
  echo "       SetNewAdmin cannot revoke a key that also holds receipt-mint authority" >&2
  echo "       and an Operator PDA. Use two distinct keypairs." >&2
  exit 1
fi

# Function to get token balance for a wallet (raw amount, no decimals)
get_token_balance() {
  local wallet=$1
  local mint=$2
  spl-token balance --owner "$wallet" "$mint" -u "$RPC_URL" --output json 2>/dev/null | jq -r '.amount' || echo "0"
}

echo "Escrow admin (Instance.admin): $ESCROW_ADMIN"
echo "Channel admin / operator: $OPERATOR"
echo "Mint: $MINT"
echo "User: $USER"

echo ""
echo "=== Step 0: Clean up old containers/volumes ==="
docker compose -f docker-compose.devnet.yml --env-file .env down -v > /dev/null 2>&1 || true
echo "Cleaned up old containers and volumes"

# Get initial balances
USER_BALANCE_BEFORE=$(get_token_balance "$USER" "$MINT")
echo "User token balance (before): $USER_BALANCE_BEFORE"

# Creates the instance and its withdrawal bitmap in one instruction. The bitmap
# is 8 KB, so the admin keypair needs about 0.058 SOL of rent beyond the usual.
echo "=== Step 1: Create Instance ==="
OUTPUT=$(cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin create_instance -- \
  "$RPC_URL" \
  "$ESCROW_ADMIN_KEYPAIR")
echo "$OUTPUT"

# Parse instance ID from output (line: escrow_instance_id = "...")
INSTANCE_ID=$(echo "$OUTPUT" | grep 'escrow_instance_id' | sed 's/.*"\(.*\)".*/\1/')
echo "Instance ID: $INSTANCE_ID"

BITMAP_PDA=$(echo "$OUTPUT" | grep 'Withdrawal bitmap PDA:' | awk '{print $NF}')
echo "Withdrawal bitmap PDA: $BITMAP_PDA"

# Parse transaction signature and get slot
TX_SIG=$(echo "$OUTPUT" | grep 'Transaction signature:' | awk '{print $NF}')
echo "Transaction: $TX_SIG"

# Get slot from transaction (format: "Transaction executed in slot 425338786:")
SLOT=$(solana confirm -u "$RPC_URL" "$TX_SIG" -v 2>/dev/null | grep 'Transaction executed in slot' | sed 's/.*slot \([0-9]*\).*/\1/')
echo "Slot: $SLOT"

echo ""
echo "=== Step 2: Add Operator ==="
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin add_operator -- \
  "$RPC_URL" \
  "$ESCROW_ADMIN_KEYPAIR" \
  "$INSTANCE_ID" \
  "$OPERATOR"

echo ""
echo "=== Step 3: Allow Mint ==="
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin allow_mint -- \
  "$RPC_URL" \
  "$ESCROW_ADMIN_KEYPAIR" \
  "$INSTANCE_ID" \
  "$MINT"

echo ""
echo "=== Step 4: Update .env ==="
sedi "s/^ESCROW_INSTANCE_ID=.*/ESCROW_INSTANCE_ID=$INSTANCE_ID/" .env
export ESCROW_INSTANCE_ID=$INSTANCE_ID
echo "Updated .env with ESCROW_INSTANCE_ID=$INSTANCE_ID"

echo ""
echo "=== Step 5: Update indexer config ==="
INDEXER_CONFIG="scripts/devnet/config/indexer-solana.toml"
sedi "s/^enabled = false/enabled = true/" "$INDEXER_CONFIG"
sedi "s/^start_slot = .*/start_slot = $SLOT/" "$INDEXER_CONFIG"
echo "Updated $INDEXER_CONFIG:"
echo "  - escrow_instance_id = $INSTANCE_ID"
echo "  - backfill enabled = true"
echo "  - start_slot = $SLOT"

echo ""
echo "=== Step 6: Start Docker Compose ==="
echo "Starting docker compose..."
docker compose -f docker-compose.devnet.yml --env-file .env up -d > /dev/null 2>&1

echo "Waiting for containers to be healthy..."
sleep 10

# Wait for containers to be ready (check for unhealthy, starting, or restarting)
for i in {1..30}; do
  if docker compose -f docker-compose.devnet.yml ps | grep -qiE "unhealthy|starting|restarting"; then
    echo "Waiting for containers... ($i/30)"
    sleep 2
  else
    echo "All containers ready!"
    break
  fi
done

# Get initial deposit count from database
INITIAL_DEPOSIT_COUNT=$(docker exec private-channel-postgres-indexer psql -U private_channel -d indexer -t -c "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'deposit' AND status = 'completed';" 2>/dev/null | tr -d ' \n' || echo "0")
EXPECTED_DEPOSIT_COUNT=$((INITIAL_DEPOSIT_COUNT + 2))
echo "Initial deposit count: $INITIAL_DEPOSIT_COUNT, expecting: $EXPECTED_DEPOSIT_COUNT after test"

echo ""
echo "=== Step 7: Test Deposits ==="

echo "Deposit 1: 50000 tokens"
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin deposit -- \
  "$RPC_URL" \
  "$USER_KEYPAIR" \
  "$INSTANCE_ID" \
  "$MINT" \
  50000

sleep 2

echo ""
echo "Deposit 2: 100000 tokens"
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin deposit -- \
  "$RPC_URL" \
  "$USER_KEYPAIR" \
  "$INSTANCE_ID" \
  "$MINT" \
  100000

# Get balances after deposits
USER_BALANCE_AFTER=$(get_token_balance "$USER" "$MINT")
INSTANCE_BALANCE_AFTER=$(get_token_balance "$INSTANCE_ID" "$MINT")

echo ""
echo "=== Step 8: Restart Containers for Backfill ==="
echo "Stopping containers (keeping db)..."
docker compose -f docker-compose.devnet.yml stop > /dev/null 2>&1

echo "Waiting for containers to stop..."
sleep 5

echo "Starting containers again..."
docker compose -f docker-compose.devnet.yml --env-file .env up -d > /dev/null 2>&1

echo "Waiting for containers to be healthy..."
sleep 10

for i in {1..30}; do
  if docker compose -f docker-compose.devnet.yml ps | grep -qiE "unhealthy|starting|restarting"; then
    echo "Waiting for containers... ($i/30)"
    sleep 2
  else
    echo "All containers ready!"
    break
  fi
done

echo ""
echo "=== Step 9: Validate Backfill ==="
echo "Waiting for backfill to complete..."

for i in {1..30}; do
  TX_COUNT=$(docker exec private-channel-postgres-indexer psql -U private_channel -d indexer -t -c "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'deposit' AND status = 'completed';" 2>/dev/null | tr -d ' \n')
  if [ "$TX_COUNT" -eq "$EXPECTED_DEPOSIT_COUNT" ]; then
    echo "✅ Backfill validated: $EXPECTED_DEPOSIT_COUNT completed deposits found (added 2 new)!"
    break
  else
    echo "Waiting for backfill... ($i/30) - found $TX_COUNT completed deposits"
    sleep 1
  fi
done

if [ "$TX_COUNT" -ne "$EXPECTED_DEPOSIT_COUNT" ]; then
  echo "❌ Backfill validation failed: expected $EXPECTED_DEPOSIT_COUNT completed deposits, got $TX_COUNT"
  exit 1
fi

echo ""
echo "=== Deposit Details ==="
docker exec private-channel-postgres-indexer psql -U private_channel -d indexer -c "SELECT id, signature, slot, initiator, amount, status, transaction_type FROM transactions WHERE transaction_type = 'deposit' ORDER BY slot;" 2>/dev/null

echo ""
echo "=== Step 10: Additional Deposits ==="

# Capture balances before additional deposits
USER_SOLANA_AFTER_BACKFILL=$(get_token_balance "$USER" "$MINT")
INSTANCE_SOLANA_AFTER_BACKFILL=$(get_token_balance "$INSTANCE_ID" "$MINT")

echo "Deposit 3: 75000 tokens"
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin deposit -- \
  "$RPC_URL" \
  "$USER_KEYPAIR" \
  "$INSTANCE_ID" \
  "$MINT" \
  75000

sleep 2

echo "Deposit 4: 125000 tokens"
cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin deposit -- \
  "$RPC_URL" \
  "$USER_KEYPAIR" \
  "$INSTANCE_ID" \
  "$MINT" \
  125000

sleep 5

# Capture balances after all deposits
USER_SOLANA_AFTER_ALL_DEPOSITS=$(get_token_balance "$USER" "$MINT")
INSTANCE_SOLANA_AFTER_ALL_DEPOSITS=$(get_token_balance "$INSTANCE_ID" "$MINT")

echo ""
echo "=== Step 11: Test Withdrawal ==="
# Withdraw sum of deposits 2-4: 100000 + 75000 + 125000 = 300000
WITHDRAW_AMOUNT=300000
echo "Withdrawing $WITHDRAW_AMOUNT tokens (sum of deposits 2-4)"

cargo run --quiet --manifest-path scripts/devnet/Cargo.toml --bin withdraw -- \
  "$PRIVATE_CHANNEL_GATEWAY_URL" \
  "$USER_KEYPAIR" \
  "$MINT" \
  "$WITHDRAW_AMOUNT"

# Wait for withdrawal to be processed on-chain
EXPECTED_INSTANCE_AFTER_WITHDRAW=$((INSTANCE_SOLANA_AFTER_ALL_DEPOSITS - WITHDRAW_AMOUNT))
echo "Waiting for on-chain withdrawal to complete..."
for i in {1..30}; do
  INSTANCE_BALANCE=$(get_token_balance "$INSTANCE_ID" "$MINT")
  if [ "$INSTANCE_BALANCE" -eq "$EXPECTED_INSTANCE_AFTER_WITHDRAW" ]; then
    echo "Withdrawal confirmed on-chain!"
    break
  fi
  echo "Waiting for withdrawal... ($i/30) - instance balance: $INSTANCE_BALANCE (expected: $EXPECTED_INSTANCE_AFTER_WITHDRAW)"
  sleep 2
done

echo ""
echo "=== Final Balances ==="
USER_SOLANA_FINAL=$(get_token_balance "$USER" "$MINT")
INSTANCE_SOLANA_FINAL=$(get_token_balance "$INSTANCE_ID" "$MINT")

# Get Solana Private Channels balance via RPC - need to derive ATA first (raw amount)
USER_ATA=$(spl-token address --verbose --owner "$USER" --token "$MINT" 2>/dev/null | grep "Associated token address" | awk '{print $4}')
USER_PRIVATE_CHANNEL_FINAL=$(curl -s -X POST "$PRIVATE_CHANNEL_GATEWAY_URL" \
  -H "Content-Type: application/json" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTokenAccountBalance\",\"params\":[\"$USER_ATA\"]}" \
  | jq -r '.result.value.amount // "0"')

echo ""
echo "=== Balance Evolution ==="
echo ""

# Calculate expected values (raw token amounts)
EXPECTED_USER_AFTER_DEP12=$((USER_BALANCE_BEFORE - 150000))
EXPECTED_INSTANCE_AFTER_DEP12=150000
EXPECTED_USER_AFTER_DEP34=$((EXPECTED_USER_AFTER_DEP12 - 200000))
EXPECTED_INSTANCE_AFTER_DEP34=350000
EXPECTED_USER_AFTER_WITHDRAW=$((EXPECTED_USER_AFTER_DEP34 + WITHDRAW_AMOUNT))
EXPECTED_INSTANCE_AFTER_WITHDRAW=$((EXPECTED_INSTANCE_AFTER_DEP34 - WITHDRAW_AMOUNT))
EXPECTED_PRIVATE_CHANNEL_AFTER_WITHDRAW=$((350000 - WITHDRAW_AMOUNT))

# Helper to check if values match
check() {
  if [ "$1" = "$2" ]; then echo "✅"; else echo "❌"; fi
}

printf "%-20s %20s %20s %12s %12s %s\n" "Stage" "User (Actual)" "User (Expect)" "Inst (Act)" "Inst (Exp)" ""
printf "%-20s %20s %20s %12s %12s %s\n" "-------------------" "--------------------" "--------------------" "------------" "------------" ""
printf "%-20s %20s %20s %12s %12s\n" "1. Initial" "$USER_BALANCE_BEFORE" "$USER_BALANCE_BEFORE" "-" "-"
printf "%-20s %20s %20s %12s %12s %s\n" "2. After dep 1-2" "$USER_BALANCE_AFTER" "$EXPECTED_USER_AFTER_DEP12" "$INSTANCE_BALANCE_AFTER" "$EXPECTED_INSTANCE_AFTER_DEP12" "$(check "$INSTANCE_BALANCE_AFTER" "$EXPECTED_INSTANCE_AFTER_DEP12")"
printf "%-20s %20s %20s %12s %12s\n" "3. After backfill" "$USER_SOLANA_AFTER_BACKFILL" "$EXPECTED_USER_AFTER_DEP12" "$INSTANCE_SOLANA_AFTER_BACKFILL" "$EXPECTED_INSTANCE_AFTER_DEP12"
printf "%-20s %20s %20s %12s %12s %s\n" "4. After dep 3-4" "$USER_SOLANA_AFTER_ALL_DEPOSITS" "$EXPECTED_USER_AFTER_DEP34" "$INSTANCE_SOLANA_AFTER_ALL_DEPOSITS" "$EXPECTED_INSTANCE_AFTER_DEP34" "$(check "$INSTANCE_SOLANA_AFTER_ALL_DEPOSITS" "$EXPECTED_INSTANCE_AFTER_DEP34")"
printf "%-20s %20s %20s %12s %12s %s\n" "5. After withdrawal" "$USER_SOLANA_FINAL" "$EXPECTED_USER_AFTER_WITHDRAW" "$INSTANCE_SOLANA_FINAL" "$EXPECTED_INSTANCE_AFTER_WITHDRAW" "$(check "$INSTANCE_SOLANA_FINAL" "$EXPECTED_INSTANCE_AFTER_WITHDRAW")"
echo ""
printf "Solana Private Channels User Balance: %s (expected: %s) %s\n" "$USER_PRIVATE_CHANNEL_FINAL" "$EXPECTED_PRIVATE_CHANNEL_AFTER_WITHDRAW" "$(check "$USER_PRIVATE_CHANNEL_FINAL" "$EXPECTED_PRIVATE_CHANNEL_AFTER_WITHDRAW")"
echo ""
echo "=== Summary ==="
echo "Instance ID: $INSTANCE_ID"
echo "Start Slot: $SLOT"
echo "Total deposited: 350000 (50k + 100k + 75k + 125k)"
echo "Total withdrawn: $WITHDRAW_AMOUNT"

echo ""
echo "=== Cleanup ==="
sedi "s/^enabled = true/enabled = false/" "$INDEXER_CONFIG"
sedi "s/^start_slot = .*/start_slot = 0/" "$INDEXER_CONFIG"
echo "Reset $INDEXER_CONFIG: enabled = false, start_slot = 0"
