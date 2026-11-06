#!/usr/bin/env bash
# Shared helpers for the deploy e2e scripts. Source after setting REPO.
# Knobs (all optional):
#   E2E_REPO_ROOT      repo the deploy ships from (default: this repo)
#   E2E_PROJECT        compose project the deploy uses (default: private-channel)
#   E2E_HOST_DATA_DIR  host_data_dir for the stack (default: /tmp/pc-secrets-e2e)
#   E2E_REGISTRY       image_registry holding private-channel-app and -validator
#   E2E_TAG            image_tag to deploy
#   E2E_WORK           state dir: keys, secrets.yml, logs (default: a new temp dir)
#   E2E_SKIP_RESET=1   never run the reset_state phase (shared hosts)

E2E_REPO_ROOT="${E2E_REPO_ROOT:-$REPO}"
E2E_PROJECT="${E2E_PROJECT:-private-channel}"
E2E_HOST_DATA_DIR="${E2E_HOST_DATA_DIR:-/tmp/pc-secrets-e2e}"
E2E_REGISTRY="${E2E_REGISTRY:-127.0.0.1:5000/pc-e2e}"
E2E_TAG="${E2E_TAG:-e2e}"
E2E_WORK="${E2E_WORK:-$(mktemp -d)}"
mkdir -p "$E2E_WORK/keys" "$E2E_WORK/logs"
# Values later steps need (instance, mint, active keys), kept across runs.
STATE="$E2E_WORK/state.env"
touch "$STATE"
# shellcheck disable=SC1090
. "$STATE"
save() { printf '%s=%q\n' "$1" "$2" >>"$STATE"; eval "$1=\$2"; }
LOCAL_RPC="${LOCAL_RPC:-http://127.0.0.1:18899}"
# Program ids the stack's image was built with (a test cluster may run its own copies).
ESCROW_PROGRAM_ID="${ESCROW_PROGRAM_ID:-9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU}"
WITHDRAW_PROGRAM_ID="${WITHDRAW_PROGRAM_ID:-J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi}"
BINS="${E2E_BINS:-$E2E_REPO_ROOT/target/debug}"

say() { echo "$(date -Iseconds) $*" | tee -a "$E2E_WORK/e2e.log" >&2; }
die() {
  say "FAIL: $*"
  exit 1
}
check() {
  local what="$1"
  shift
  if "$@"; then say "ok: $what"; else die "$what"; fi
}

# A private copy of the deploy dir, so its secrets.yml and ansible.log stay in E2E_WORK.
prepare_deploy_copy() {
  rm -rf "$E2E_WORK/deploy"
  cp -r "$E2E_REPO_ROOT/private-channel-deploy" "$E2E_WORK/deploy"
  rm -f "$E2E_WORK/deploy/secrets.yml" "$E2E_WORK/deploy/ansible.log"
  export ANSIBLE_CONFIG="$E2E_WORK/deploy/ansible.cfg" ANSIBLE_LOG_PATH="$E2E_WORK/logs/ansible.log"
  unset DOPPLER_TOKEN
}

keygen() { [[ -f "$E2E_WORK/keys/$1.json" ]] || solana-keygen new --no-bip39-passphrase --silent -o "$E2E_WORK/keys/$1.json" >/dev/null; }
pub() { solana-keygen pubkey "$E2E_WORK/keys/$1.json"; }

# Random, sentinel-free secrets; every value is also listed in secret-values.txt for the leak greps.
write_secrets() {
  local f="$E2E_WORK/deploy/secrets.yml" k
  : >"$f"
  for k in postgres_password postgres_replication_password postgres_runtime_password \
    postgres_auth_runtime_password postgres_auth_owner_password postgres_gateway_password \
    postgres_monitoring_password postgres_grafana_password postgres_indexer_password grafana_admin_password; do
    printf '%s: %s\n' "$k" "$(openssl rand -hex 16)" >>"$f"
  done
  printf 'jwt_secret: %s\n' "$(openssl rand -hex 32)" >>"$f"
  printf 'notify_webhook: http://e2e-hook-%s.invalid/notify\n' "$(openssl rand -hex 12)" >>"$f"
  printf 'ghcr_user: ""\nghcr_token: ""\n' >>"$f"
  chmod 600 "$f"
}

# All current secret values (the leak greps search for these).
secret_values() {
  python3 - "$E2E_WORK/deploy/secrets.yml" "$E2E_WORK/keys" <<'PY'
import glob, json, sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
skip = {"ghcr_user", "yellowstone_endpoint"}
for k, v in d.items():
    if k not in skip and v and len(str(v)) >= 8:
        print(v)
A = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
for p in glob.glob(sys.argv[2] + "/*.json"):
    b = bytes(json.load(open(p)))
    n = int.from_bytes(b, "big"); s = b""
    while n:
        n, r = divmod(n, 58); s = A[r:r + 1] + s
    print((b"1" * (len(b) - len(b.lstrip(b"\0"))) + s).decode())
    print(json.dumps(list(b)).replace(" ", ""))
PY
}

# Fails when any current secret value appears in the given files.
assert_no_secret_in() {
  local what="$1"
  shift
  secret_values >"$E2E_WORK/secret-values.txt"
  if grep -rqF -f "$E2E_WORK/secret-values.txt" "$@" 2>/dev/null; then
    die "$what: a secret value leaked (matching file: $(grep -rlF -f "$E2E_WORK/secret-values.txt" "$@" | head -3 | tr '\n' ' '))"
  fi
  say "ok: $what has no secret value"
}

secret() { python3 -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))[sys.argv[2]] or "")' "$E2E_WORK/deploy/secrets.yml" "$1"; }

# deploy <log-name> [ansible args]: a full deploy of localnet with the e2e vars.
deploy() {
  local log="$1"
  shift
  local reset=()
  [[ "${E2E_SKIP_RESET:-0}" == 1 ]] && reset=(--skip-tags reset_state -e reset_state=false)
  (cd "$E2E_WORK/deploy" && ansible-playbook -i localhost, -c local deploy.yml \
    -e env=dev -e "repo_root=$E2E_REPO_ROOT" -e "host_data_dir=$E2E_HOST_DATA_DIR" \
    -e "image_registry=$E2E_REGISTRY" -e "image_tag=$E2E_TAG" \
    -e "admin_keypair_path=$E2E_WORK/keys/${E2E_ADMIN_KEY:-admin}.json" \
    -e "operator_keypair_path=$E2E_WORK/keys/${E2E_OPERATOR_KEY:-operator}.json" \
    -e "permanent_delegate=$(pub delegate)" "${reset[@]}" ${E2E_DEPLOY_ARGS:-} "$@") \
    >"$E2E_WORK/logs/$log.log" 2>&1
}

compose() {
  local c="$E2E_HOST_DATA_DIR/config" f=docker-compose.yml
  [[ -f "$c/docker-compose.devnet.yml" ]] && f=docker-compose.devnet.yml
  docker compose -p "$E2E_PROJECT" --env-file "$c/versions.env" --env-file "$c/.env" \
    -f "$c/$f" -f "$c/compose.override.yml" "$@"
}

cid() { docker inspect -f '{{.Id}}' "$1" 2>/dev/null; }

psql_primary() { docker exec -i private-channel-postgres-primary psql -U private_channel -d private_channel -tAq "$@"; }
psql_replica() { docker exec -i private-channel-postgres-replica psql -U private_channel -d postgres -tAq "$@"; }
psql_indexer() { docker exec -i private-channel-postgres-indexer psql -U private_channel_indexer -d indexer -tAq "$@"; }

rpc() {
  curl -s -X POST "$1" -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":${3:-[]}}"
}

# wait_for <seconds> <description> <command...>
wait_for() {
  local secs="$1" what="$2" i
  shift 2
  for ((i = 0; i < secs; i += 3)); do
    if "$@" >/dev/null 2>&1; then
      say "ok: $what"
      return 0
    fi
    sleep 3
  done
  die "timed out waiting for: $what"
}

# Escrow setup bins, built once from the deploy's repo.
bin() { "$BINS/$1" "${@:2}"; }

# The pubkeys that signed a transaction (first numRequiredSignatures account keys).
tx_signers() {
  rpc "$1" getTransaction "[\"$2\",{\"encoding\":\"json\",\"maxSupportedTransactionVersion\":0,\"commitment\":\"confirmed\"}]" \
    | python3 -c 'import json, sys; t = json.load(sys.stdin)["result"]["transaction"]["message"]; print(" ".join(t["accountKeys"][:t["header"]["numRequiredSignatures"]]))'
}

GW=http://127.0.0.1:8899 WRITE=http://127.0.0.1:8900 READ=http://127.0.0.1:8901 AUTH=http://127.0.0.1:8903
FEE=1000 MINW=1000

# Waits until the airdrop is finalized: the bins simulate against finalized state.
airdrop() {
  solana airdrop -u "$LOCAL_RPC" "${2:-10}" "$(pub "$1")" >/dev/null
  wait_for 90 "airdrop to $1 finalized" bash -c \
    "solana balance -u '$LOCAL_RPC' --commitment finalized '$(pub "$1")' | grep -vq '^0 SOL'"
}
# Funds a key name (or, with fund_addr, a pubkey): a transfer from E2E_FUND_FROM (a key
# name) when set, else an airdrop.
fund() { fund_addr "$(pub "$1")" "${2:-}"; }
fund_addr() {
  if [[ -n "${E2E_FUND_FROM:-}" ]]; then
    solana transfer -u "$LOCAL_RPC" -k "$E2E_WORK/keys/$E2E_FUND_FROM.json" --allow-unfunded-recipient \
      "$1" "${2:-0.05}" >>"$E2E_WORK/logs/txs.log" 2>&1
  else
    solana airdrop -u "$LOCAL_RPC" "${2:-10}" "$1" >/dev/null
  fi
  wait_for 90 "funding of $1 finalized" bash -c \
    "solana balance -u '$LOCAL_RPC' --commitment finalized '$1' | grep -vq '^0 SOL'"
}
token_bal() { spl-token balance --owner "$1" "$2" -u "${3:-$LOCAL_RPC}" 2>/dev/null || echo 0; }
withdrawals_completed() { psql_indexer -c "SELECT count(*) FROM transactions WHERE transaction_type='withdrawal' AND status='completed'"; }
deposits_completed() { psql_indexer -c "SELECT count(*) FROM transactions WHERE transaction_type='deposit' AND status='completed'"; }
# Predicates for wait_for, re-evaluated on every poll.
more_deposits_than() { test "$(deposits_completed)" -gt "$1"; }
more_withdrawals_than() { test "$(withdrawals_completed)" -gt "$1"; }

unsettled_withdrawals() {
  psql_indexer -c "SELECT count(*) FROM transactions WHERE transaction_type='withdrawal'
    AND status::text NOT IN ('completed', 'failed', 'failed_reminted')"
}
has_unsettled_withdrawal() { test "$(unsettled_withdrawals)" -gt 0; }
no_unsettled_withdrawal() { test "$(unsettled_withdrawals)" -eq 0; }
indexer_db_url() { echo "postgres://private_channel_indexer:$(secret postgres_indexer_password)@127.0.0.1:5434/indexer"; }

# A deposit lands on the channel, then the user burns to withdraw; sets BEFORE_W for the release wait.
deposit_and_burn() {
  local before_d
  before_d="$(deposits_completed)"
  bin deposit "$LOCAL_RPC" "$E2E_WORK/keys/user.json" "$INSTANCE" "$MINT" 100000 >>"$E2E_WORK/logs/txs.log" 2>&1
  wait_for 300 "deposit completed" more_deposits_than "$before_d"
  BEFORE_W="$(withdrawals_completed)"
  PRIVATE_CHANNEL_READ_RPC_URL=$READ bin withdraw "$GW" "$E2E_WORK/keys/user.json" "$MINT" 5000 \
    >>"$E2E_WORK/logs/txs.log" 2>&1
}

# A deposit lands on the channel, then a withdrawal is released on Solana; echoes the release signature.
deposit_and_withdraw() {
  local sig
  deposit_and_burn
  wait_for 360 "withdrawal released" more_withdrawals_than "$BEFORE_W"
  sig="$(psql_indexer -c "SELECT counterpart_signature FROM transactions WHERE transaction_type='withdrawal' AND status='completed' ORDER BY id DESC LIMIT 1")"
  echo "$sig"
}

# The channel ATA (Token-2022) of an owner for MINT, derived offline.
channel_ata() {
  solana find-program-derived-address ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL \
    "pubkey:$1" pubkey:TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb "pubkey:$MINT"
}
channel_bal() {
  rpc "$READ" getTokenAccountBalance "[\"$1\"]" \
    | python3 -c 'import json, sys; print((json.load(sys.stdin).get("result") or {}).get("value", {}).get("amount", "0"))'
}

# sweep_fees <signer-arg> <treasury-pubkey> <log-name>: the treasury withdraws all its
# channel fees (no fee, no minimum) and the operator releases them to it on Solana.
sweep_fees() {
  local signer="$1" owner="$2" log="$3" ata fees bal0
  ata="$(channel_ata "$owner")"
  fees="$(channel_bal "$ata")"
  [[ "$fees" -gt 0 ]] || die "no withdraw fees on the channel ATA $ata of $owner"
  # ReleaseFunds needs the recipient's token account on Solana, or it fails and is reminted.
  local sol_ata
  sol_ata="$(spl-token address --owner "$owner" --token "$MINT" --verbose -u "$LOCAL_RPC" | awk '/Associated token address/ {print $NF}')"
  if ! solana account -u "$LOCAL_RPC" "$sol_ata" >/dev/null 2>&1; then
    spl-token create-account -u "$LOCAL_RPC" --fee-payer "$E2E_WORK/keys/user.json" --owner "$owner" "$MINT" \
      >>"$E2E_WORK/logs/txs.log" 2>&1 || die "could not create the Solana token account of $owner"
  fi
  wait_for 90 "the Solana token account of $owner finalized" solana account -u "$LOCAL_RPC" --commitment finalized "$sol_ata"
  BEFORE_W="$(withdrawals_completed)"
  bal0="$(token_bal "$owner" "$MINT")"
  PRIVATE_CHANNEL_READ_RPC_URL=$READ bin withdraw "$GW" "$signer" "$MINT" "$fees" >"$E2E_WORK/logs/$log.log" 2>&1 \
    || die "sweep failed (logs/$log.log)"
  check "the sweep runs as the treasury (fee 0)" grep -q "Withdraw fee: 0 " "$E2E_WORK/logs/$log.log"
  wait_for 360 "sweep of $fees fee units released" more_withdrawals_than "$BEFORE_W"
  check "the channel fee balance is drained" test "$(channel_bal "$ata")" = 0
  check "the fees landed on Solana for $owner" python3 -c 'import sys; sys.exit(not float(sys.argv[2]) > float(sys.argv[1]))' \
    "$bal0" "$(token_bal "$owner" "$MINT")"
  say "sweep $log: burn $(awk '/Transaction signature:/ {print $NF}' "$E2E_WORK/logs/$log.log" | tail -1) release \
$(psql_indexer -c "SELECT counterpart_signature FROM transactions WHERE transaction_type='withdrawal' AND status='completed' ORDER BY id DESC LIMIT 1")"
}

# Creates a Solana mint, funds the user, and allows the mint on the instance.
setup_mint() {
  keygen mint
  spl-token create-token -u "$LOCAL_RPC" --fee-payer "$E2E_WORK/keys/escrow-admin.json" \
    --mint-authority "$(pub escrow-admin)" "$E2E_WORK/keys/mint.json" >/dev/null
  save MINT "$(pub mint)"
  local ata
  ata="$(spl-token create-account -u "$LOCAL_RPC" --fee-payer "$E2E_WORK/keys/user.json" --owner "$(pub user)" "$MINT" 2>&1 \
    | awk '/Creating account/ {print $3}')"
  spl-token mint -u "$LOCAL_RPC" --fee-payer "$E2E_WORK/keys/escrow-admin.json" \
    --mint-authority "$E2E_WORK/keys/escrow-admin.json" "$MINT" 1 "$ata" >/dev/null
  # The bins read and simulate against finalized state.
  wait_for 90 "mint and user balance finalized" bash -c \
    "spl-token balance -u '$LOCAL_RPC' --owner '$(pub user)' '$MINT' 2>/dev/null | grep -q '^1' && \
     solana account -u '$LOCAL_RPC' --commitment finalized '$MINT' >/dev/null 2>&1"
  bin allow_mint "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$MINT" $FEE $MINW 0 >>"$E2E_WORK/logs/txs.log" 2>&1
}
