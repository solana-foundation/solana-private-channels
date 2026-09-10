# Devnet Setup Commands

## Prerequisites
```bash
cp .env.example .env
# Edit .env with your keys
# Required (no defaults shipped): set POSTGRES_PASSWORD and POSTGRES_REPLICATION_PASSWORD
# (and JWT_SECRET if enabling auth, ADMIN_PRIVATE_KEY for the operator) or services fail to start
# Generate strong values with: openssl rand -hex 32
```

## Two admin keys

Steps 1-3 below are escrow governance and are signed by `keypairs/escrow-admin.json`,
the `Instance.admin` key. The services run as a *different* key, the channel admin in
`ADMIN_PRIVATE_KEY`, which holds receipt-mint authority and is what step 2 registers as
the operator. Keep them separate: `SetNewAdmin` only rewrites `Instance.admin`, so a key
that is also the channel admin keeps its receipt-mint authority and its Operator PDA
after a handover and can still drain escrow custody.

The commands below read their signers from keypair files. Create them first:
```bash
mkdir -p keypairs
solana-keygen new -o ./keypairs/escrow-admin.json -s --no-bip39-passphrase  # signs create_instance / add_operator / allow_mint
solana-keygen new -o ./keypairs/user.json -s --no-bip39-passphrase          # signs deposit / withdraw
```

Run all commands from project root:

## 1. Create Instance

Also allocates the instance's withdrawal bitmap (8 KB, about 0.058 SOL of rent),
so fund the admin keypair accordingly before running this.

```bash
cargo run --bin create_instance -- \
  https://api.devnet.solana.com \
  ./keypairs/escrow-admin.json
```

## 2. Add Operator
```bash
cargo run --bin add_operator -- \
  https://api.devnet.solana.com \
  ./keypairs/escrow-admin.json \
  <INSTANCE_ID> \
  <OPERATOR_PUBKEY>
```

## 3. Allow Mint
```bash
cargo run --bin allow_mint -- \
  https://api.devnet.solana.com \
  ./keypairs/escrow-admin.json \
  <INSTANCE_ID> \
  <MINT_ADDRESS>
```

## 4. Deposit (Solana → Solana Private Channels)
```bash
cargo run --bin deposit -- \
  https://api.devnet.solana.com \
  ./keypairs/user.json \
  <INSTANCE_ID> \
  <MINT_ADDRESS> \
  <AMOUNT>
```

## 5. Withdraw (Solana Private Channels → Solana)
```bash
cargo run --bin withdraw -- \
  http://localhost:8899 \
  ./keypairs/user.json \
  <MINT_ADDRESS> \
  <AMOUNT>
```

## Monitor
```bash
# Watch deposit processing
docker logs -f private-channel-operator-solana

# Watch withdrawal processing
docker logs -f private-channel-operator-private-channel
```
