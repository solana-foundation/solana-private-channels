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
  <MINT_ADDRESS> \
  <WITHDRAW_FEE> \
  <MIN_WITHDRAW_AMOUNT> \
  <MIN_DEPOSIT_AMOUNT>
```
`<WITHDRAW_FEE>` is in the mint's base units, charged on top of every Solana Private Channels withdrawal and paid to the operator admin; size it to cover one release's SOL cost. `<MIN_WITHDRAW_AMOUNT>` is the smallest amount one withdrawal may move, also in base units, which limits how many releases a balance can queue at once. `0` is allowed for either, but a fee of `0` leaves the operator paying every release, so use it only where every participant is known; see the [zero-fee warning](../../docs/ESCROW_INTERACTION_GUIDE.md#allowmint). Run again with new values, to or from `0`, to reprice; they apply from the mint's next deposit. Re-running also re-opens both gates and re-pins the mint profile (accepting any change since the last allow), so follow the [reprice procedure](../../docs/ESCROW_INTERACTION_GUIDE.md#allowmint). `<MIN_DEPOSIT_AMOUNT>` is the smallest amount a deposit must land in the escrow, in base units, `0` for none; the escrow enforces it from the moment `allow_mint` lands.

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
Prints the mint's withdraw fee and minimum before sending, and stops if `<AMOUNT>` is below the minimum. The balance must cover `<AMOUNT>` plus the fee, and only `<AMOUNT>` is released on Solana. The operator admin withdraws without either.

## Block Mint (set the deposit / withdrawal gates)
Both flags are absolute, so passing `false` for one re-opens that gate. The script
prints the current gates before sending and warns when a flag re-opens one.
Blocking deposits leaves already-escrowed balances withdrawable.
```bash
cargo run --bin block_mint -- \
  https://api.devnet.solana.com \
  ./keypairs/escrow-admin.json \
  <INSTANCE_ID> \
  <MINT_ADDRESS> \
  <BLOCK_DEPOSITS> \
  <BLOCK_WITHDRAWALS>
```

## Monitor
```bash
# Watch deposit processing
docker logs -f private-channel-operator-solana

# Watch withdrawal processing
docker logs -f private-channel-operator-private-channel
```
