# Runbook - Token-2022 Channel Reset

**Status: untested guidance.** The sequence below is derived from the code, not from a
rehearsed drill. Rehearse it on devnet first, and have someone who has run an operator
drain review it before following it on a shared environment.

## When to use it

Once, to move a running stack onto the release whose channel runs only Token-2022.
That release changes every channel mint and token account:

- Channel mints become 202-byte Token-2022 mints carrying the configured permanent
  delegate, and token accounts become 170-byte Token-2022 ATAs.
- Channel ATA addresses change, because the token program is an ATA seed.
- The legacy SPL Token program is no longer loaded, so existing channel balances
  could never move again.

There is no migration. The cutover is a reset: every channel mint is emptied first,
then the channel account state is wiped, and the new release recreates each mint on
its first deposit. Mainnet is untouched: the escrow program, its instances, allowed
mints and custody stay as they are.

## Prerequisites

- The new release's images, and the keys it needs:
  - the permanent delegate pubkey (required, no default; use a dedicated key in
    production),
  - optionally the compliance owner pubkey, which must differ from the delegate.
- The escrow instance admin keypair, to block and reopen deposits per mint.
- A quiet window. Every holder must withdraw their whole channel balance before the
  reset, so announce it well ahead.
- `psql` access to `postgres-primary` and `postgres-indexer`, and `curl` and `jq` on
  the host.

`<channel-rpc>` below is the read node or write node on the host (for example
`http://127.0.0.1:8901`), which needs no JWT. Through the gateway's public port it
needs an Operator JWT.

## 1. Drain

1. Block deposits on every allowed mint (`BlockMint` with `blockDeposits: true`,
   `blockWithdrawals: false`; `block_mint` in `scripts/devnet`). Withdrawals stay open.
2. Ask every holder to withdraw their full channel balance. Cancel or settle every
   open DvP swap: escrowed swap balances count toward supply.
3. Sweep the treasury fees: the operator admin's channel ATA holds the collected
   withdraw fees. Withdraw them with the running release's tooling
   (`scripts/devnet` `withdraw`), which charges the treasury no fee and no minimum.
   Create the operator admin's Mainnet token account for each mint first
   (`spl-token create-account <mint> --owner <admin> --url <solana-rpc>`, with
   `--program-2022` for a Token-2022 mint): fees accrue only on the channel, so it
   usually has none, and without it the escrow refuses the release and the fees are
   reminted.
4. Let both operators finish their queues.

## 2. Sign-off: every supply is 0, nothing pending

Both checks are read-only. On-call signs off on their output before anything is
stopped. If either fails, go back to step 1.

**Supply per channel mint.** The channel mint shares its address with the Mainnet
mint in the indexer's `mints` table. Supply is a little-endian u64 at bytes 36..44 of
the mint data, so a `dataSlice` reads just that:

```bash
docker compose exec -T postgres-indexer sh -c \
  'psql -At -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT mint_address FROM mints ORDER BY mint_address"' \
  > mints.txt

while read -r mint; do
  resp=$(curl -s <channel-rpc> -H 'Content-Type: application/json' -d \
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$mint\",{\"encoding\":\"base64\",\"dataSlice\":{\"offset\":36,\"length\":8}}]}")
  if echo "$resp" | jq -e '.error' >/dev/null; then
    echo "$mint ERROR $(echo "$resp" | jq -c '.error')"
  elif [ "$(echo "$resp" | jq -r '.result.value')" = "null" ]; then
    echo "$mint absent (supply 0)"
  else
    supply=$(echo "$resp" | jq -r '.result.value.data[0]' | base64 -d | od -An -t u8 --endian=little | tr -d ' ')
    echo "$mint $supply"
  fi
done < mints.txt
```

Every line must read `0` or `absent`. An `ERROR` line is not a pass: fix the RPC and
run it again.

**Nothing pending.** Against the indexer database (`postgres-indexer`):

```sql
SELECT transaction_type, status, COUNT(*)
  FROM transactions
 WHERE status IN ('pending', 'processing', 'parked', 'pending_remint', 'manual_review')
 GROUP BY transaction_type, status
 ORDER BY transaction_type, status;
```

It must return no rows: no pending remint, withdrawal or deposit, and no row in
manual review. Resolve each one through its own runbook first.

## 3. Stop

Stop in the order of [UPGRADING.md](../../UPGRADING.md#stop-order). The databases and
the `pg-backup-*` sidecars stay up.

## 4. Back up, then wipe the channel account state

1. Take the labelled base backups of both clusters described in
   [UPGRADING.md](../../UPGRADING.md#before-every-upgrade), and a dump of the account
   table:

   ```bash
   docker compose exec -T postgres-primary sh -c \
     'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t accounts -Fc' > accounts_pre_t22.dump
   ```

   If a Redis cache is configured (`PRIVATE_CHANNEL_REDIS_CACHE_URL`), save its
   snapshot too (`redis-cli -u <redis-url> BGSAVE`, then copy the `dump.rdb`).

2. Wipe the channel account state. Only the `accounts` table is cleared: blocks,
   transactions and the address index stay, so the slot sequence continues and the
   withdraw indexer's checkpoint and channel fence still match the ledger.

   ```bash
   docker compose exec -T postgres-primary sh -c \
     'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE accounts"'
   ```

   The replica follows the primary. If Redis is configured, clear it:
   `redis-cli -u <redis-url> FLUSHDB`.

3. Check the wipe: `SELECT count(*) FROM accounts` must return 0. Do not skip this.
   A leftover SPL mint fails the solvency check's supply read with `reset incomplete`,
   and after a few ticks reconciliation halts both pipelines until it is wiped.

## 5. Configure

Set, in the env file the stack starts from (or `permanent_delegate` and
`compliance_owner` in the Ansible vars):

- `PRIVATE_CHANNEL_PERMANENT_DELEGATE`: the delegate pubkey. Required: blank, the
  write node refuses to start.
- `OPERATOR_PERMANENT_DELEGATE`: the same pubkey. The shipped compose files fill a
  blank value from `PRIVATE_CHANNEL_PERMANENT_DELEGATE`; blank there too, the
  operators refuse to start.
- `PRIVATE_CHANNEL_COMPLIANCE_OWNER`: the compliance owner, or leave it unset or
  blank to refuse every delegate move.

## 6. Start the new release

Deploy the new `image_tag` and start in the order of
[UPGRADING.md](../../UPGRADING.md#start-order).

## 7. Smoke check

1. Reopen deposits on one mint (`BlockMint` with both flags `false`).
2. Make a small deposit of it, of at least its `minDepositAmount`.
3. Once the operator has minted it, check the channel mint:

   ```bash
   spl-token display <mint> --program-2022 --url <channel-rpc>
   ```

   It must be a Token-2022 mint whose `Permanent delegate` is the configured
   delegate, and `solana account <mint> --url <channel-rpc>` must show a length of
   202 bytes. The depositor's balance must sit in their Token-2022 ATA (`spl-token address --token <mint> --owner <depositor> --program-2022`).
4. Withdraw part of it and confirm the release lands on Mainnet.
5. Reopen deposits on the remaining mints.

## Rollback

- **Before step 4:** nothing has changed. Redeploy the previous `image_tag` and
  reopen deposits.
- **After step 4:** restore, do not only swap images back. Stop everything, restore
  both clusters from the step 4 base backups following [`../PITR.md`](../PITR.md),
  restore the Redis snapshot if one is configured, then redeploy the previous
  `image_tag`. Channel activity since the reset is lost, which is why deposits stay
  blocked on all but the smoke mint until step 7 passes. The `accounts` dump is for
  inspection.

## Rotating the permanent delegate

Not part of the reset. To move the delegate to another key later, follow
[Rotating the delegate](../ESCROW_INTERACTION_GUIDE.md#rotating-the-delegate): set the
new key, restart, then immediately rotate every mint's on-chain delegate with deposits
paused. Until a mint is rotated, the old key is unpoliced on it.
