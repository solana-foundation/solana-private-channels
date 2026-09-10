# Runbook - Mint Idempotency Memo Cutover

**Status: untested guidance.** The sequence below is derived from the code paths
named in it, not from a rehearsed drill. Have someone who has run an operator
drain review it before following it under pressure.

## Symptom

A resync aborts during its pre-flight with:

```
Consumed-set enumeration failed; aborting resync before drop: channel mint <SIG>
carries an idempotency memo that does not parse to a current-scheme
source-event-id (legacy memo scheme); resync cannot reconcile across the memo
cutover - drain the operator and follow the cutover runbook
```

The error surfaces as `ReconciliationError::ConsumedSetUnavailable`.

**Nothing has been destroyed.** The consumed-set is built before the resync drops
any state, so the abort leaves the database exactly as it was. There is no data
loss to recover from and no rush.

## Why the resync refuses rather than continuing

A resync rebuilds `transactions` from source history as fresh `pending` rows. To
avoid re-minting a deposit that was already serviced, it first enumerates every
mint the channel authority has already made and keys them by a durable source
event id carried in the transaction's memo.

Two memo schemes exist:

| Scheme | Prefix | Payload |
|---|---|---|
| Current | `private_channel:mint-idempotency:` / `private_channel:remint:` | encoded source event id |
| Legacy | same prefixes | local database transaction id |

A legacy memo names a row in a database the resync is about to rebuild, so it
cannot identify the source event that produced the mint. Treating it as
"unrecognised, therefore unserviced" would replay a real deposit and mint
unbacked supply, which is the failure the whole consumed-set exists to prevent.
So the enumeration fails closed on the first memo it cannot parse.

## Recovery

### 1. Confirm the scheme boundary

Take the signature from the error and read its memo:

```
solana confirm -v <SIG> --url <CHANNEL_RPC>
```

Then find where the schemes change over. List the authority's mints newest
first and note the newest signature whose memo still fails to parse:

```
solana transaction-history <CHANNEL_AUTHORITY> --url <CHANNEL_RPC> --show-transactions
```

Everything at or older than that point predates the cutover.

### 2. Drain the operator

Stop new work reaching the sender and let in-flight work settle, so the channel
gains no new mints while you reconcile:

```
docker compose stop operator-solana operator-private_channel
```

Confirm nothing is left mid-flight before continuing:

```sql
SELECT status, transaction_type, COUNT(*)
FROM transactions
WHERE status IN ('processing', 'parked', 'pending_remint')
GROUP BY 1, 2;
```

Resolve anything still `processing` or `pending_remint` by its own runbook
(`withdrawal_manual_review.md`, `deposit_manual_review.md`) before going on. A
resync started with rows in flight races the sender that owns them.

### 3. Choose a genesis slot past the cutover

The supported resolution is to resync from a source slot **after** the last
legacy-scheme mint, so the consumed-set only has to parse current-scheme memos.
Find the source slot of the deposit that produced that newest legacy mint, and
use the slot after it as the resync genesis.

This is safe precisely because the deposits before that point were already
serviced: they are not in the rebuilt window, so they cannot be replayed.

### 4. Resync

Run the resync with the chosen genesis slot and the channel RPC configured, so
the consumed-set is built rather than skipped. If it aborts again naming a newer
signature, the cutover point was wrong; repeat from step 1 with that signature.

### 5. Restart and verify

Restart the operators, then confirm startup reconciliation passes and no deposit
was re-minted:

```sql
SELECT COUNT(*) FROM transactions
WHERE transaction_type = 'deposit' AND status = 'completed';
```

Compare against the count taken before the resync. It must not have grown.

## What NOT to do

- **Do not resync without a channel RPC configured.** That path logs
  `Resync running WITHOUT channel reconciliation` and rebuilds every row as
  `pending` with no consumed-set at all. It is safe only against a genuinely
  empty channel, and it is exactly the replay this error prevents.
- **Do not widen the memo parser to accept legacy payloads.** A legacy memo
  carries a database id whose meaning the resync is in the middle of destroying;
  accepting it would produce confident, wrong reconciliation.
- **Do not resync from genesis** on a channel that has any legacy-scheme mint.
  It will abort at the same place, having done no harm but no good either.
