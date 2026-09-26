# Runbook - Resync Refused: Channel Mint Does Not Pay Its Source Event

**Status: untested guidance.** Derived from the code paths named below, not
from a rehearsed drill.

## Symptom

A resync aborts during its pre-flight with:

```
channel mint <channel_signature> names source event <source_event_id> (source tx
<source_signature>) but does not pay it (<reason>); resync aborted before drop
```

The error is `ReconciliationError::ConsumedMintMismatch`. `<reason>` lists each
field that disagrees as `field: channel X, row Y`, one of:

- `kind` - a remint marker names a deposit, or a mint marker names a withdrawal.
- `mint`, `recipient ATA` or `amount` - the channel `MintTo` does not pay what
  the source event says.

A sibling refusal fires when one source event has two successful authority-signed
mints, a double issuance even if both pay the right amount:

```
consumed-set unavailable, resync aborted before drop: source event <id> has two
successful channel mints signed by the authority, <sig_a> and <sig_b>; one
event may be minted once, see docs/runbooks/resync_consumed_mint_mismatch.md
```

Handle it the same way, inspecting both signatures.

**Nothing has been destroyed.** The resync replays the source history without
writing and checks every channel mint against the event it names before it
deletes anything. The database is exactly as it was.

## What it means

Only the operator's mint authority can produce these channel transactions:
resync ignores any the authority did not sign. So a mismatch means one of:

- An operator bug issued a mint with the wrong mint, recipient or amount, or
  labelled it with the wrong marker.
- The mint authority key signed a transaction the operator did not build.

Either way the channel issuance for this event cannot be trusted, and rerunning
the resync will fail the same way.

## What to do

1. Do not retry the resync. The inputs are on-chain and will not change.
2. Inspect the channel transaction:
   `solana confirm -v <channel_signature> --url <private-channel-rpc-url>`.
   Note the `MintTo` mint, destination and amount.
3. Find the live row by its source transaction:
   ```sql
   SELECT id, transaction_type, status, recipient, initiator, mint, amount,
          counterpart_signature, landed_remint_signature
     FROM transactions
    WHERE signature = :source_signature;
   ```
4. [Escalate](_escalation.md) (Tier 1) with both. If the channel mint paid a
   different recipient or amount, tokens were mis-issued; if nothing in the
   operator's logs shows it building that transaction, treat the authority
   key as compromised.

**Keep the operators stopped.** An intact database is not a safe one: the row
may still be `pending` while a channel mint already paid it, and an operator
that claims it with no journaled signature mints it again. Indexers may
restart, since they only ingest.

Before any operator restarts, engineering must clear the signing authority and
take every row of that source transaction out of reach:

1. Record the signature journals in the incident record. Recovery deletes a
   row's remint journal once it leaves `pending_remint`, so this comes first.
   ```sql
   SELECT transaction_id, signature, last_valid_block_height
     FROM pending_release_signatures
    WHERE transaction_id IN (SELECT id FROM transactions WHERE signature = :source_signature);
   SELECT transaction_id, signature, last_valid_block_height
     FROM pending_remint_signatures
    WHERE transaction_id IN (SELECT id FROM transactions WHERE signature = :source_signature);
   ```
2. Quarantine every state the operators resume on their own. The sender
   restores `pending_remint` rows on startup and recovery requeues `parked`
   ones.
   ```sql
   UPDATE transactions SET status = 'manual_review', updated_at = NOW()
    WHERE signature = :source_signature
      AND status IN ('pending', 'processing', 'parked', 'pending_remint');
   ```
3. Confirm nothing is left to pick up. This must return 0:
   ```sql
   SELECT COUNT(*) FROM transactions
    WHERE signature = :source_signature
      AND status IN ('pending', 'processing', 'parked', 'pending_remint');
   ```

Resync stays blocked until engineering resolves the contradiction.

## Related refusals: incomplete channel history

Two more `ConsumedSetUnavailable` refusals mean the channel history the
consumed-set was built from cannot be shown complete. Both fire before the
wipe, so the database is untouched.

**A serviced row is missing from the channel history:**

```
consumed-set unavailable, resync aborted before drop: <n> serviced Deposit row(s)
are missing from the channel history, first <signature>; the channel index may
lag, rerun once it catches up
```

Every `completed` deposit (escrow resync) or `failed_reminted` withdrawal
(withdraw resync) must appear in the authority's channel history. A missing one
would be rebuilt `pending` and paid again. The usual cause is lag: the channel
writes its address index after each block commits, and a read replica can trail
the primary. Wait, then rerun. If it keeps refusing, check that the write node has
been up since its last crash (it rebuilds missing index rows at startup) and that
the configured authority is the one that minted the named row.

**The channel history was pruned:**

```
consumed-set unavailable, resync aborted before drop: channel history is pruned
below slot <floor>, so the consumed-set may miss serviced mints
```

or `channel first available block unreadable: ...`. Once the channel has been
truncated, its mint history is incomplete and a resync cannot prove what was
already paid. Resync is not supported on a truncated channel. Escalate
(Tier 2); do not truncate a channel you may need to resync.

### Resyncing an empty database

With no rows, the missing-row check has nothing to compare, and the pruning check
does not cover lag. Before resyncing an empty database, or rerunning a resync that
was interrupted after its wipe, confirm all three:

1. The write node has been up since its last crash, so its startup index repair
   has run.
2. The read replica has replayed up to the primary: `pg_last_wal_replay_lsn()` on
   the replica is at or past `pg_current_wal_lsn()` read on the primary just before.
3. On the primary, the `address_signatures_flushed_slot` metadata value is at or
   above the slot of the newest block that holds transactions.

Waiting a fixed time is not a substitute: a crashed write node leaves index rows
missing until it restarts, and replica lag has no bound.

### Failed deposits

The missing-row check covers `completed` deposits only. A `failed` deposit keeps
no mint signature (its broadcast journal is deleted once the row is terminal),
so resync cannot tell a mint that never landed from one that landed after the
confirmation timed out. If the channel index lags, a landed one is rebuilt
`pending` and minted again. Before any escrow resync, triage every `failed`
deposit with [`deposit_failed.md`](deposit_failed.md): a `LANDED` verdict makes
the row `completed`, and the missing-row check then covers it.

### Release gate

Core hides history below `getFirstAvailableBlock` instead of failing on it. An
indexer that predates the checks above would read that shorter history as
complete and could re-mint pruned deposits. So:

- deploy core with this behaviour only after every indexer runs a version with
  both checks;
- never roll the indexer back below that version while core has it.
