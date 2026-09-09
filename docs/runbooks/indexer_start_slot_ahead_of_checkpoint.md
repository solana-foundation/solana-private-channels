# Runbook - Indexer Start Slot Ahead Of Checkpoint

This runbook covers the **`StartSlotAheadOfCheckpoint`** startup refusal: a configured
start slot sits above the durable checkpoint, so booting would begin indexing above slots
that were never indexed and nothing would ever go back for them.

This is an **indexer** condition, not an operator one. It marks no transaction row
`failed` or `manual_review`, so it is **not routed by the webhook dispatch table** in
[`README.md`](README.md). It surfaces as a boot-time crash loop: the process exits before
serving anything, so a container restart policy will retry it forever. Recognize it from
the log marker, not an alert.

---

## Symptom

The indexer exits during startup, before any slot is indexed, and the logs carry:

```
Configured indexer.backfill.start_slot 200 is ahead of the durable checkpoint 100
for withdraw: the slots after 100 and below 200 have never been indexed and would be
skipped. Lower indexer.backfill.start_slot to 100 or below, unset it, or run a
destructive resync if the skip is intended.
```

"CheckpointWriter service started" usually appears *before* the refusal, so it is not a
sign this is a different fault: outside backfill-only mode the writer starts first and the
floor is settled after it. The message above is the marker to grep for.

The `setting` in the message is the key to change. It is one of two:

| Setting | Meaning | Remedy |
|---|---|---|
| `indexer.backfill.start_slot` | Backfill is enabled and its configured floor is above the checkpoint. | Lower or unset it (below). |
| `indexer.rpc_polling.start_slot` | Backfill is **disabled**, so the live stream is the only producer and nothing can fill the gap. | **Enable backfill** (below). |

---

## Why the indexer refuses rather than continuing

The durable checkpoint is evidence of what was actually processed. A configured start slot
is only intent. Starting above the checkpoint leaves the slots in between unfetched and
unrecorded: no backfill covers them, the checkpoint writer runs ungated and advances past
them on the first live slot, and reconnect repair replays from a checkpoint that is now
above the hole. Once a higher checkpoint commits, those slots are unreachable and nothing
distinguishes them from indexed history.

For the withdraw program that is a burn whose release is never queued. For escrow it shows
up later as a custody reconciliation mismatch, which points an operator at the wrong fault.

---

## Recovery

### 1. Confirm the state

```sql
SELECT program_type, last_committed_slot FROM indexer_state;
```

Compare `last_committed_slot` against the configured start slot. Remember that the
environment overrides the TOML file, so check both:

```bash
env | grep -E 'INDEXER_(BACKFILL|RPC_POLLING)_START_SLOT'
grep -A5 '\[indexer.backfill\]' <your-indexer-config>.toml
```

### 2a. `indexer.backfill.start_slot` is too high

Lower it to the checkpoint or below, or unset it entirely. Unsetting is usually correct:
the checkpoint already says where to resume, and the knob only exists to initialize a
database that has never been indexed.

Backfill then covers everything from the checkpoint to the chain tip. If that range is
larger than `max_gap_slots`, the next boot stops with `GapTooLarge` instead. That is the
existing "manual intervention" signal, not a new problem: raise `max_gap_slots`
deliberately once you have decided the range is safe to fill.

### 2b. `indexer.rpc_polling.start_slot` is too high

Backfill is disabled, so lowering the polling start does not help: the live source only
walks forward and nothing fills what is below it. Set
`indexer.backfill.enabled = true` with `start_slot` unset, so the gap between the
checkpoint and the tip is actually fetched.

### 2c. The skip is genuinely intended

If the skipped range is known-empty or its history is deliberately being abandoned, the
supported path is a destructive resync, which drops the checkpoint and rebuilds from a
chosen genesis slot under fail-closed channel reconciliation:

```bash
private-channel-indexer resync --genesis-slot <slot> --channel-rpc-url <url>
```

A withdraw resync additionally needs `common.escrow_instance_id` and `--escrow-rpc-url`
(a Solana RPC), and refuses unless the escrow's withdrawal bitmap is at generation 0 with
no set bits.

Do **not** hand-edit `indexer_state` to make the refusal go away. That is the silent data
loss the refusal exists to prevent, and it is the same move
[`indexer_block_unavailable.md`](indexer_block_unavailable.md) forbids.

---

## The special case: a checkpoint of exactly 0

A row whose `last_committed_slot` is `0` **and** which has no `transactions` rows is almost
certainly a phantom. Older builds created the `indexer_state` row with a
`NOT NULL DEFAULT 0` slot before the indexer ever flushed a checkpoint, so a never-indexed
program read back as "indexed through genesis".

Newer builds leave the slot unset in that case, but they cannot repair rows that already
hold a defaulted zero: nothing distinguishes one from a real genesis checkpoint. Confirm
with:

```sql
SELECT s.program_type, s.last_committed_slot,
       (SELECT COUNT(*) FROM transactions t
         WHERE t.transaction_type = CASE s.program_type
                                      WHEN 'withdraw' THEN 'withdrawal'
                                      ELSE 'deposit'
                                    END::transaction_type) AS tx_rows
FROM indexer_state s;
```

The count is scoped to the program's own rows on purpose: both programs can share one
database, and the other program's rows say nothing about whether this one was ever indexed.

If `last_committed_slot = 0` and `tx_rows` is 0, the row never carried a real checkpoint.
Clear the slot so the configured start slot can initialize the ledger:

```sql
UPDATE indexer_state SET last_committed_slot = NULL WHERE program_type = '<program>';
```

If any of those three conditions does not hold, treat the `0` as a real checkpoint and use
the normal recovery above.

---

## What NOT to do

- Do not raise the checkpoint by hand to clear the refusal. The skipped range is then lost
  with no record that it ever existed.
- Do not delete the `indexer_state` row to "start fresh" unless you also intend to resync.
  Dropping the checkpoint lets the configured start slot take effect, which is exactly the
  skip being refused.
- Do not disable backfill to get past a `GapTooLarge` that appears after fixing this. That
  swaps one fail-closed refusal for the silent version of the same loss.
