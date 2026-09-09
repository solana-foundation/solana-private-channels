# Runbook - Indexer Block Unavailable

This runbook covers the **`block_unavailable`** indexer wedge: the indexer has
positive proof that a block exists at some slot, and the RPC endpoint it is
pointed at will not serve that block. The indexer refuses to advance its
checkpoint past a slot whose contents it cannot read, so ingestion for that
program stops until an operator repoints it at an endpoint that has the block.

This is an **indexer** condition, not an operator one. It marks no transaction
row `failed` or `manual_review`, so it is **not routed by the webhook dispatch
table** in [`README.md`](README.md); it is paged by the Grafana alert
`indexer-block-unavailable` instead. It follows the same non-paged-halt shape as
[`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md):
recognize it from the alert plus the log marker, not from a webhook payload.

---

## What "unavailable" means here, precisely

The indexer does not guess. For each batch of slots it asks the node which slots
in the range produced a block (`getBlocks`), fetches only those, and walks their
`parentSlot` links forward from the last already-proven slot. A slot is reported:

- **skipped** only when a later block's `parentSlot` names the previous proven
  slot, which proves nothing was produced in between. Safe to advance past.
- **unavailable** when a block demonstrably exists at that slot and this endpoint
  will not hand it over. Never advanced past.
- **unproven** when the classifier could not obtain the witness it needs to decide
  either way. Also never advanced past, but it is *not* proof a block exists.

So a `block_unavailable` alert means the data exists somewhere and this endpoint
does not have it. The usual causes:

1. The endpoint has pruned that region of history and is not archival.
2. A load balancer is splitting requests across replicas at different heights, so
   one replica lists a slot that another will not serve.
3. The endpoint was restarted from a recent snapshot and never backfilled.

### Symptom

- Grafana alert `indexer-block-unavailable` fires at `severity: critical`.
- The indexer's current slot stops advancing for one `program_type` while the
  chain tip keeps rising, so the lag panel climbs.
- Deposits or withdrawals at and after the wedged slot never appear in the DB.
- The process stays healthy and keeps retrying; it does not crash-loop.

### Detection

The alert keys off the error counter, not the checkpoint frontier-lag gauge:

```
rate(private_channel_indexer_rpc_errors_total{error_type="block_unavailable"}[5m]) > 0
```

Use the counter, **not** `private_channel_indexer_checkpoint_frontier_lag`. That
gauge reads zero whenever the checkpoint writer is ungated, which is the normal
steady state for both RPC-polling indexers, so a wedged indexer shows a flat
frontier and a zero lag gauge. The counter is the only reliable signal.

The sibling alert `indexer-block-unproven` keys off the same counter under a
different label:

```
rate(private_channel_indexer_rpc_errors_total{error_type="block_unproven"}[5m]) > 0
```

Both wedge the checkpoint and both need an operator, but they mean different
things and the confirmation step below differs. `block_unavailable` is a positive
proof: `getBlocks(N, N)` returns `[N]` while `getBlock(N)` refuses. `block_unproven`
means no such proof was obtainable, most often a trailing run of non-producers
whose first block past the batch the endpoint will not serve. For that case the
wedged slot named in the log is where the indexer stopped, **not** necessarily the
slot the endpoint is missing, so confirm by asking for the first block at or after
the batch end (`getBlocksWithLimit(<end+1>, 1)`) and trying to fetch that one.

The corresponding log lines name the slot:

```
# live polling
Slot <N> is unavailable: a block exists here that this endpoint will not serve; refusing to checkpoint past unknown contents

# backfill or reconnect gap-fill
Backfill slot <N> is unavailable: a block exists here that this endpoint will not serve; aborting before checkpoint
```

Confirm the durable checkpoint is parked just below the named slot:

```sql
SELECT program_type, last_committed_slot, updated_at FROM indexer_state;
```

Confirm the endpoint really cannot serve it (substitute the slot from the log):

```sh
curl -s "$COMMON_RPC_URL" -H 'content-type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[<N>,{"encoding":"json","transactionDetails":"none","maxSupportedTransactionVersion":0,"rewards":false,"commitment":"finalized"}]}'
```

An error with code `-32004`, `-32007` or `-32009`, or a `null` result, confirms
it. Then check that the node nonetheless lists the slot as a producer:

```sh
curl -s "$COMMON_RPC_URL" -H 'content-type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[<N>,<N>,{"commitment":"finalized"}]}'
```

If that returns `[<N>]` while `getBlock` refuses, the endpoint is internally
inconsistent (cause 2 above) and repointing at a single archival node fixes it.

### Recovery

1. Identify a full-history endpoint for the affected chain. For
   `indexer-solana` that is an archival Solana RPC provider; for
   `indexer-private-channel` that is a read node whose `blocks` table still
   holds the slot (check whether truncation removed it).
2. Repoint the indexer and restart it:
   - `COMMON_RPC_URL` for the primary datasource, and
   - `COMMON_FALLBACK_RPC_URL` if one is configured.

   Both are read once at startup, so the process must be restarted.
3. Watch the counter go quiet and the checkpoint resume:

   ```sql
   SELECT program_type, last_committed_slot FROM indexer_state;
   ```

   The indexer re-reads from its stored checkpoint, so no manual replay is
   needed and nothing is skipped.

### If the block genuinely no longer exists anywhere

If no reachable endpoint retains the slot, the data is gone and no automated
path can recover it. The indexer will stay wedged, which is the intended
behaviour: advancing past it would silently lose whatever was in that block.
Escalate per [`_escalation.md`](_escalation.md) with the slot number, the
program type, and the endpoints already checked. A decision to skip the slot is
a deliberate, human, recorded one; there is no operator command that marks a slot
skipped, and there should not be.

### What NOT to do

- Do not raise the indexer's `start_slot` past the wedged slot to unstick it.
  That is exactly the silent data loss the fail-closed behaviour prevents. The
  indexer now enforces this: a start slot above the durable checkpoint refuses to
  boot, see
  [`indexer_start_slot_ahead_of_checkpoint.md`](indexer_start_slot_ahead_of_checkpoint.md).
- Do not point the indexer at a non-archival peer hoping it differs. A
  load-balanced peer can answer identically, which turns the restart into a
  no-op and burns time.
- Do not restart the indexer repeatedly without changing the endpoint. It
  already retries with backoff on its own.
