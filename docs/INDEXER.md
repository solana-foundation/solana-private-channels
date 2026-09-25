
# Indexer Architecture

## Indexer Components

Monitors Solana Mainnet and the Solana Private Channels payment channel for deposits/withdrawals and writes to database.

### Datasource Strategies

**1. Yellowstone gRPC**

Real-time block streaming via gRPC (requires a gRPC endpoint). Handles both Escrow and Withdraw program types.

**Location**: [`indexer/src/indexer/datasource/yellowstone/`](../indexer/src/indexer/datasource/yellowstone/)


**2. RPC Polling (Mainnet or Solana Private Channels)**

Enumerates the producing slots in each batch with `getBlocks`, then fetches only those blocks in parallel with `getBlock`. Higher latency (~1-5 seconds) but no special infrastructure required.

Slots and blocks are decoupled on a Solana Private Channels node: slots tick every `blocktime_ms` whether or not a block is produced, and an idle node produces one block per second. A batch window can therefore contain no block at all. When that happens the poller looks past the window with `getBlocksWithLimit` for the next producing slot and claims the range up to it, so `batch_size` caps how much work one batch does and never determines whether the indexer can advance. It is not coupled to the node's `blocktime_ms` or its idle block cadence. That search is bounded: a node heartbeats one block a second, so the widest idle gap is `1000 / blocktime_ms` slots and never more than 1 000, and the poller searches ten times that before treating the distance as a hole in the ledger rather than an idle stretch. The same bound is how far backfill looks below the chain tip for the last produced block, since the tip itself is usually a slot with no block and cannot anchor the range.

**Location**: [`indexer/src/indexer/datasource/rpc_polling/`](../indexer/src/indexer/datasource/rpc_polling/)


### Backfill Strategy

Recovers missed slots on indexer restart or network issues:
1. Read last processed slot from database (`indexer_state` table). That checkpoint is the
   lower bound. A configured `start_slot` only applies to a ledger that has never been
   indexed; one set above an existing checkpoint would skip the slots in between, so the
   indexer refuses to start instead. The same rule covers
   `indexer.rpc_polling.start_slot` when backfill is disabled, where no fill exists to
   recover those slots at all. See
   [`indexer_start_slot_ahead_of_checkpoint.md`](runbooks/indexer_start_slot_ahead_of_checkpoint.md)
2. Query RPC for current slot
3. If gap > threshold, for each batch of slots:
   - Enumerate which slots in the batch produced a block (`getBlocks`)
   - Fetch only those blocks in parallel (configurable batch size)
   - Walk their `parentSlot` links to prove the remaining slots empty; a slot that
     cannot be proven empty aborts the batch rather than being checkpointed past
   - Process blocks in order
   - Update checkpoint per slot via `CheckpointWriter` (driven by `SlotComplete` events)
4. For the Yellowstone datasource, persist a startup anchor before the live stream runs, so a
   durable checkpoint always exists: every connection, the first one included, replays from it up
   to the slot the stream opened at, and withholds live slots rather than advancing the checkpoint
   without one. The anchor is the resolved backfill range's floor, or the current chain tip when
   backfill is disabled. RPC polling has no reconnect repair and writes no anchor; it resumes from
   its configured start slot
5. Switch to real-time mode (Yellowstone or polling)

**Location**: [`indexer/src/indexer/backfill.rs`](../indexer/src/indexer/backfill.rs)

#### Backfill-only mode

Setting `indexer.backfill.backfill_only = true` (alongside `backfill.enabled`) turns the
indexer into a one-shot repair: it fills the resolved slot range and exits instead of
starting a live datasource. This is the tool to run when finalized deposits or withdrawals
are known to be missing from the database.

The mode runs the same pipeline as normal indexing (backfill producer, transaction
processor, checkpoint writer), so the rows it recovers land exactly as a live run would
have written them: deposits enter as `pending` for the operator to service. Startup
reconciliation is deliberately skipped, because the database is known-incomplete and
reconciling it would block the very repair that fixes it. An escrow instance id is still
required: without it every escrow instruction is filtered out as out of scope.

The exit code is the contract:

- **Exit 0** means every slot in the resolved range is durably recorded *and* the committed
  checkpoint reached the top of that range. The checkpoint is re-read from the database
  after the pipeline drains, so a stalled or failed checkpoint write cannot be reported as
  success.
- **Non-zero** means the range was not fully recorded. The checkpoint is left at the last
  slot that was completely stored, so re-running the repair resumes from there rather than
  redoing work that already committed.

Re-running a completed repair is safe: the range is resolved from the committed checkpoint,
and every write is idempotent, so no rows are duplicated.

**The range never reaches below the committed checkpoint.** It is resolved as
`(max(start_slot - 1, last_committed_slot), tip]`, so `backfill.start_slot` can only move
the floor *up*. If the hole sits below the checkpoint (the indexer has since streamed past
it), setting `start_slot` to the hole does nothing: the repair refills slots that were never
missing and exits 0 with the hole intact. Lower the checkpoint first, then run the repair:

```sql
UPDATE indexer_state SET last_committed_slot = <slot before the hole>
WHERE program_type = 'escrow';
```

Everything between that slot and the tip is then re-indexed. That is safe but not free, so
pick the highest slot that still sits below the hole.

**Raising `start_slot` above the checkpoint is refused.** The floor would land above slots
that were never indexed, and because the checkpoint writer is gated from that floor it would
walk to the top of the range and commit a checkpoint over them. Nothing would go back for
them afterwards: the next run resolves its floor from that higher checkpoint. The run stops
with `StartSlotAheadOfCheckpoint` instead. A configured `start_slot` may set the floor only
on a database that has never been indexed, where there is no checkpoint to skip past. If a
skip is genuinely intended, drop the checkpoint with a destructive resync rather than
raising the start slot. That resync still refuses a genesis above the program's earliest
row, so it can skip only slots that hold none of its rows.

### Resync and the live-state lock

`resync` deletes the rows of one program (escrow: deposits and its checkpoint; withdraw:
withdrawals, the nonce sequence and its checkpoint) and rebuilds them from chain. It never
touches the other program's rows, journals, nonces or checkpoint, because it cannot rebuild
them. It also keeps `mints` and `mint_status_history`: withdrawals survive an escrow resync
and reconciliation only sees mints listed in `mints`, and the rebuild upserts both. The
nonce reset is an `ALTER SEQUENCE ... RESTART`, which rolls back with the rest of the delete
if anything fails. It is guarded these ways.

1. **`--destroy-existing-data` is required.** No environment variable binding, so it
   cannot be left switched on in a deployment's env file.
2. **The live-state lock.** Every indexer and operator takes one Postgres advisory key
   in shared mode for its whole life; resync takes the same key exclusively and holds
   it for the entire rebuild. Postgres enforces the separation: workers coexist freely,
   resync refuses to start while any worker is up, and a worker refuses to start while
   a resync runs. Ownership is re-proved on a heartbeat, and once more synchronously
   immediately before the rows are deleted. A role that cannot prove it still owns
   the lock stops itself. A probe that goes unanswered is not treated as proof: the
   server may simply be slow, and the session still holds the lock while we retry, so
   the timeout is tolerated for 30s. An answer of "not held", or a dead session, is
   proof and stops the role at once. The indexer and operator also increment
   `private_channel_live_state_lock_lost_total{role,reason}`, while resync, being a
   one-shot command with no metrics server, reports it as a failed command instead.
3. **The reconciliation halt flag.** A halt means custody and the ledger disagree, so
   resync refuses while one is set rather than rebuilding over the evidence behind it.
4. **Nothing it deletes may be in flight.** A row of its own program that is
   `processing`, `pending_remint` or `manual_review`, or a non-terminal row with a
   broadcast journal, refuses the resync: deleting the journal would let the rebuilt row
   be sent again. Run the operator until the rows settle, stop it, then retry.
5. **A withdraw resync needs proof no nonce was spent.** It restarts the nonce sequence, so
   it refuses on any `completed` or `failed` withdrawal, any `observed_releases` row, or a
   bitmap that has advanced or cannot be read.
6. **An unfinished resync blocks every worker.** The delete writes a `resync_state` row and
   sets the reconciliation halt with a resync reason, in the same transaction; only a
   rebuild that completes clears both (on the lock's own session, and the halt only if its
   reason is still the resync's). A resync that dies after the delete leaves both, so
   indexers and operators refuse to start, and operators built before the marker stop
   fetching because of the halt, until the same program's resync is rerun to completion.
   A resync refuses under any halt except its own next to its own marker.
7. **The genesis slot may not drop indexed rows.** The delete takes every row of the
   program but the rebuild replays only from the genesis slot, so a genesis above the
   program's earliest row refuses before anything is deleted. The marker keeps the lowest
   slot its wipe deleted, so a rerun after an interrupted resync is held to the same bound.

The delete runs in one transaction on the lock session and is capped at 300s. Measured at
roughly 30k deposit rows a second with one journal each (6s for 200k rows, 30s for 1M), so
the cap covers about 5M rows with room to spare. A delete past the cap is abandoned and
reports `fenced work did not finish`; waits on another session's row locks are cut sooner,
at the lock session's 10s `lock_timeout`. Failures there keep their own cause, and only
`live-state lock ownership could not be proven` means the lock was lost.

The lock session sets its own TCP keepalives, so a holder whose host vanishes is reaped
by Postgres in under two minutes instead of the OS default of roughly two hours. Without
that, one dead worker host would refuse every resync for that long with nothing running.

Only workers running a build that takes the lock are visible to the refusal, so during
a rolling upgrade confirm they are stopped by process, not by the refusal alone.
Operators from a build older than the `resync_state` marker still stop on its halt. An
older indexer can still start, but it moves no value, and the resync rerun (which needs it
stopped) rebuilds its rows. Only builds older than the live-state lock are fully
unprotected, and those are already excluded above. Stop the streamer during a
resync and restart it after: rebuilt rows get new ids, so a running streamer would re-emit them.
Session-level advisory locks also do not survive a pooler in transaction-pooling mode,
the same constraint the sender's singleton lock already carries.

**Locations**: lock [`live_lock.rs`](../indexer/src/storage/common/storage/live_lock.rs);
rebuild [`resync.rs`](../indexer/src/indexer/resync.rs); runbook
[`live_state_lock_runbook.md`](runbooks/live_state_lock_runbook.md).

### Transaction Identity & CPI Indexing

Each indexed instruction is keyed on the triple **`(signature, instruction_index, inner_index)`**:

- `instruction_index` — absolute position of the top-level instruction (or, for a CPI, of its top-level ancestor) in the transaction.
- `inner_index` — `NULL` for a top-level instruction; otherwise the instruction's position in the **flattened inner-instruction list** of that top-level ancestor.

**This works at any CPI depth, not just one level.** The validator flattens *every* CPI depth under a top-level instruction into a single inner-instruction list (`meta.innerInstructions[i].instructions`), each entry carrying a `stackHeight`. So a deposit invoked two or more hops deep (`A → B → escrow.Deposit`) is still one entry in that flat list with a unique `inner_index` — `inner_index` is a flat position, **not** a nesting level. Deposit-event scoping likewise keys on `stackHeight` (it walks the contiguous run of deeper entries after the deposit), so it resolves the correct `DepositEvent` regardless of nesting depth.

**Locations**: identity column [`indexer/src/storage/common/models.rs`](../indexer/src/storage/common/models.rs); position capture [`InstructionLocation`/`InnerLocation`](../indexer/src/indexer/datasource/common/types.rs); event scoping `parse_deposit` in [`escrow.rs`](../indexer/src/indexer/datasource/common/parser/escrow.rs).


## Operator Components

Processes pending deposits/withdrawals and executes transactions between Solana Mainnet and the Solana Private Channels payment channel.

### Three-Stage Pipeline

**Location**: [`indexer/src/operator/`](../indexer/src/operator/)

#### 1. Fetcher

Polls database for pending transactions with row-level locking to prevent duplicate processing. Uses PostgreSQL `SELECT FOR UPDATE SKIP LOCKED` to prevent duplicate processing.

**Location**: [`indexer/src/operator/fetcher.rs`](../indexer/src/operator/fetcher.rs)


#### 2. Processor

Validates transactions and builds Solana instructions that are managed by the Solana Private Channels instance's authorized operators/admins. The processor is responsible for two main tasks:
- Processing deposits (Mainnet → Solana Private Channels) - handles building a `MintTo` instruction for the user on the Solana Private Channels payment channel.
- Processing withdrawals (Solana Private Channels to Mainnet) - handles building a `ReleaseFunds` instruction for the user on Mainnet, which consumes that withdrawal's nonce in the escrow instance's withdrawal bitmap.

**Location**: [`indexer/src/operator/processor.rs`](../indexer/src/operator/processor.rs)


#### 3. Sender

Submits transactions to the respective cluster with:
- Exponential backoff retry (configurable max attempts)
- Transaction confirmation polling
- Status updates to database (processing → completed/failed)
- Just-in-time mint initialization (if mint is not yet initialized on the Solana Private Channels payment channel, the Sender will include an `InitializeMint` instruction in the transaction prior to the `MintTo` instruction)
- Rotating the withdrawal bitmap on the Mainnet escrow instance. On a timer the sender compares the bitmap's generation against the lowest withdrawal nonce that still owes a release, and arms a `RotateBitmap` when that nonce belongs to a later generation. Driving it from state rather than from a particular withdrawal means the rotation still happens when the row on the boundary was quarantined or never written.

**Location**: [`indexer/src/operator/sender/`](../indexer/src/operator/sender/)

### Additional Components

#### Reconciliation

Runs alongside the three-stage pipeline to detect and resolve discrepancies between on-chain state and the indexer database. Runtime reconciliation checks two per-mint invariants over finalized reads and fails closed on a proven insolvency: `channel_supply <= custody` beyond the in-flight envelope (over-issuance), and ledger liabilities `<= custody` (a drain), where liabilities are all deposits minus the releases the escrow indexer observed, both at the custody slot once the indexer's checkpoint covers it. Either breach for three consecutive finalized ticks trips a durable DB halt flag that freezes both operators' fetchers (plus quarantine + forced-unhealthy + mandatory webhook); recovery is manual per [`docs/runbooks/reconciliation_halt_runbook.md`](runbooks/reconciliation_halt_runbook.md). A release discharges what the chain says it moved, capped at what its withdrawal row owed. A tick that cannot pin the ledger leaves the liability arm unarmed, which is counted, exported and alerted on rather than passing silently. Startup runs the same supply invariant and the same custody-vs-ledger comparison before the pipeline boots; when its ledger checkpoint sits below the custody snapshot it re-reads the ledger with completed withdrawals counted as released, so indexer lag cannot fail a healthy boot and an unexplained shortfall still stops it.

**Location**: [`indexer/src/operator/reconciliation.rs`](../indexer/src/operator/reconciliation.rs), [`indexer/src/indexer/reconciliation.rs`](../indexer/src/indexer/reconciliation.rs)

#### DB Transaction Writer

Handles batched database writes for transaction status updates from the operator pipeline.

**Location**: [`indexer/src/operator/db_transaction_writer.rs`](../indexer/src/operator/db_transaction_writer.rs)

#### Program Type

The indexer uses a `ProgramType` enum (`Escrow` | `Withdraw`) to determine which pipeline branch runs. This is why two parallel instances are deployed: one watching the Escrow program on Mainnet, and one watching the Withdraw program on the Solana Private Channels payment channel.
