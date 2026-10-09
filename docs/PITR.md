# Point-in-Time Recovery (PITR)

This document describes how WAL archiving, base backups, and point-in-time recovery work for Solana Private Channels's two PostgreSQL databases.

## Architecture

```
┌──────────────────┐       ┌─────────────────────────────┐
│ postgres-primary  │──WAL──▶ primary-wal-archive volume   │
│  (accounts DB)    │       └─────────────────────────────┘
└──────────────────┘
        ▲
        │ pg_basebackup (every 6h)
┌──────────────────┐       ┌─────────────────────────────┐
│ pg-backup-primary │──────▶ primary-basebackups volume    │
└──────────────────┘       └─────────────────────────────┘

┌──────────────────┐       ┌─────────────────────────────┐
│ postgres-indexer   │──WAL──▶ indexer-wal-archive volume   │
│  (indexer DB)     │       └─────────────────────────────┘
└──────────────────┘
        ▲
        │ pg_basebackup (every 6h)
┌──────────────────┐       ┌─────────────────────────────┐
│ pg-backup-indexer  │──────▶ indexer-basebackups volume    │
└──────────────────┘       └─────────────────────────────┘
```

### Volumes

| Volume | Contents |
|---|---|
| `postgres-primary-wal-archive` | Archived WAL segments from accounts DB |
| `postgres-indexer-wal-archive` | Archived WAL segments from indexer DB |
| `postgres-primary-basebackups` | Periodic full base backups (accounts DB) |
| `postgres-indexer-basebackups` | Periodic full base backups (indexer DB) |

### Configuration

| Env Var | Default | Description |
|---|---|---|
| `PG_BACKUP_INTERVAL_HOURS` | 6 | Hours between base backups |
| `PG_BACKUP_RETENTION_COUNT` | 3 | Number of base backups to retain |

Both must be positive integers of at most 4 digits, with no sign and no leading zero. The
sidecar exits at startup, before waiting for Postgres, on any other value. A retention of 0
is refused because it would delete the backup just taken.

After each backup and prune, the sidecar checks that a complete base backup is still on
disk and logs `Retention verified`. If none is left it logs an `ERROR` and skips WAL
pruning, so the archive is not trimmed with no base backup to restore from. It does not
exit, since a restart would only take another full backup. Alert on that `ERROR` line.

## Restore Procedure

### Prerequisites

- Docker Compose access to the target environment
- Identify the **target recovery time** (UTC timestamp)
- Identify which database to restore (`primary` or `indexer`)

### The one rule: the indexer DB is never ahead of the channel

The channel primary (accounts DB) and the indexer DB are backed up and restored on their
own, and each holds facts about the other. The indexer DB records which channel blocks it
read, which deposits were minted and which withdrawals were released. If it is restored to
a point *after* the channel's, it describes a history the channel no longer has: completed
deposits whose credit is gone, pending withdrawals whose burn was rolled back, and a
checkpoint above the channel's tip. Every case of that loses or double-pays funds.

So:

- **Indexer only.** Allowed. Restore `postgres-indexer` to any target inside the channel's
  retained history. The operators find every mint the channel already made in its own mint
  history. If any withdrawal was released after the target, the withdraw operator refuses
  to start, because the release can no longer be tied to a row; pick a target after the
  last release, or follow the manual procedure in the halt runbook (see below).
- **Primary, then indexer.** If the primary is restored, the indexer DB **must** be
  restored too, to a target **at least 60 seconds earlier** than the primary's. Every
  indexer row is written after the channel commit it describes, so an earlier target is
  safe by order alone; the margin covers clock skew between the two database hosts. Do not
  shrink it.
- **Primary only is forbidden.** In most cases the withdraw indexer and both operators
  refuse to start on it (the channel fence, below), but not always: the fence is the
  newest block the withdraw indexer had flushed, normally a few seconds behind the channel
  and longer if that indexer was lagging. A primary target newer than the fence passes
  every check, and anything the channel did between the fence and the target is then lost
  without a refusal. Always restore the indexer DB too.
- **Indexer targets must lie inside the channel's retained history.** After
  `private-channel-admin truncate`, the channel no longer serves history below its floor
  (`getFirstAvailableBlock`). The operators find forgotten mints only in the history the
  channel still serves, and they cannot tell a target below the floor from a quiet mint, so
  this is not checked: read the floor's block time before choosing an indexer target, and
  never restore the indexer to before it.

Every restore, of either database, stops all of `write-node`, `read-node`, `streamer`,
both indexers and both operators first, and starts them again only in the order in Step 6.

> **Note:** The Compose project name is `private-channel` (set via the `name:` key
> in `docker-compose.yml`), so volumes are prefixed `private-channel_` and containers
> `private-channel-`. Run `docker volume ls | grep postgres` to confirm. All
> `docker compose` commands below assume you're in the repo root (they resolve
> `docker-compose.yml` and the `private-channel` project automatically); if compose
> reports unset variables, prepend the standard env chain
> `--env-file versions.env --env-file .env.local`, or use the guarded `make docker-*`
> targets.

### Restore whole databases only

Restore a whole database, with the write node stopped, never selected rows of
`accounts`. An account row's data does not name its own pubkey, so a row-level
restore that puts one account's bytes under another key cannot be detected by
the node. Any row-level restore tool, or a design with more than one writer,
reopens this risk (audit finding SOLA6-104) and needs its own review.

### Step 1: Stop everything that reads or writes either database

The same services stop for either restore. An indexer or operator left running across a
primary restore keeps acting on the old channel history.

```bash
docker compose stop indexer-solana indexer-private-channel operator-solana operator-private-channel streamer read-node write-node

# Then the database being restored, and its backup sidecar.
# For postgres-primary:
docker compose stop postgres-replica postgres-primary pg-backup-primary

# For postgres-indexer:
docker compose stop pg-backup-indexer postgres-indexer
```

> `streamer` depends on both `postgres-replica` (accounts DB) and `postgres-indexer` (indexer DB), so it must be stopped for either restore scenario.

For a primary restore, restore **both** databases: do Steps 2 to 5 for `postgres-primary`
and then again for `postgres-indexer` with the earlier target, before Step 6. Do not run a
withdraw resync instead: it clears the fence, which turns off the checks until the next
checkpoint flush.

### Step 2: Clear the data volume

```bash
# For postgres-primary:
docker run --rm -v private-channel_postgres-primary-data:/data alpine sh -c "rm -rf /data/*"

# For postgres-indexer:
docker run --rm -v private-channel_postgres-indexer-data:/data alpine sh -c "rm -rf /data/*"
```

### Step 3: Restore the base backup

Pick the most recent base backup **before** your target recovery time.

```bash
# List available backups:
docker run --rm -v private-channel_postgres-primary-basebackups:/backups alpine ls -la /backups/

# Restore (example with base_20260304_060000):
docker run --rm \
  -v private-channel_postgres-primary-basebackups:/backups:ro \
  -v private-channel_postgres-primary-data:/data \
  postgres:16-alpine sh -c "
    cd /data
    tar xzf /backups/base_20260304_060000/base.tar.gz
    mkdir -p pg_wal
    tar xzf /backups/base_20260304_060000/pg_wal.tar.gz -C pg_wal/
  "
```

For postgres-indexer, substitute `primary` → `indexer` in volume names.

### Step 4: Configure recovery target

PostgreSQL 16 uses `postgresql.auto.conf` + `recovery.signal` (not the removed `recovery.conf`).

```bash
docker run --rm \
  -v private-channel_postgres-primary-data:/data \
  alpine sh -c "
    cat >> /data/postgresql.auto.conf << 'EOF'
restore_command = 'cp /wal_archive/%f %p'
recovery_target_time = '<YYYY-MM-DD HH:MM:SS UTC>'
recovery_target_action = 'promote'
EOF
    touch /data/recovery.signal
    chown 70:70 /data/postgresql.auto.conf /data/recovery.signal
  "
```

Replace the timestamp with your target recovery time.

### Step 5: Start the database

```bash
# For postgres-primary:
docker compose up -d postgres-primary
docker compose logs -f postgres-primary  # watch for "database system is ready"

# For postgres-indexer:
docker compose up -d postgres-indexer
docker compose logs -f postgres-indexer
```

### Step 6: Restart in order

```bash
# After a primary restore: wipe the replica first. A standby left on the old timeline
# keeps serving blocks from before the restore.
docker compose stop postgres-replica
docker run --rm -v private-channel_postgres-replica-data:/data alpine sh -c "rm -rf /data/*"
docker compose up -d postgres-replica pg-backup-primary

# The write node first. Wait until it is healthy and its log shows
# "Redis cache aligned with Postgres": the Redis cache survives a restore of the same
# database, and the write node is what purges blocks from before the restore.
docker compose up -d write-node
docker compose logs -f write-node   # wait for "Redis cache aligned with Postgres"

# Then the read node (compose also waits for write-node to be healthy).
docker compose up -d read-node

# The withdraw operator next, before either indexer, and wait for its boot gate.
docker compose up -d pg-backup-indexer operator-private-channel
docker compose logs -f operator-private-channel   # wait for "Withdrawal bitmap verification passed"

# Then the rest.
docker compose up -d indexer-solana indexer-private-channel streamer operator-solana
```

The withdraw operator must pass its boot gate before `indexer-private-channel` starts. The
gate compares the escrow bitmap with the restored rows; once the withdraw indexer
re-indexes burns, their new nonces can reach a later generation and hide a rotation the
restored DB is behind.

After an indexer-only restore the replica wipe and the Redis wait are not needed, but the
channel services were stopped in Step 1, so start them in the same order.


If the withdraw indexer or an operator then refuses to start, see
[What a refusal after a restore means](#what-a-refusal-after-a-restore-means).

## Indexer-Specific Notes

What happens after `postgres-indexer` is restored to an earlier point:

1. **Re-indexing** - `indexer_state.last_committed_slot` is flushed in batches (about every
   5 seconds), so the restored checkpoint may sit below rows it already holds. Each indexer
   resumes from its checkpoint and re-reads what came after it. Re-indexing is idempotent:
   a transaction row is keyed by `(signature, instruction_index, inner_index)` and an
   existing key is skipped, and mint rows by mint address.
2. **Rows come back as they were at the target.** A deposit minted after the target comes
   back `pending`, a withdrawal released or reminted after it comes back `pending` too, and
   re-indexed burns may get different withdrawal nonces than before, because the nonce
   sequence is restored with the DB and sequences have gaps.
3. **Deposits and remints are matched to the channel's mint history.** At boot each
   operator walks every mint's channel history back to the newest mint of its own kind
   the DB already knows (a completed deposit, or a landed remint), and keeps every operator mint it finds by its source event. A claimed row whose
   mint is already on the channel is completed (deposit) or marked `failed_reminted`
   (withdrawal) with the original signature, and nothing is sent. A channel mint for the
   row that pays a different amount or account parks the row in `manual_review`.
4. **Released nonces must be explained.** The withdraw operator diffs the escrow's
   withdrawal bitmap against the DB at boot. A nonce released on Solana that no row's
   landed, journaled release signature explains refuses boot, because a release names no
   burn and the renumbered rows cannot be told apart. So does a DB whose highest nonce is in
   a bitmap generation the chain has rotated past.
5. **Startup reconciliation** - the escrow indexer still runs its balance reconciliation on
   startup (unless in backfill-only mode).

## What a refusal after a restore means

**Channel fence.** The withdraw indexer stores the hash of the newest channel block at or
below its checkpoint (`indexer_state.fence_slot`, `fence_blockhash` on the `withdraw` row).
At boot the withdraw indexer and both operators wait until the channel tip reaches that
slot and then compare the block. The withdraw indexer also checks that every new block
links to the previous one, and the operators re-check the fence on every recovery tick
(60 seconds). A restored channel re-produces the same slots with new hashes, so any of
these fails. The log reads `channel fence check failed: ...`, and the metric
`private_channel_channel_fence_mismatch_total` increases.

- Cause: the primary was restored without the indexer, or the indexer was restored to a
  target later than the primary's.
- Fix: stop everything again and restore `postgres-indexer` to a target at least 60
  seconds before the primary's target. Do not clear the fence columns by hand: that only
  hides the skew, and the money it protects is real.
- Funds already affected before the refusal (credits rolled back on the channel, or
  withdrawals paid for burns the channel no longer has) are not repaired automatically.
  Treat them through [withdrawal_pipeline_halt_runbook.md](runbooks/withdrawal_pipeline_halt_runbook.md#channel-fence-refused).

`channel fence could not be checked: ...` means the RPC did not answer, the tip stayed
below the fence slot for 10 minutes, or the fence block was pruned by `truncate` (the
message says so). A pruned fence block can only happen if the withdraw indexer was stopped
or lagging for longer than the retention window. Confirm that no primary restore happened
since the indexer last ran, then set `fence_slot` and `fence_blockhash` to NULL on the
`withdraw` row of `indexer_state`; the next checkpoint flush writes a new fence. This is
the only case where clearing the fence by hand is right. If the RPC answers and the tip is far below the fence
slot, that is almost certainly a primary restore that rolled back more than 10 minutes:
treat it as a fence refusal. Otherwise restore the channel RPC; the next restart retries.

A fence that is null (first boot after upgrade, or right after a withdraw resync) is not
checked; the first checkpoint flush writes it.

**Unexplained consumed nonce.** `Withdrawal nonces [...] are consumed on-chain but no row's
landed release signature explains them` or `The database's highest withdrawal nonce is in
generation ...`: the indexer DB was restored to before releases or a bitmap rotation that
the chain has. See
[withdrawal_pipeline_halt_runbook.md](runbooks/withdrawal_pipeline_halt_runbook.md#unexplained-consumed-nonce-on-startup).

**Consumed set unavailable.** `consumed set unavailable: ...` at operator boot: a mint's
channel history could not be read, or it ends before the newest mint the DB knows, or it
holds a memo in the retired scheme. History that ends early is accepted when `truncate`
pruned it (a warning is logged); a channel serving its whole history that lacks the known
mint was rewound. Otherwise fix the RPC.

## Backup Verification

### Check WAL archiving is active

```bash
# Should show WAL files accumulating:
docker run --rm -v private-channel_postgres-primary-wal-archive:/archive alpine ls -la /archive/ | tail -5
docker run --rm -v private-channel_postgres-indexer-wal-archive:/archive alpine ls -la /archive/ | tail -5
```

### Check base backups exist

```bash
docker run --rm -v private-channel_postgres-primary-basebackups:/backups alpine ls -la /backups/
docker run --rm -v private-channel_postgres-indexer-basebackups:/backups alpine ls -la /backups/
```

### Check sidecar logs

```bash
docker compose logs pg-backup-primary --tail 20
docker compose logs pg-backup-indexer --tail 20
```

### Smoke-test PITR

1. Insert a test row with a known timestamp
2. Note the current time (recovery target)
3. Insert another row after the target time
4. Perform PITR to the target time (steps 1-6 above)
5. Verify: first row present, second row absent
