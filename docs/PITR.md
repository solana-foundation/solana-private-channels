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

## Restore Procedure

### Prerequisites

- Docker Compose access to the target environment
- Identify the **target recovery time** (UTC timestamp)
- Identify which database to restore (`primary` or `indexer`)

> **Note:** The Compose project name is `private-channel` (set via the `name:` key
> in `docker-compose.yml`), so volumes are prefixed `private-channel_` and containers
> `private-channel-`. Run `docker volume ls | grep postgres` to confirm. All
> `docker compose` commands below assume you're in the repo root (they resolve
> `docker-compose.yml` and the `private-channel` project automatically); if compose
> reports unset variables, prepend the standard env chain
> `--env-file versions.env --env-file .env.local`, or use the guarded `make docker-*`
> targets.

### Step 1: Stop the database and dependents

```bash
# For postgres-primary:
docker compose stop streamer write-node read-node postgres-replica postgres-primary pg-backup-primary

# For postgres-indexer:
docker compose stop indexer-solana indexer-private-channel operator-solana operator-private-channel streamer pg-backup-indexer postgres-indexer
```

> `streamer` depends on both `postgres-replica` (accounts DB) and `postgres-indexer` (indexer DB), so it must be stopped for either restore scenario.

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

### Step 6: Restart dependents

```bash
# For postgres-primary:
docker compose up -d postgres-replica write-node read-node streamer pg-backup-primary

# For postgres-indexer:
docker compose up -d indexer-solana indexer-private-channel operator-solana operator-private-channel streamer pg-backup-indexer
```

> **Note:** The postgres-replica will need to re-sync from scratch after a primary PITR. Delete its data volume if it fails to start:
> ```bash
> docker compose stop postgres-replica
> docker run --rm -v private-channel_postgres-replica-data:/data alpine sh -c "rm -rf /data/*"
> docker compose up -d postgres-replica
> ```

## Indexer-Specific Notes

Restoring `postgres-indexer` via PITR is safe:

1. **Checkpoint lag tolerance** — `indexer_state.last_committed_slot` is updated asynchronously (batched every ~5 seconds), not within the same transaction as data inserts. After PITR, the checkpoint may lag behind the actual indexed data. The indexer resumes from the checkpoint slot and re-processes transactions that were already indexed, which is safe because inserts are idempotent (see point 2).
2. **Idempotent inserts** — All transaction inserts use `ON CONFLICT (signature) DO NOTHING`. Replaying already-indexed transactions is a no-op.
3. **Startup reconciliation** — The escrow indexer runs a balance reconciliation check on startup (unless running in backfill-only mode), catching any drift between on-chain and indexed state. Non-escrow program types (e.g., withdraw) skip reconciliation.
4. **Worst case** — Wasted re-indexing work, not data corruption.

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
