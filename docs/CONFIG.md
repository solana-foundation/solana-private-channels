# Configuration & Operations

Reference for configuring, tuning, and operating Solana Private Channels services.

**Note:** When running via Docker Compose, all configuration is set through environment variables in `.env.devnet` (or your environment file). The CLI flags listed below are their equivalent for running binaries directly. You do not need to modify Dockerfiles or container commands — just update your env file and restart the service.

---

## Configuration Reference

### Write Node (`private-channel-node --mode write`)

**Source**: [`core/src/bin/node.rs`](../core/src/bin/node.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--mode` | `PRIVATE_CHANNEL_MODE` | `aio` | Node mode: `read`, `write`, or `aio` (all-in-one) |
| `--port` | `PRIVATE_CHANNEL_PORT` | `8899` | RPC listen port |
| `--sigverify-workers` | `PRIVATE_CHANNEL_SIGVERIFY_WORKERS` | `4` | Parallel signature verification threads |
| `--sigverify-queue-size` | `PRIVATE_CHANNEL_SIGVERIFY_QUEUE_SIZE` | `1000` | Bounded queue between dedup and sigverify |
| `--max-tx-per-batch` | `PRIVATE_CHANNEL_MAX_TX_PER_BATCH` | `64` | Max transactions per sequencer batch |
| `--max-connections` | `PRIVATE_CHANNEL_MAX_CONNECTIONS` | `100` | Max concurrent RPC connections |
| `--blocktime-ms` | `PRIVATE_CHANNEL_BLOCKTIME_MS` | `100` | Settlement interval (ms) |
| `--transaction-expiration-ms` | `PRIVATE_CHANNEL_TRANSACTION_EXPIRATION_MS` | `15000` | Transaction lifetime before dedup eviction |
| `--admin-keys` | `PRIVATE_CHANNEL_ADMIN_KEYS` | — | Comma-separated base58 pubkeys for admin operations |
| `--accountsdb-connection-url` | `PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL` | — | PostgreSQL connection string |
| `--log-level` | `PRIVATE_CHANNEL_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `--json-logs` | `PRIVATE_CHANNEL_JSON_LOGS` | `false` | Structured JSON log output |
| `--perf-sample-period-secs` | `PRIVATE_CHANNEL_PERF_SAMPLE_PERIOD_SECS` | `60` | Performance metrics sampling interval |
| `--metrics` | `PRIVATE_CHANNEL_METRICS` | `false` | Enable Prometheus stage metrics server |
| — | `PRIVATE_CHANNEL_METRICS_PORT` | `9090` | Port for the stage metrics server |

**Startup validation:** The node rejects `blocktime_ms == 0` and `transaction_expiration_ms < blocktime_ms` to prevent misconfiguration.

### Read Node (`private-channel-node --mode read`)

Uses the same binary with `--mode read` (or `PRIVATE_CHANNEL_MODE=read`). Points to a PostgreSQL replica for read isolation.

### Gateway

**Source**: [`gateway/src/lib.rs`](../gateway/src/lib.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `GATEWAY_PORT` | `8898` | Listen port |
| `--write-url` | `GATEWAY_WRITE_URL` | — | Write node URL |
| `--read-url` | `GATEWAY_READ_URL` | — | Read node URL |
| `--cors-allowed-origin` | `GATEWAY_CORS_ALLOWED_ORIGIN` | `*` | CORS origin |

Routes `sendTransaction` to the write node; all other RPC methods go to the read node.

### Streamer

**Source**: [`core/src/bin/streamer.rs`](../core/src/bin/streamer.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` (fallback: `STREAMER_PORT`) | `8902` | WebSocket listen port |
| `--accountsdb-connection-url` | `STREAMER_ACCOUNTSDB_CONNECTION_URL` | — | Solana Private Channels DB connection |
| `--poll-interval-ms` | `STREAMER_POLL_INTERVAL_MS` | `700` | DB polling interval (ms) |
| `--cors-allowed-origin` | `STREAMER_CORS_ALLOWED_ORIGIN` | `*` | CORS origin |

Exposes `/ws` for real-time transaction streaming and `/health` for health checks.

## Changing Configuration

Set the corresponding environment variable in your `.env.devnet` file (or equivalent) and restart the service:

```shell
# Example: increase sigverify workers to 16
PRIVATE_CHANNEL_SIGVERIFY_WORKERS=16

# Restart the write node to pick up changes
docker compose -f docker-compose.devnet.yml --env-file versions.env --env-file .env.devnet up -d write-node
```

## Restart & Recovery

### Write Node

On restart, the write node recovers state from PostgreSQL before accepting transactions:

**Source**: [`core/src/stages/dedup.rs`](../core/src/stages/dedup.rs), [`core/src/stages/settle.rs`](../core/src/stages/settle.rs)

1. **Dedup cache rebuild**: Reads the last N blocks (where N = `transaction_expiration_ms / blocktime_ms`) and reconstructs the signature dedup cache. This prevents duplicate transaction execution after restart. Failure here is fatal — the node will not start with an empty cache if blocks exist in the DB.

2. **Settlement state**: Queries `latest_slot` and `latest_blockhash` from the database to resume block production from the correct point.

3. **Redis cache warming** (optional): If `REDIS_URL` is configured, preloads the latest account state from PostgreSQL into Redis on startup.

The write node does not use a WAL — all state is deterministically recoverable from the PostgreSQL block history.

### Indexer

On restart, the indexer compares its last checkpoint slot against the current on-chain slot. If the gap exceeds the configured threshold, it triggers a parallel backfill before switching to real-time mode. See [Indexer Architecture](INDEXER.md) for details.

## Operational Tools

### Admin CLI

**Source**: [`core/src/bin/admin.rs`](../core/src/bin/admin.rs)

The `private-channel-admin` binary provides database maintenance commands:

```shell
# Truncate old blocks/transactions (requires recent backup)
private-channel-admin truncate --keep-slots 100000

# Dry run to preview what would be deleted
private-channel-admin truncate --keep-slots 100000 --dry-run
```

### Makefile Targets

**Source**: [`Makefile`](../Makefile)

| Target | Description |
|--------|-------------|
| `make build` | Build all components |
| `make fmt` | Format and lint all code |
| `make unit-test` | Run unit tests |
| `make all-test` | Run all unit + integration tests |
| `make build-devnet` | Build programs for devnet |
| `make deploy-devnet` | Deploy programs to devnet |
| `make generate-clients` | Generate IDL and TypeScript/Rust clients |
| `make obs-up` / `make obs-down` | Start/stop observability stack (Prometheus, Grafana, cAdvisor) |
| `make obs-devnet-up` / `make obs-devnet-down` | Start/stop devnet observability stack |
| `make docker-up` / `make docker-down` | Start/stop the full local stack (wraps the `--env-file versions.env --env-file .env.local` chain) |
| `make docker-build` / `make docker-rebuild` | Build images / rebuild and restart the full local stack |
| `make docker-devnet-up` / `make docker-devnet-down` | Start/stop the full devnet stack (reads `.env.devnet`) |
| `make install-buildkit-cache` | One-time setup: install BuildKit GC config into `/etc/docker/daemon.json` (required before first `docker-build`) |

### Operational Scripts

**Source**: [`scripts/`](../scripts/)

| Script | Description |
|--------|-------------|
| `scripts/ensure-operator-keypair.sh` | Generate operator keypair if missing |
| `scripts/update-admin-env.sh` | Update `.env` with admin pubkey |
| `scripts/reconcile-escrow-balance.sh` | Reconcile on-chain vs DB escrow balances (supports alert webhooks) |
| `scripts/devnet/devnet-test.sh` | Full E2E test: instance creation through deposit/withdrawal/backfill validation |

