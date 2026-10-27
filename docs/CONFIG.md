# Configuration & Operations

Reference for configuring, tuning, and operating Solana Private Channels services.

**Note:** When running via Docker Compose, all configuration is set through environment variables in `.env.devnet` (or your environment file). The CLI flags listed below are their equivalent for running binaries directly. You do not need to modify Dockerfiles or container commands — just update your env file and restart the service.

---

## Configuration Reference

### Write Node (`private-channel-node --mode write`)

**Source**: [`core/src/bin/node.rs`](../core/src/bin/node.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--mode` | `PRIVATE_CHANNEL_MODE` | — | Node mode: `read`, `write`, or `aio` (all-in-one). Required, so a dropped variable fails startup instead of starting a read deployment as a writer |
| `--port` | `PRIVATE_CHANNEL_PORT` | `8899` | RPC listen port |
| `--sigverify-workers` | `PRIVATE_CHANNEL_SIGVERIFY_WORKERS` | `4` | Parallel signature verification threads |
| `--sigverify-queue-size` | `PRIVATE_CHANNEL_SIGVERIFY_QUEUE_SIZE` | `1000` | Bounded sigverify→dedup queue |
| `--ingress-queue-capacity` | `PRIVATE_CHANNEL_INGRESS_QUEUE_CAPACITY` | `10000` | Bounded RPC→sigverify queue; a full queue sheds (`sendTransaction` returns `-32003`, retryable) and increments `rpc_ingress_shed_total` |
| `--sequencer-queue-capacity` | `PRIVATE_CHANNEL_SEQUENCER_QUEUE_CAPACITY` | `1000` | Bounded dedup→sequencer queue; a full queue applies upstream backpressure |
| `--execution-results-capacity` | `PRIVATE_CHANNEL_EXECUTION_RESULTS_CAPACITY` | `1000` | Bounded queue from the executor to the settler; a full queue applies upstream backpressure. Also bounded by an internal in-flight byte budget, and the settler stops draining once a tick's buffered account bytes reach a budget of their own |
| `--max-tx-per-batch` | `PRIVATE_CHANNEL_MAX_TX_PER_BATCH` | `64` | Max transactions per sequencer batch |
| `--max-connections` | `PRIVATE_CHANNEL_MAX_CONNECTIONS` | `100` | Max concurrent RPC connections |
| `--blocktime-ms` | `PRIVATE_CHANNEL_BLOCKTIME_MS` | `100` | Tick interval (ms). A block is produced only when the tick carried transactions or the 1s idle heartbeat came due, and the slot it lands on counts every tick since the last block |
| `--max-blockhashes` | `PRIVATE_CHANNEL_MAX_BLOCKHASHES` | `150` | How many **blocks** a blockhash stays valid for. Matches Solana's `MAX_PROCESSING_AGE`. Also the depth of the settled-blockhash queue from the settler to dedup. Write and Aio nodes publish the resolved value to the ledger at every boot; a Read node ignores its own value and advertises the published one, following a writer restarted with a new window within replica lag. A write node refuses to boot with a larger value than the ledger recorded unless `--allow-max-blockhashes-increase` is set |
| `--allow-max-blockhashes-increase` | `PRIVATE_CHANNEL_ALLOW_MAX_BLOCKHASHES_INCREASE` | `false` | Let a write node boot with a larger `--max-blockhashes` than the ledger recorded. Every hash already handed out then stays admissible past the `lastValidBlockHeight` it was advertised with, by the size of the increase, so a client that re-signs at its old deadline can see both land. Use it only when no client holds an unconfirmed transaction, and remove it after that boot |
| `--transaction-expiration-ms` | `PRIVATE_CHANNEL_TRANSACTION_EXPIRATION_MS` | none | **Deprecated.** Expiry as a duration. When set it overrides `--max-blockhashes` with `transaction_expiration_ms / blocktime_ms` and logs a warning. Removed after the next release |
| `--admin-keys` | `PRIVATE_CHANNEL_ADMIN_KEYS` | — | Comma-separated base58 channel admin pubkeys, gating the Token-2022 admin instructions (`InitializeMint`, `InitializeMint2`, `InitializePermanentDelegate`), `MintTo`, `MintToChecked`, `FreezeAccount`, `ThawAccount`, every `SetAuthority` except `AccountOwner` and `CloseAccount`, and the withdraw program's `SetWithdrawConfig`. Not the escrow `Instance.admin`, which is a separate offline key |
| `--permanent-delegate` | `PRIVATE_CHANNEL_PERMANENT_DELEGATE` | — | Base58 pubkey written into every channel mint as its Token-2022 permanent delegate, and the only key the seize rule polices. Required in `write` and `aio` modes; a bad value stops startup. The shipped compose files fall back to `PRIVATE_CHANNEL_ADMIN_KEYS` when it is blank. Must equal the operator's `OPERATOR_PERMANENT_DELEGATE`. Use a dedicated key in production |
| `--compliance-owner` | `PRIVATE_CHANNEL_COMPLIANCE_OWNER` | — | Base58 owner of the compliance ATAs, the only place the delegate may move other owners' balances to (`TransferChecked` only). Unset refuses every delegate move. Must differ from the delegate, in any mode. Blank is read as unset |
| `--accountsdb-connection-url` | `PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL` | — | PostgreSQL connection string. Must be PostgreSQL: Redis is a cache, never the accounts database |
| `--redis-cache-url` | `PRIVATE_CHANNEL_REDIS_CACHE_URL` | — | Optional Redis cache in front of the read path. Misses resolve against PostgreSQL |
| `--redis-block-ttl-secs` | `PRIVATE_CHANNEL_REDIS_BLOCK_TTL_SECS` | `3600` | Expiry on cached `block:{slot}` and `tx:{signature}` entries, bounding cache growth. PostgreSQL is the source of truth, so an expired entry is a miss that re-reads. `0` disables it |
| `--log-level` | `PRIVATE_CHANNEL_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `--json-logs` | `PRIVATE_CHANNEL_JSON_LOGS` | `false` | Structured JSON log output |
| `--perf-sample-period-secs` | `PRIVATE_CHANNEL_PERF_SAMPLE_PERIOD_SECS` | `60` | Performance metrics sampling interval |
| `--metrics` | `PRIVATE_CHANNEL_METRICS` | `false` | Enable Prometheus stage metrics server |
| — | `PRIVATE_CHANNEL_METRICS_PORT` | `9090` | Port for the stage metrics server |

**Startup validation:** The node rejects a missing `--mode`, `blocktime_ms == 0`, `max_blockhashes == 0`, and any write-pipeline queue capacity of `0` (which would panic the channel constructors) to prevent misconfiguration. It also refuses a write or aio node with no permanent delegate, since it could never create a mint, and a compliance owner equal to the delegate, since the delegate's own ATA is exempt from the seize rule.

**Operator delegate:** both operators read `OPERATOR_PERMANENT_DELEGATE` (base58), next to `ADMIN_SIGNER`. Unset or blank, it is the admin signer's pubkey; the shipped compose files fill a blank value from `PRIVATE_CHANNEL_PERMANENT_DELEGATE` first. `operator-solana` creates channel mints with `InitializePermanentDelegate` set to it, followed by `InitializeMint2` with the admin as mint and freeze authority, and never mints into a channel mint whose delegate is missing or another key. It must equal the node's `PRIVATE_CHANNEL_PERMANENT_DELEGATE`, or the node refuses every mint the operator creates.

**Slot, block height and expiry.** They are separate counters, as on Solana. Slots count ticks, so a block lands on a slot that already counts every idle tick before it; `getSlot` reports the live slot, advancing every tick whether or not a block is produced, so most slots carry no block and `getBlock` on one answers `-32007` (`SlotSkipped`), never a `null`. `getBlockHeight` counts blocks actually produced: one per tick that carried transactions, plus one per second while the node is idle. Expiry is block-counted: `lastValidBlockHeight = getBlockHeight() + max_blockhashes - 1`, with the window the write node published, the last height at which the hash is still in the window, so a stock client's `confirmTransaction` loop terminates on exactly the event the node uses to evict it.

The wall-clock duration of the window therefore moves with load, which is also how Solana behaves. At 150 blocks it is about 15 seconds under continuous load and about 2.5 minutes fully idle; a node taking occasional traffic lands between them. Document a range to clients, never a figure.

**Blockhash window and the operators:** the blockhash validity window (`max_blockhashes`) bounds how much history an operator must see retained before it may call a channel signature dead. Operators journal the slot each broadcast's blockhash was read at, so changing the setting needs no operator restart and no operator config. An attempt journaled before that column existed reports the verdict as uncertain rather than dead, because a window narrowed since the broadcast cannot be reconstructed.

**Tuning the queue capacities:** these aren't machine-spec values. Each slot holds one transaction (a few KB), so even the `10000` ingress queue is only ~tens of MB — RAM is never the limit. Size them by traffic, not hardware: a larger ingress queue absorbs bigger bursts but adds latency when backed up. Tune by watching `rpc_ingress_shed_total` — raise `--ingress-queue-capacity` if real bursts are being shed, lower it if latency under load is too high.

### Read Node (`private-channel-node --mode read`)

Uses the same binary with `--mode read` (or `PRIVATE_CHANNEL_MODE=read`). Points to a PostgreSQL replica for read isolation.

`GATEWAY_READ_URL` must be exactly one read node on one replica. Load-balancing several read nodes or replicas behind it is not supported: clients decide when to re-sign by reading `getBlockHeight` and then `getSignatureStatuses`, and that is only safe when both reads see the same replica.

### Gateway

**Source**: [`gateway/src/lib.rs`](../gateway/src/lib.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `GATEWAY_PORT` | `8898` | Listen port |
| `--internal-port` | `GATEWAY_INTERNAL_PORT` | — | Internal listen port; unset means no internal listener |
| `--write-url` | `GATEWAY_WRITE_URL` | — | Write node URL |
| `--read-url` | `GATEWAY_READ_URL` | — | Read node URL |
| `--cors-allowed-origin` | `GATEWAY_CORS_ALLOWED_ORIGIN` | `*` | CORS origin |
| `--upstream-timeout-secs` | `GATEWAY_UPSTREAM_TIMEOUT_SECS` | `30` | Max seconds (1 to 3600) per request, from authorization (auth database and ownership fetch) until the response has been written to the client's socket. Past it the caller gets a 504, or, once the body has started, the connection is closed; either way the read slot, connection slot and upstream socket are released. It applies whether the node stalls or the client reads slowly or stops reading, so a slow client can show as `upstream_timeout` even when the node answered promptly |
| `--max-forwarded-reads` | `GATEWAY_MAX_FORWARDED_READS` | `768` | Max public requests in flight to the read node; more are shed at once with a 503 and a closed connection, so writes keep free connection slots. Must be below `GATEWAY_MAX_CONNECTIONS` |
| `--read-node-pg-max-connections` | `PRIVATE_CHANNEL_PG_MAX_CONNECTIONS` | `32` | The read node's Postgres pool size, read exactly as the read node reads it: unset, empty, unparseable or zero means 32, and values above 256 are clamped. Public `getBlocks` and `getBlocksWithLimit` calls the read node is still answering, of any span, are capped at a quarter of it (at least 1), and one client (an IPv4 address or an IPv6 /64) may hold at most a quarter of those slots until it has read its responses; more are shed at once with a 503 and a closed connection. Set it to the same value the read node gets: the shipped compose files pass the gateway the same variable. A gateway deployed without it assumes 32 |
| `--whitelist-rpc-url` | `GATEWAY_WHITELIST_RPC_URL` | — | Mainnet RPC for the withdraw whitelist check. With `GATEWAY_WHITELIST_INSTANCE`, a public `sendTransaction` with a `WithdrawFunds` is refused with a 403 when the escrow whitelists withdrawals and the withdrawer or destination is not allowed to withdraw. It reads the accounts at `confirmed` with a 2 s timeout and forwards the transaction if the read fails. Reads are cached for 5 s, so a whitelist change reaches the check up to 5 s late. Use an RPC key only the gateway uses. Setting only one of the two refuses to start; both unset or blank leaves the check off. Each withdrawal the check could not run on logs `Withdraw whitelist not checked` at warn level; the boot line `enabled` only means both settings are set |
| `--whitelist-instance` | `GATEWAY_WHITELIST_INSTANCE` | — | Escrow instance PDA the withdraw whitelist check reads. Use the same value as `COMMON_ESCROW_INSTANCE_ID` |
| `--whitelist-reads-per-second` | `GATEWAY_WHITELIST_READS_PER_SECOND` | `20` | Most Mainnet reads per second the withdraw whitelist check makes, across all clients. Past it, a withdrawal that needs a read is forwarded unchecked. Size it below the RPC key's plan |

Routes `sendTransaction` and `isBlockhashValid` to the write node; all other RPC methods go to the read node.

The internal port serves the operator's own services: no RBAC, no rate limiting,
and transaction errors are not collapsed. It must never be published to the host.
Compose gives it no `ports:` entry, which is the only thing keeping it internal.

**Internal services must never use `GATEWAY_URL`.** They carry no JWT, so once
RBAC is on the public port answers 401 to every gated read (`getBlock`,
`getAccountInfo`, `getSignaturesForAddress`) and indexing or minting stalls while
the stack still looks healthy. Use:

- `GATEWAY_INTERNAL_URL` if the service also sends transactions, since only the
  gateway routes writes to the write node.
- `GATEWAY_READ_URL` if it only reads. The read node has no auth layer at all.

### Streamer

**Source**: [`core/src/bin/streamer.rs`](../core/src/bin/streamer.rs)

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `STREAMER_PORT` | `8902` | WebSocket listen port |
| `--accountsdb-connection-url` | `STREAMER_ACCOUNTSDB_CONNECTION_URL` | — | Solana Private Channels DB connection |
| `--poll-interval-ms` | `STREAMER_POLL_INTERVAL_MS` | `700` | DB polling interval (ms) |
| `--cors-allowed-origin` | `STREAMER_CORS_ALLOWED_ORIGIN` | `*` | CORS origin |

Exposes `/ws` for real-time transaction streaming and `/health` for health checks.

#### Accessing the streamer (internal-only)

The streamer feed is unauthenticated, so it is **not published to the host**. It listens only on the
`private-channel-network` Docker network at `ws://streamer:8902/ws` (`streamer` is the Compose service
DNS name; container `private-channel-streamer`).

To listen to the feed, attach a throwaway WebSocket client to the network:

```shell
docker run --rm --network private-channel_private-channel-network solsson/websocat ws://streamer:8902/ws
```

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

1. **Dedup cache rebuild**: Reads the last `max_blockhashes` blocks and reconstructs the signature dedup cache. This prevents duplicate transaction execution after restart. The window is block-counted, so age is not consulted: those blocks are exactly the hashes still inside the `lastValidBlockHeight` clients were handed, however long ago an idle node produced them. What the rebuild uses is always an unbroken run of block heights ending at the durable `block_height` counter, and how a broken window is handled depends on where it breaks.

   Blocks missing from the oldest end are fine: truncation deletes oldest first, and a transaction from a deleted block can only name a blockhash that was deleted with it. A block missing from the *middle* is survivable the same way, once the blocks at or below it are dropped as well: the node keeps only the contiguous suffix above the gap and logs a `WARNING` naming the heights either side of it. This is safe, because those blockhashes leave the live set and a transaction naming one is rejected as an unknown blockhash before the dedup cache is ever consulted. It is not free: such a transaction is refused inside the `lastValidBlockHeight` its client was handed, so the warning is a data loss signal worth alerting on, and the missing rows are worth investigating.

   A block missing from the *top* of the window is still fatal, and so is a missing `block_height` counter, a `block_height` counter behind the newest stored block, or rows missing while the counter claims a chain. Those blocks' transactions named blockhashes that survive, so the hashes stay live with no record of what already used them, which is exactly the replay hole. The node refuses to start. For a missing block at the top the recovery is to restore from backup; for a lost `block_height` counter it is not, since a backup of the same ledger is missing the same metadata key, so the counter is put back from the newest stored block's own height once that block is confirmed to be the chain tip. Failure here is fatal: the node will not start with an empty cache if blocks exist in the DB.

2. **Settlement state**: Reads the highest block row from PostgreSQL and resumes block production from it, taking the slot, the blockhash and the block height from that one row so they always describe the same block. The durable `block_height` and `current_slot` counters override it when present, so an idle node resumes past the ticks it reached rather than from its last block. Failure here is fatal - the node will not start when the tip cannot be read, because treating an unreadable tip as an empty ledger would restart a live chain from slot 0.

3. **Redis cache alignment** (optional): If `--redis-cache-url` is configured, checks that the cache belongs to this ledger before the first write to it. The same flag wires the read path, so a writer cannot stop mirroring a cache that readers still trust. Each PostgreSQL database is stamped with a `deployment_id` at creation and the cache carries a copy; a cache naming a different deployment, or whose tip does not match PostgreSQL exactly, is emptied. The chain tip is then published and the cache stamped. Account, block and transaction state is not preloaded: a key missing from the cache is a miss that resolves against PostgreSQL. The two node roles differ on failure: a write node that cannot verify the cache runs PostgreSQL-only rather than writing a second ledger into it, holding the cache paused for the same probe that recovers one lost at runtime, while a read node refuses to start, because it cannot purge the cache itself and has no business serving what it found. A write node that cannot *connect* to Redis at startup has no cache handle to hold, so it runs PostgreSQL-only until it is restarted; `private_channel_redis_cache_disabled_total` is the signal.

   The same stamp governs the cache while the node runs. Every cached read checks it, so clearing it takes the cache out of service on the next read. If a settled batch fails to reach the cache, the cached tip stops advancing; the next batch notices, clears the stamp and rebuilds the cache in the background, after which it is stamped again and serves normally. A cache purged on every startup usually means two deployments sharing one Redis instance. `private_channel_redis_cache_purged_total` is the signal. A cache that keeps failing or stops answering is given up on once it has failed more batches than it has made up, after which the node runs PostgreSQL-only until a cooldown expires. It then probes the cache with a PING rather than a batch, off the block path; one that answers is purged and rebuilt before it is mirrored to again, and one that does not is left alone for another cooldown. `private_channel_redis_cache_disabled_total` counts the give-ups.

The write node does not use a WAL — all state is deterministically recoverable from the PostgreSQL block history.

### Indexer

On restart, the indexer compares its last checkpoint slot against the current on-chain slot. If the gap exceeds the configured threshold, it triggers a parallel backfill before switching to real-time mode. See [Indexer Architecture](INDEXER.md) for details.

The durable checkpoint is always the lower bound. `indexer.backfill.start_slot` (and `indexer.rpc_polling.start_slot` when backfill is off) only picks the starting point on a database that has never been indexed; set above an existing checkpoint it would skip real slots, so the indexer refuses to start. See [`indexer_start_slot_ahead_of_checkpoint.md`](runbooks/indexer_start_slot_ahead_of_checkpoint.md). A start slot of `0` begins at slot 1, because proving a slot empty needs a predecessor to anchor on and genesis has none.

## Operational Tools

### Admin CLI

**Source**: [`core/src/bin/admin.rs`](../core/src/bin/admin.rs)

The `private-channel-admin` binary provides database maintenance commands:

```shell
# Take a custom-format dump of the primary first; truncation refuses without one
pg_dump -Fc -f ledger.dump "$PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL"

# Truncate old blocks/transactions
private-channel-admin truncate --keep-slots 100000 --pg-dump-path ledger.dump \
  --withdraw-indexer-db-url "$WITHDRAW_INDEXER_DB_URL"

# Dry run to preview what would be deleted (proves the dump and runs every check too)
private-channel-admin truncate --keep-slots 100000 --pg-dump-path ledger.dump \
  --withdraw-indexer-db-url "$WITHDRAW_INDEXER_DB_URL" --dry-run
```

Nothing is deleted, not even in a dry run, until the dump is proven to hold every
row the run would delete. The proof reads the whole dump once through `pg_restore`
and requires: a clean exit with nothing on stderr, data for `blocks`,
`transactions`, `address_signatures`, `metadata` (and `account_history` if present), the live
database's `deployment_id`, and exactly the live number of blocks and
`account_history` rows below the cutoff. The report prints the dump's SHA-256 so it
can be matched against the backup you keep. WAL archiving and file age are not
accepted as proof.

- Only custom format (`-Fc`) is accepted, and `pg_restore` must be at least the
  server's major version.
- The runtime image ships no PostgreSQL client. Run `admin` from a host with PG16
  client tools, or pass `--pg-restore-bin` pointing at a wrapper such as
  `docker exec -i <pg16 container> pg_restore "$@"`.
- Run it against the primary, never a replica.
- `--max-backup-age-hours` is deprecated and ignored.

Truncation deletes in batches, and each batch commits the new retention floor
(`getFirstAvailableBlock`) in the same transaction as its deletions. A partial or
aborted run therefore never advertises history it has already removed. Each batch
also deletes the `address_signatures` rows of the transactions it removes, so the
dump must hold that table too. Rows left by truncations run before this change stay
until removed once by hand: `DELETE FROM address_signatures WHERE slot < <getFirstAvailableBlock>;`

**Truncation always keeps `2 x max_blockhashes` blocks.** `--keep-slots` is a
slot span, but `truncate` refuses, dry runs included, any cut that leaves fewer
than `2 x max_blockhashes` blocks. Blocks are counted from the `blocks` table and
the window is read from the ledger, where the write node publishes its resolved
value (deprecated `--transaction-expiration-ms` override applied) at every boot,
so a ledger whose writer has not yet booted on this build refuses every cut. The
refusal names the smallest `--keep-slots` that passes. The newest
`max_blockhashes` blocks are the dedup window a restart rebuilds; the next
`max_blockhashes` keep a landed transaction's status readable for at least that
many blocks past its `lastValidBlockHeight`, which is the grace the client retry
rule relies on (see `getSignatureStatuses` in
[RPC_COMPATIBILITY_ANALYSIS.md](RPC_COMPATIBILITY_ANALYSIS.md)). Blocks are
sparse relative to slots: an idle node produces one per second, so on an idle
ledger the floor is about `2 x max_blockhashes x ceil(1000 / blocktime_ms)`
slots, 3000 at the defaults. A job sized by the old 1500 figure is refused, so
read the refusal rather than computing it. `--max-blockhashes` and
`--blocktime-ms` are still accepted and ignored, but every existing invocation
must now add `--withdraw-indexer-db-url` or `--skip-withdraw-indexer-check`
(below) or it fails to parse. A node that boots into a window shorter than
`max_blockhashes` (after a manual delete, a restore, or an allowed window
increase) logs the shortfall at warn and still starts, since the missing hashes
are rejected as unknown either way.

The grace beyond the deadline is only `max_blockhashes` blocks, about 15s under
load. Channel operators decide a stuck mint or remint from the same retained
history, so truncating while an operator is down can turn an attempt it would
have proven dead into a manual review. Truncate while the operators are running.

**Upgrade order.** Upgrade and boot the write node first. Until a write node has
booted on this build the ledger holds no published window: read nodes answer
`getLatestBlockhash` with a `-32000` error and truncation refuses. Do not change
`--max-blockhashes` while a write node runs an older build, which does not
publish it, or read nodes keep advertising the stale value.

**Truncation never cuts past the withdraw indexer.** The withdraw indexer reads
core history, and a block deleted before it has read it wedges it until history
is restored. Pass `--withdraw-indexer-db-url` (env
`PRIVATE_CHANNEL_WITHDRAW_INDEXER_DB_URL`) pointing at the indexer database
(`postgres-indexer`, so the admin host must reach it); `truncate` refuses unless
`indexer_state.last_committed_slot` for `withdraw` is at least the cut slot minus
one, and `fence_slot`, when set, is at or above the cut: the withdraw indexer and
the operator both refuse to boot once the channel fence block is gone. A missing row, an unset slot (an indexer that has never anchored), an
unreachable database and a database without `indexer_state` all refuse. A role
with `SELECT` on `indexer_state` alone is enough and is preferred over
`POSTGRES_INDEXER_USER`. A deployment with no withdraw indexer passes
`--skip-withdraw-indexer-check` instead, which logs a warning. The two flags are
exclusive, and the env var counts as the URL, so unset it when skipping.

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
| `scripts/devnet/devnet-test.sh` | Full E2E test: instance creation through deposit/withdrawal/backfill validation |
