# Solana Private Channels Integration Tests

This package contains integration tests for the Solana Private Channels stack.

## Structure

- `tests/private-channel/` — node and JSON-RPC end-to-end tests. `integration.rs` is the main suite and pulls in the per-method tests under `rpc/`; standalone targets (backpressure, ordered shutdown, truncate, sparse chain, duplicate writer) sit alongside it.
- `tests/indexer/` — indexer and operator tests. Shared code lives in `tests/indexer/helpers/` and `tests/indexer/setup.rs`.
- `tests/helpers.rs`, `tests/setup.rs` — shared by the private-channel suite via `#[path]`.

Each standalone test binary is declared as its own `[[test]]` target in `Cargo.toml`. Files pulled in by one of those binaries via `#[path]` — most of `tests/private-channel/rpc/`, the `test_truncate_*.rs` set, and several under `tests/indexer/` — are not targets themselves.

## Requirements

- Docker — Postgres and Redis run as testcontainers.
- `cargo-nextest` — install with `make install`.
- Program `.so` artifacts in `test_utils/programs/` and the Yellowstone geyser plugin in `test_utils/geyser/`.

## Running Tests

```bash
make integration-test
```

This is the entry point. Tests are grouped by the escrow `.so` build feature and the target rebuilds the escrow program between groups: production settings for the private-channel group, `--features test-tree` for the indexer group, production again for the rest. `validator_helper` fingerprints the deployed escrow `.so` against the feature the test was compiled with; a mismatch poisons a `std::sync::Once` and cascades failures across every later test in the binary, so running all targets in one pass with plain `cargo test` does not work.

Run a single target with the matching escrow build already in place:

```bash
cargo test --test reconciliation_integration -- --nocapture
cargo test --features test-tree --test indexer_integration -- --nocapture
```

## Coverage

```bash
make integration-coverage
```

Builds the `.so` artifacts, runs both groups under `cargo llvm-cov`, and writes `coverage/coverage-integration-e2e.lcov`. `check-coverage-floor` fails the run if filtered coverage drops below `COVERAGE_FLOOR` (75.0). CI calls `integration-coverage-private-channel`, `integration-coverage-private-channel-redis`, and `integration-coverage-indexer` directly against pre-built artifacts.

## Test Coverage

**Private-channel node** — JSON-RPC surface (blocks, slots, signature statuses, supply, epoch, health), SPL Token and DvP swap flows, transaction validation (empty, mixed admin/non-admin, oversized, address-lookup rejection), replay protection and dedup persistence, simulate-transaction guards, ingress backpressure, ordered shutdown, single-writer leases, truncate, and both the PostgreSQL and Redis backends.

**Indexer and operator** — deposit and withdrawal pipelines end to end, backfill and startup ordering, reconciliation and liability checks, gap detection and fail-closed reconnect, resync, remint and stuck-row recovery, Token-2022 mint gates (pausable, permanent delegate, transfer hook), the Yellowstone and RPC-polling datasources, and sender fault injection against a mock RPC server.
