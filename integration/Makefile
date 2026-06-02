.PHONY: install integration-test fmt check-coverage-floor
.PHONY: integration-coverage integration-coverage-private-channel integration-coverage-private-channel-redis integration-coverage-indexer integration-coverage-report

COVERAGE_DIR ?= ../coverage

install:
	@echo "Installing integration test dependencies..."
	@cargo install cargo-nextest@0.9.130 --locked

# Run integration tests.
#
# Tests are grouped by escrow `.so` feature to avoid the validator_helper
# fingerprint-poisoning issue (see `integration-coverage-indexer` for the
# detailed write-up). Order:
#
#   1. private_channel group        (prod escrow)
#   2. test-tree group     (rebuild escrow with `--features test-tree`)
#   3. prod-indexer group  (rebuild escrow back to prod)
#
# Keep this list in sync with the corresponding `integration-coverage-*`
# targets — same set of `[[test]]` entries, same group assignment.
integration-test: export PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS=600
integration-test:
	@echo "=== Building escrow with prod settings for private_channel group ==="
	@$(MAKE) -C ../private-channel-escrow-program build
	@echo "=== private_channel group (prod escrow) ==="
	@cargo nextest run --test private_channel_integration
	@cargo test --test test_rpc_http_malformed -- --nocapture
	@cargo test --test test_simulate_transaction_guards -- --nocapture
	@cargo test --test test_health_endpoint_standalone -- --nocapture
	@cargo test --test test_sequencer_zero_deadline -- --nocapture
	@cargo test --test test_signatures_corruption_guard -- --nocapture
	@cargo test --test test_node_config_validation -- --nocapture
	@cargo test --test test_redis_cache_path -- --nocapture --test-threads=1
	@echo "=== Building escrow with test-tree for test-tree-feature group ==="
	@$(MAKE) -C ../private-channel-escrow-program build-test
	@echo "=== test-tree feature group ==="
	@cargo test --features test-tree --test indexer_integration -- --nocapture
	@cargo test --features test-tree --test operator_lifecycle_integration -- --nocapture
	@cargo test --features test-tree --test withdrawal_null_nonce -- --nocapture
	@echo "=== Rebuilding escrow in prod for prod-feature indexer group ==="
	@$(MAKE) -C ../private-channel-escrow-program build-no-clients
	@echo "=== prod-feature indexer group ==="
	@cargo test --test reconciliation_integration -- --nocapture
	@cargo test --test mint_idempotency_integration -- --nocapture
	@cargo test --test gap_detection_integration -- --nocapture
	@cargo test --test truncate_integration -- --nocapture
	@cargo test --test pausable_mint_integration -- --nocapture
	@cargo test --test permanent_delegate_mint_integration -- --nocapture
	@cargo test --test resync_integration -- --nocapture
	@cargo test --test reconciliation_e2e_test -- --nocapture
	@cargo test --test mock_rpc_retry -- --nocapture
	@cargo test --test checkpoint_partial_flush -- --nocapture
	@cargo test --test remint_recovery -- --nocapture
	@cargo test --test bootstrap_validation -- --nocapture
	@cargo test --test yellowstone_wiring -- --nocapture
	@cargo test --test malformed_yellowstone_update -- --nocapture
	@cargo test --test yellowstone_reconnect_gap -- --nocapture
	@cargo test --test yellowstone_inner_and_unknown -- --nocapture
	@cargo test --test harness_sanity -- --nocapture
	@cargo test --test sender_mint_idempotency -- --nocapture
	@cargo test --test sender_mint_validator_encodings -- --nocapture
	@cargo test --test sender_mint_signature_failures -- --nocapture
	@cargo test --test sender_poll_rpc_error -- --nocapture
	@cargo test --test sender_sign_and_send_error -- --nocapture
	@cargo test --test sender_max_retries -- --nocapture
	@cargo test --test sender_onchain_error_arms -- --nocapture
	@cargo test --test jit_mint_helper -- --nocapture
	@cargo test --test storage_update_failure -- --nocapture
	@cargo test --test processor_quarantine -- --nocapture
	@cargo test --test state_recovery_malformed -- --nocapture
	@cargo test --test remint_flow -- --nocapture
	@cargo test --test mint_builder_validation -- --nocapture
	@cargo test --test sender_channel_close -- --nocapture
	@cargo test --test sender_cancellation_drain -- --nocapture

# Format and lint code
fmt:
	@echo "Formatting integration tests..."
	@cargo fmt --all
	@cargo clippy --all-targets -- -D warnings

###################################
# Coverage
###################################
# NOTE: Integration tests require pre-built program .so files in target/deploy/.
# CI builds these separately; do NOT rebuild them here.
#
# Uses --no-report for individual runs, then combines with cargo llvm-cov report.
# Excludes test/test_utils code so we measure coverage of core/indexer source only.

# ──────────────────────────────────────────────────────────────────────────────
# Coverage ignore list — E2E Integration row
# ──────────────────────────────────────────────────────────────────────────────
# Each entry in COV_IGNORE_PATTERNS is a POSIX regex matched against absolute
# source paths in the merged LCOV. Files matching any pattern are dropped
# from both `LF` and `LH` counters — they affect neither the E2E Integration
# percentage nor the denominator.
#
# Add a file here ONLY when at least one of the three categories below fits.
# Removing an entry means "a test for this file is now expected."
#
# ┌──────┬────────────────────────────────────────────────────────────────────┐
# │ cat  │ meaning                                                            │
# ├──────┼────────────────────────────────────────────────────────────────────┤
# │ (i)  │ test infrastructure, not product code (always ignore)              │
# │ (a)  │ boilerplate / types / SDK glue with no real branching logic        │
# │ (b)  │ bindings to external services CI cannot exercise (HSMs, etc.)      │
# │ (c)  │ already comprehensively covered by in-crate unit tests; integration│
# │      │ duplication adds no signal                                         │
# └──────┴────────────────────────────────────────────────────────────────────┘
#
# Rationale table (keep in sync with COV_IGNORE_PATTERNS below):
#
#   pattern                              │ cat │ why ignored
#   ─────────────────────────────────────┼─────┼───────────────────────────────
#   ^tests/                              │ (i) │ test fixtures, not product
#   /test_utils/                         │ (i) │ test fixtures, not product
#   \.cargo/registry                     │ (i) │ third-party deps
#   src/bin/                             │ (i) │ CLI entry points (main())
#   shutdown_utils\.rs                   │ (a) │ graceful-shutdown plumbing
#   metrics\.rs                          │ (a) │ metric-macro expansions
#   test_helpers\.rs                     │ (a) │ test-only helpers in src/
#   operator/constants\.rs               │ (a) │ pure const definitions
#   accounts/error\.rs                   │ (a) │ error-enum Display/From impls
#   accounts/types\.rs                   │ (a) │ StoredTransaction wrapper
#   accounts/mod\.rs                     │ (a) │ bpf_loader_* account factories
#   rpc/get_recent_blockhash_impl\.rs    │ (a) │ default-value fallbacks only
#   core/src/client\.rs                  │ (a) │ deprecated RPC client stub
#   operator/utils/signer_util\.rs       │ (b) │ Vault/Turnkey/Privy HSM paths
#   core/src/processor\.rs               │ (b) │ BPF host-function stubs
#   scheduler/greedy\.rs                 │ (c) │ unit-tested in-file
#   scheduler/dag\.rs                    │ (c) │ unit-tested in-file
#   core/src/webhook\.rs                 │ (c) │ wiremock unit tests cover all
#   accounts/redis\.rs                   │ (c) │ optional backend, unit-tested
#   storage/common/storage/mock\.rs      │ (i) │ MockStorage implementation — test infra only
#   storage/common/storage/.*\.rs        │ (a) │ zero-logic dispatch forwarders; real SQL in
#                                        │     │ postgres/db.rs. ⚠ remove if any file here
#                                        │     │ grows beyond a single-match dispatch
#
# When bumping any entry: update the rationale row *and* the pattern list.

COV_IGNORE_PATTERNS := \
    ^tests/ \
    /test_utils/ \
    \.cargo/registry \
    src/bin/ \
    shutdown_utils\.rs \
    metrics\.rs \
    test_helpers\.rs \
    operator/constants\.rs \
    accounts/error\.rs \
    accounts/types\.rs \
    accounts/mod\.rs \
    rpc/get_recent_blockhash_impl\.rs \
    core/src/client\.rs \
    operator/utils/signer_util\.rs \
    core/src/processor\.rs \
    scheduler/greedy\.rs \
    scheduler/dag\.rs \
    core/src/webhook\.rs \
    accounts/redis\.rs \
    storage/common/storage/.*\.rs

# Collapse whitespace-separated patterns into a single alternation regex.
# e.g. "a b c" → "(a|b|c)"
COV_IGNORE_REGEX := ($(shell echo '$(COV_IGNORE_PATTERNS)' | tr -s ' \t\n' '|' | sed 's/^|//;s/|$$//'))

integration-coverage-private-channel:
	@echo "Running private_channel integration tests with coverage instrumentation..."
	@cargo llvm-cov clean --workspace
	@cargo llvm-cov nextest --no-report --workspace --test private_channel_integration
	@echo "Running HTTP malformed-body tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_rpc_http_malformed -- --nocapture
	@echo "Running simulate-transaction guards tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_simulate_transaction_guards -- --nocapture
	@echo "Running standalone health-endpoint tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_health_endpoint_standalone -- --nocapture
	@echo "Running sequencer zero-deadline drain tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_sequencer_zero_deadline -- --nocapture
	@echo "Running get_signatures corruption-guard tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_signatures_corruption_guard -- --nocapture
	@echo "Running run_node config-validation tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_node_config_validation -- --nocapture

# Redis-backend warm-cache + read-path coverage.
# Runs a Postgres+Redis node and a Redis-only node to exercise settle.rs's
# `REDIS_URL`/best-effort-write branches and the
# `get_signatures_for_address_redis` dispatch arm. Runs independently of the
# main private_channel_integration suite — its profraw aggregates into the same
# `cargo llvm-cov report` output, so coverage accumulates naturally.
# CI should invoke this target in parallel with (or immediately after)
# `integration-coverage-private-channel`.
integration-coverage-private-channel-redis:
	@echo "Running Redis-cache-path tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test test_redis_cache_path -- --nocapture --test-threads=1

# integration-coverage-indexer runs tests in two feature groups to avoid
# stomping the escrow `.so`:
#
#   1. test-tree group — all tests compiled with `--features test-tree`.
#      Requires the escrow `.so` deployed with test-tree (TREE_HEIGHT=3).
#      Caller (e.g. `integration-coverage`) is responsible for building
#      test-tree before invoking this target.
#
#   2. prod group — all other tests. After the test-tree group finishes
#      we rebuild the escrow in production configuration (TREE_HEIGHT=16)
#      before running them.
#
# The `validator_helper` fingerprint check in test_utils poisons a
# std::sync::Once if a test finds a `.so` that doesn't match its build
# feature — cascading every subsequent test in the binary to fail with
# "Once instance has previously been poisoned". Grouping by feature and
# rebuilding between groups prevents this.
integration-coverage-indexer: export PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS=600
integration-coverage-indexer:
	@echo "=== test-tree feature group ==="
	@echo "PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS=$$PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS (coverage headroom)"
	@echo "Running indexer integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --features test-tree --test indexer_integration -- --nocapture
	@echo "Running operator lifecycle integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --features test-tree --test operator_lifecycle_integration -- --nocapture
	@echo "Running withdrawal NULL-nonce quarantine tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --features test-tree --test withdrawal_null_nonce -- --nocapture
	@echo "=== Rebuilding escrow in production configuration for prod-feature tests ==="
	@# Use `build-no-clients` to skip pnpm-driven IDL / client regeneration —
	@# clients are already present from the initial `make build` (or, in CI,
	@# from the restored `ci-build-artifacts` tarball).
	@$(MAKE) -C ../private-channel-escrow-program build-no-clients
	@echo "=== prod feature group ==="
	@echo "Running reconciliation integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test reconciliation_integration -- --nocapture
	@echo "Running mint idempotency integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test mint_idempotency_integration -- --nocapture
	@echo "Running gap detection integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test gap_detection_integration -- --nocapture
	@echo "Running truncate integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test truncate_integration -- --nocapture
	@echo "Running pausable mint integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test pausable_mint_integration -- --nocapture
	@echo "Running permanent delegate mint integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test permanent_delegate_mint_integration -- --nocapture
	@echo "Running resync integration tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test resync_integration -- --nocapture
	@echo "Running reconciliation e2e tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test reconciliation_e2e_test -- --nocapture
	@echo "Running mock-rpc retry tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test mock_rpc_retry -- --nocapture
	@echo "Running checkpoint partial-flush tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test checkpoint_partial_flush -- --nocapture
	@echo "Running remint recovery tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test remint_recovery -- --nocapture
	@echo "Running indexer bootstrap validation tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test bootstrap_validation -- --nocapture
	@echo "Running Yellowstone wiring tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test yellowstone_wiring -- --nocapture
	@echo "Running malformed Yellowstone update tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test malformed_yellowstone_update -- --nocapture
	@echo "Running Yellowstone reconnect-gap tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test yellowstone_reconnect_gap -- --nocapture
	@echo "Running Yellowstone inner-ix + unknown-discriminator tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test yellowstone_inner_and_unknown -- --nocapture
	@echo "Running OperatorMockHarness sanity test with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test harness_sanity -- --nocapture
	@echo "Running sender mint-idempotency tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_mint_idempotency -- --nocapture
	@echo "Running sender mint validator-encodings tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_mint_validator_encodings -- --nocapture
	@echo "Running sender mint signature-failures tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_mint_signature_failures -- --nocapture
	@echo "Running sender poll-RPC-error tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_poll_rpc_error -- --nocapture
	@echo "Running sender sign-and-send error tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_sign_and_send_error -- --nocapture
	@echo "Running sender max-retries tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_max_retries -- --nocapture
	@echo "Running sender on-chain error-arm tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_onchain_error_arms -- --nocapture
	@echo "Running JIT-mint helper coverage tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test jit_mint_helper -- --nocapture
	@echo "Running storage-update-failure tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test storage_update_failure -- --nocapture
	@echo "Running processor-quarantine tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test processor_quarantine -- --nocapture
	@echo "Running state-recovery malformed-row tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test state_recovery_malformed -- --nocapture
	@echo "Running remint idempotency tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test remint_flow -- --nocapture
	@echo "Running MintToBuilder field-validation tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test mint_builder_validation -- --nocapture
	@echo "Running sender channel-close shutdown tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_channel_close -- --nocapture
	@echo "Running sender cancellation-drain tests with coverage instrumentation..."
	@cargo llvm-cov test --no-report --workspace --test sender_cancellation_drain -- --nocapture

integration-coverage-report:
	@echo "Generating combined integration E2E coverage report..."
	@mkdir -p $(COVERAGE_DIR)
	@cargo llvm-cov report --lcov \
		-p private-channel-core -p private-channel-indexer \
		--ignore-filename-regex '$(COV_IGNORE_REGEX)' \
		--output-path $(COVERAGE_DIR)/coverage-integration-e2e.lcov
	@echo "Coverage report: $(COVERAGE_DIR)/coverage-integration-e2e.lcov"
	@$(MAKE) check-coverage-floor

# Coverage ratchet — fail CI if filtered E2E coverage falls below this floor.
# The default targets the combined run (`make integration-coverage`), which is
# what CI executes. The floor sits a few points below the latest observed
# combined value so normal test-ordering / timing variance doesn't trip the
# gate. Ratchet up as coverage lands durably above the floor.
COVERAGE_FLOOR ?= 75.0

check-coverage-floor:
	@python3 -c "import sys, pathlib; \
	lcov = pathlib.Path('$(COVERAGE_DIR)/coverage-integration-e2e.lcov').read_text(); \
	lf = sum(int(l[3:]) for l in lcov.splitlines() if l.startswith('LF:')); \
	lh = sum(int(l[3:]) for l in lcov.splitlines() if l.startswith('LH:')); \
	pct = 100.0 * lh / lf if lf else 0.0; \
	floor = float('$(COVERAGE_FLOOR)'); \
	print(f'E2E Integration coverage: {pct:.2f}% (LF={lf} LH={lh})  floor={floor}%'); \
	sys.exit(0 if pct >= floor else 1)" \
	|| { echo 'ERROR: coverage regressed below the floor. Either raise coverage or justify lowering COVERAGE_FLOOR.' ; exit 1 ; }

# Local convenience target: builds the required .so artifacts before running coverage.
# CI calls integration-coverage-private-channel and integration-coverage-indexer directly,
# with .so files pre-built in a separate job (do NOT add build steps to those targets).
integration-coverage:
	@echo "Building escrow with production settings for private_channel integration tests..."
	@$(MAKE) -C ../private-channel-escrow-program build
	@$(MAKE) integration-coverage-private-channel
	@$(MAKE) integration-coverage-private-channel-redis
	@echo "Building escrow with test-tree for indexer/operator-lifecycle tests..."
	@$(MAKE) -C ../private-channel-escrow-program build-test
	@$(MAKE) integration-coverage-indexer
	@$(MAKE) integration-coverage-report
