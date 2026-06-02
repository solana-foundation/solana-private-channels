# Master Makefile for Solana Private Channels Programs
# Delegates to subdirectory Makefiles

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

PROGRAM_DIRS := private-channel-escrow-program private-channel-withdraw-program dvp-swap-program
RUST_DIRS := core indexer gateway auth
FMT_DIRS := $(PROGRAM_DIRS) $(RUST_DIRS) integration
OBS_SERVICES := cadvisor prometheus grafana

.PHONY: all help
.PHONY: install install-toolchain check-toolchain check-docker build fmt generate-idl generate-clients
.PHONY: unit-test integration-test all-test drills drill
.PHONY: ci-unit-test ci-integration-test ci-integration-test-prebuilt ci-integration-test-build-test-tree ci-integration-test-indexer
.PHONY: unit-test-ci integration-test-ci integration-test-ci-prebuilt integration-test-ci-build-test-tree integration-test-ci-indexer integration-test-ci-no-build
.PHONY: unit-coverage coverage-html all-coverage ci-unit-coverage ci-e2e-coverage
.PHONY: yellowstone-prepare yellowstone-build-plugin yellowstone-clean ensure-geyser-plugin
.PHONY: download-yellowstone-grpc build-geyser-plugin clean-geyser
.PHONY: generate-operator-keypair build-localnet build-devnet deploy-devnet
.PHONY: profile obs-up obs-down obs-logs obs-devnet-up obs-devnet-down obs-devnet-logs
.PHONY: install-buildkit-cache check-buildkit-cache
.PHONY: docker-build docker-up docker-rebuild docker-restart docker-down docker-clean docker-logs docker-ps
.PHONY: docker-devnet-build docker-devnet-up docker-devnet-rebuild docker-devnet-restart docker-devnet-down docker-devnet-clean docker-devnet-logs docker-devnet-ps

all: build

# versions.env is the source of truth for SOLANA_VERSION; when absent (e.g.
# inside the Docker build context, where the CLI is already installed by the
# Dockerfile's ARG), we skip rather than fail.
install-toolchain:
	@if [ -f versions.env ]; then set -a; source versions.env; set +a; fi; \
	if [ -z "$${SOLANA_VERSION:-}" ]; then \
	    echo "SOLANA_VERSION not set (no versions.env, no env override) — skipping Solana CLI install"; \
	elif ! command -v solana >/dev/null 2>&1 || \
	     [ "$$(solana --version 2>/dev/null | awk '{print $$2}')" != "$$SOLANA_VERSION" ]; then \
	    echo "Installing Solana CLI v$$SOLANA_VERSION..."; \
	    attempt=0; \
	    until sh -c "$$(curl -sSfL https://release.anza.xyz/v$$SOLANA_VERSION/install)"; do \
	        attempt=$$((attempt + 1)); \
	        if [ "$$attempt" -ge 3 ]; then \
	            echo "Solana CLI install failed after 3 attempts"; exit 1; \
	        fi; \
	        delay=$$((attempt * 10)); \
	        echo "Solana CLI install attempt $$attempt failed (transient network error from solana-install fetching the tarball); retrying in $${delay}s..."; \
	        sleep "$$delay"; \
	    done; \
	else \
	    echo "Solana CLI already at v$$SOLANA_VERSION — skipping"; \
	fi
	@# Warm the SBF platform tools cache so the first `make build`
	@# doesn't interleave a large download with compilation output.
	@cargo-build-sbf --version >/dev/null 2>&1 || true
	@# rustup toolchain for the host side (core/indexer/gateway/auth).
	@# rust-toolchain.toml drives the channel; this just pre-fetches it.
	@if command -v rustup >/dev/null 2>&1; then \
	    rustup show active-toolchain >/dev/null 2>&1 || rustup toolchain install; \
	else \
	    echo "rustup not found; install it manually (https://rustup.rs) then re-run"; \
	fi

check-toolchain:
	@if [ -f versions.env ]; then set -a; source versions.env; set +a; fi; \
	if [ -z "$${SOLANA_VERSION:-}" ]; then \
	    echo "SOLANA_VERSION not set (no versions.env, no env override) — skipping check"; \
	else \
	    installed="$$(solana --version 2>/dev/null | awk '{print $$2}')"; \
	    if [ "$$installed" != "$$SOLANA_VERSION" ]; then \
	        echo "ERROR: solana CLI is '$$installed', versions.env pins '$$SOLANA_VERSION'."; \
	        echo "Run: make install-toolchain"; \
	        exit 1; \
	    fi; \
	fi
	@command -v cargo-build-sbf >/dev/null || { echo "ERROR: cargo-build-sbf not on PATH"; exit 1; }

# Docker floor check. Separate from check-toolchain because `make build` is cargo-only
# and must work for contributors who don't have Docker installed. Gate compose/docker
# build targets on this instead. Floor is 26.0 to match the README; the Dockerfiles use
# BuildKit `--mount=type=cache` and the `# syntax=docker/dockerfile:1.7` frontend.
check-docker:
	@if ! command -v docker >/dev/null 2>&1; then \
	    echo "ERROR: docker not found on PATH — required for compose-based dev/test stacks (>= 26.0)"; \
	    exit 1; \
	fi
	@ver="$$(docker version --format '{{.Server.Version}}' 2>/dev/null || docker version --format '{{.Client.Version}}' 2>/dev/null)"; \
	if [ -z "$$ver" ]; then \
	    echo "ERROR: docker present but version could not be read (daemon not running?)"; \
	    exit 1; \
	fi; \
	major="$$(echo "$$ver" | cut -d. -f1)"; \
	if [ "$$major" -lt 26 ] 2>/dev/null; then \
	    echo "ERROR: docker is $$ver; this repo requires >= 26.0 (BuildKit cache mounts)."; \
	    exit 1; \
	fi

install: install-toolchain ensure-geyser-plugin
	@echo "Installing dependencies for all projects..."
	@for dir in $(PROGRAM_DIRS); do \
		$(MAKE) -C $$dir install; \
	done

build: check-toolchain
	@echo "Building all projects..."
	@for dir in $(PROGRAM_DIRS) $(RUST_DIRS); do \
		$(MAKE) -C $$dir build; \
	done

fmt:
	@echo "Formatting all projects..."
	@for dir in $(FMT_DIRS); do \
		$(MAKE) -C $$dir fmt; \
	done
	@cd scripts/devnet && cargo fmt

generate-idl:
	@echo "Generating IDL for all programs..."
	@for dir in $(PROGRAM_DIRS); do \
		$(MAKE) -C $$dir generate-idl; \
	done

generate-clients:
	@echo "Generating clients for all programs..."
	@for dir in $(PROGRAM_DIRS); do \
		$(MAKE) -C $$dir generate-clients; \
	done

unit-test:
	@echo "Running unit tests for all projects..."
	@for dir in $(PROGRAM_DIRS) $(RUST_DIRS); do \
		$(MAKE) -C $$dir unit-test; \
	done

integration-test:
	@echo "Running integration tests for all projects..."
	@$(MAKE) -C private-channel-escrow-program integration-test
	@$(MAKE) -C private-channel-withdraw-program integration-test
	@$(MAKE) -C dvp-swap-program integration-test
	@# Delegate to integration/Makefile so escrow-feature grouping + the full
	@# `[[test]]` set stay in one place. See integration/Makefile:integration-test
	@# for the private-channel/test-tree/prod group ordering.
	@$(MAKE) -C integration integration-test

ci-unit-test:
	@echo "Running CI unit tests for core + indexer..."
	@$(MAKE) -C core unit-test
	@$(MAKE) -C indexer unit-test

ci-integration-test:
	@echo "Running CI integration tests (non-program suites)..."
	@echo "Building program artifacts once for integration crate tests..."
	@$(MAKE) -C private-channel-escrow-program build
	@$(MAKE) -C private-channel-withdraw-program build
	@$(MAKE) ci-integration-test-build-test-tree

ci-integration-test-build-test-tree:
	@# Order matters: prod-feature first (escrow already prod from caller),
	@# then rebuild test-tree, then run only test-tree-feature tests. Mixing
	@# escrow features in a single run poisons the validator_helper Once.
	@$(MAKE) ci-integration-test-prebuilt
	@echo "Building escrow with test-tree for indexer and operator lifecycle tests..."
	@$(MAKE) -C private-channel-escrow-program build-test
	@echo "=== test-tree feature group ==="
	@cd integration && cargo test --features test-tree --test indexer_integration -- --nocapture
	@cd integration && cargo test --features test-tree --test operator_lifecycle_integration -- --nocapture
	@cd integration && cargo test --features test-tree --test withdrawal_null_nonce -- --nocapture

ci-integration-test-prebuilt:
	@# Prod-feature suites that assume a prod escrow `.so` is already in
	@# target/deploy/. Caller is responsible for the build. Keep in sync with
	@# integration/Makefile's `integration-coverage-private-channel` (private-channel group) and
	@# the prod-feature subset of `integration-coverage-indexer`.
	@echo "=== private-channel group (prod escrow) ==="
	@cd integration && cargo nextest run --test private_channel_integration
	@cd integration && cargo test --test test_rpc_http_malformed -- --nocapture
	@cd integration && cargo test --test test_simulate_transaction_guards -- --nocapture
	@cd integration && cargo test --test test_health_endpoint_standalone -- --nocapture
	@cd integration && cargo test --test test_sequencer_zero_deadline -- --nocapture
	@cd integration && cargo test --test test_signatures_corruption_guard -- --nocapture
	@cd integration && cargo test --test test_node_config_validation -- --nocapture
	@cd integration && cargo test --test test_redis_cache_path -- --nocapture --test-threads=1
	@echo "=== prod-feature indexer group ==="
	@cd integration && cargo test --test reconciliation_integration -- --nocapture
	@cd integration && cargo test --test mint_idempotency_integration -- --nocapture
	@cd integration && cargo test --test gap_detection_integration -- --nocapture
	@cd integration && cargo test --test truncate_integration -- --nocapture
	@cd integration && cargo test --test pausable_mint_integration -- --nocapture
	@cd integration && cargo test --test permanent_delegate_mint_integration -- --nocapture
	@cd integration && cargo test --test resync_integration -- --nocapture
	@cd integration && cargo test --test reconciliation_e2e_test -- --nocapture
	@cd integration && cargo test --test mock_rpc_retry -- --nocapture
	@cd integration && cargo test --test checkpoint_partial_flush -- --nocapture
	@cd integration && cargo test --test remint_recovery -- --nocapture
	@cd integration && cargo test --test stuck_processing_recovery -- --nocapture
	@cd integration && cargo test --test bootstrap_validation -- --nocapture
	@cd integration && cargo test --test deposit_allowlist_e2e -- --nocapture
	@cd integration && cargo test --test yellowstone_wiring -- --nocapture
	@cd integration && cargo test --test malformed_yellowstone_update -- --nocapture
	@cd integration && cargo test --test yellowstone_reconnect_gap -- --nocapture
	@cd integration && cargo test --test yellowstone_inner_and_unknown -- --nocapture
	@cd integration && cargo test --test harness_sanity -- --nocapture
	@cd integration && cargo test --test sender_mint_idempotency -- --nocapture
	@cd integration && cargo test --test sender_mint_validator_encodings -- --nocapture
	@cd integration && cargo test --test sender_mint_signature_failures -- --nocapture
	@cd integration && cargo test --test sender_poll_rpc_error -- --nocapture
	@cd integration && cargo test --test sender_sign_and_send_error -- --nocapture
	@cd integration && cargo test --test sender_max_retries -- --nocapture
	@cd integration && cargo test --test sender_onchain_error_arms -- --nocapture
	@cd integration && cargo test --test jit_mint_helper -- --nocapture
	@cd integration && cargo test --test storage_update_failure -- --nocapture
	@cd integration && cargo test --test processor_quarantine -- --nocapture
	@cd integration && cargo test --test state_recovery_malformed -- --nocapture
	@cd integration && cargo test --test remint_flow -- --nocapture
	@cd integration && cargo test --test mint_builder_validation -- --nocapture
	@cd integration && cargo test --test sender_channel_close -- --nocapture
	@cd integration && cargo test --test sender_cancellation_drain -- --nocapture

# CI-focused integration target that runs indexer integration tests only.
# Same group-ordering invariant as integration/Makefile:integration-coverage-indexer:
#   1. Build test-tree → run test-tree-feature tests.
#   2. Rebuild escrow as prod → run prod-feature indexer tests.
ci-integration-test-indexer:
	@echo "Building escrow with test-tree for test-tree-feature group..."
	@$(MAKE) -C private-channel-escrow-program build-test
	@echo "=== test-tree feature group ==="
	@cd integration && cargo test --features test-tree --test indexer_integration -- --nocapture
	@cd integration && cargo test --features test-tree --test operator_lifecycle_integration -- --nocapture
	@cd integration && cargo test --features test-tree --test withdrawal_null_nonce -- --nocapture
	@echo "=== Rebuilding escrow in prod for prod-feature indexer group ==="
	@$(MAKE) -C private-channel-escrow-program build-no-clients
	@echo "=== prod-feature indexer group ==="
	@cd integration && cargo test --test reconciliation_integration -- --nocapture
	@cd integration && cargo test --test mint_idempotency_integration -- --nocapture
	@cd integration && cargo test --test gap_detection_integration -- --nocapture
	@cd integration && cargo test --test truncate_integration -- --nocapture
	@cd integration && cargo test --test pausable_mint_integration -- --nocapture
	@cd integration && cargo test --test permanent_delegate_mint_integration -- --nocapture
	@cd integration && cargo test --test resync_integration -- --nocapture
	@cd integration && cargo test --test reconciliation_e2e_test -- --nocapture
	@cd integration && cargo test --test mock_rpc_retry -- --nocapture
	@cd integration && cargo test --test checkpoint_partial_flush -- --nocapture
	@cd integration && cargo test --test remint_recovery -- --nocapture
	@cd integration && cargo test --test stuck_processing_recovery -- --nocapture
	@cd integration && cargo test --test bootstrap_validation -- --nocapture
	@cd integration && cargo test --test deposit_allowlist_e2e -- --nocapture
	@cd integration && cargo test --test yellowstone_wiring -- --nocapture
	@cd integration && cargo test --test malformed_yellowstone_update -- --nocapture
	@cd integration && cargo test --test yellowstone_reconnect_gap -- --nocapture
	@cd integration && cargo test --test yellowstone_inner_and_unknown -- --nocapture
	@cd integration && cargo test --test harness_sanity -- --nocapture
	@cd integration && cargo test --test sender_mint_idempotency -- --nocapture
	@cd integration && cargo test --test sender_mint_validator_encodings -- --nocapture
	@cd integration && cargo test --test sender_mint_signature_failures -- --nocapture
	@cd integration && cargo test --test sender_poll_rpc_error -- --nocapture
	@cd integration && cargo test --test sender_sign_and_send_error -- --nocapture
	@cd integration && cargo test --test sender_max_retries -- --nocapture
	@cd integration && cargo test --test sender_onchain_error_arms -- --nocapture
	@cd integration && cargo test --test jit_mint_helper -- --nocapture
	@cd integration && cargo test --test storage_update_failure -- --nocapture
	@cd integration && cargo test --test processor_quarantine -- --nocapture
	@cd integration && cargo test --test state_recovery_malformed -- --nocapture
	@cd integration && cargo test --test remint_flow -- --nocapture
	@cd integration && cargo test --test mint_builder_validation -- --nocapture
	@cd integration && cargo test --test sender_channel_close -- --nocapture
	@cd integration && cargo test --test sender_cancellation_drain -- --nocapture

# Backward-compatible aliases.
unit-test-ci: ci-unit-test
integration-test-ci: ci-integration-test
integration-test-ci-build-test-tree: ci-integration-test-build-test-tree
integration-test-ci-prebuilt: ci-integration-test-prebuilt
integration-test-ci-indexer: ci-integration-test-indexer
integration-test-ci-no-build:
	@echo "Deprecated: use integration-test-ci-build-test-tree"
	@$(MAKE) ci-integration-test-build-test-tree

all-test: unit-test integration-test

# Runbook drills are #[ignore]-flagged in the indexer test suite so they're
# skipped by default. They spin up a real Postgres via testcontainers and
# verify the SQL in docs/runbooks/*.md still matches the schema and the
# operator code's contracts. They are NOT in CI by design, run them
# manually before merging a runbook edit, or after touching processor.rs /
# sender/transaction.rs / sender/remint.rs / db_transaction_writer.rs /
# db_transaction_writer's webhook serializer / the indexer schema.
drills:
	@cargo test -p private-channel-indexer --test runbook_drills -- --ignored --nocapture

drill:
	@if [ -z "$(NAME)" ]; then \
	    echo "usage: make drill NAME=drill_3"; \
	    echo "       (run a single drill by name; substring match is fine)"; \
	    exit 1; \
	fi
	@cargo test -p private-channel-indexer --test runbook_drills -- --ignored --nocapture $(NAME)

unit-coverage:
	@echo "Running unit tests with coverage..."
	@for dir in $(PROGRAM_DIRS) $(RUST_DIRS); do \
		$(MAKE) -C $$dir unit-coverage; \
	done

coverage-html:
	@echo "Generating HTML coverage reports..."
	@for dir in $(PROGRAM_DIRS) $(RUST_DIRS); do \
		$(MAKE) -C $$dir coverage-html; \
	done

all-coverage:
	@echo "Running all coverage tasks..."
	@for dir in $(PROGRAM_DIRS) $(RUST_DIRS); do \
		$(MAKE) -C $$dir all-coverage; \
	done

ci-unit-coverage:
	@echo "Running CI unit tests with coverage for core + indexer + gateway + auth..."
	@$(MAKE) -C core unit-coverage
	@$(MAKE) -C indexer unit-coverage
	@$(MAKE) -C gateway unit-coverage
	@$(MAKE) -C auth unit-coverage

ci-e2e-coverage:
	@echo "Running E2E integration tests with coverage..."
	@$(MAKE) -C integration integration-coverage

#############
# Integration Test Setup
#############
yellowstone-prepare:
	@set -a; source versions.env; set +a; \
	echo "Building Yellowstone Geyser plugin at $$YELLOWSTONE_TAG..."; \
	mkdir -p integration/.yellowstone-grpc; \
	if [ ! -d "integration/.yellowstone-grpc/.git" ]; then \
		echo "Cloning yellowstone-grpc repository..."; \
		git clone https://github.com/rpcpool/yellowstone-grpc.git integration/.yellowstone-grpc; \
	fi; \
	echo "Checking out $$YELLOWSTONE_TAG..."; \
	cd integration/.yellowstone-grpc && \
		git fetch origin --tags && \
		git checkout "$$YELLOWSTONE_TAG"
	@echo "Applying macOS compatibility fixes..."
	@if [ "$$(uname)" = "Darwin" ]; then \
		echo "Copying macOS-fixed files from test_utils/geyser/mac-files-fix/..."; \
		cp -rf test_utils/geyser/mac-files-fix/yellowstone-grpc-geyser/* \
			integration/.yellowstone-grpc/yellowstone-grpc-geyser/; \
		cp -f test_utils/geyser/mac-files-fix/Cargo.toml \
			integration/.yellowstone-grpc/; \
		echo "macOS fixes applied (affinity -> core_affinity)"; \
	else \
		echo "Skipping macOS fixes (not on macOS)"; \
	fi

yellowstone-build-plugin: yellowstone-prepare
	@echo "Building plugin (this may take a few minutes)..."
	@cd integration/.yellowstone-grpc/yellowstone-grpc-geyser && \
		cargo build --release --no-default-features
	@echo "Copying plugin to test_utils/geyser/..."
	@mkdir -p test_utils/geyser
	@if [ -f integration/.yellowstone-grpc/target/release/libyellowstone_grpc_geyser.dylib ]; then \
		cp integration/.yellowstone-grpc/target/release/libyellowstone_grpc_geyser.dylib \
			test_utils/geyser/libyellowstone_grpc_geyser.dylib; \
		echo "Geyser plugin built: test_utils/geyser/libyellowstone_grpc_geyser.dylib"; \
	elif [ -f integration/.yellowstone-grpc/target/release/libyellowstone_grpc_geyser.so ]; then \
		cp integration/.yellowstone-grpc/target/release/libyellowstone_grpc_geyser.so \
			test_utils/geyser/libyellowstone_grpc_geyser.so; \
		echo "Geyser plugin built: test_utils/geyser/libyellowstone_grpc_geyser.so"; \
	else \
		echo "Error: Plugin binary not found after build"; \
		exit 1; \
	fi

yellowstone-clean:
	@echo "Cleaning Yellowstone Geyser build artifacts..."
	@rm -rf integration/.yellowstone-grpc
	@rm -f test_utils/geyser/libyellowstone_grpc_geyser.dylib
	@rm -f test_utils/geyser/libyellowstone_grpc_geyser.so
	@rm -f test_utils/geyser/.dylib-tag
	@echo "Geyser artifacts cleaned"

# Ensure the Yellowstone Geyser plugin is available for integration tests.
#   Linux: the .so is checked into test_utils/geyser/ — just sanity-check it.
#   macOS: build the .dylib from source on first run (~3-5 min) and cache it.
#          Cross-compiling from Linux to macOS isn't practical (Apple SDK
#          license + ABI fragility), so the .dylib is generated locally on
#          each Mac. A stamp file tracks YELLOWSTONE_TAG so a versions.env
#          bump triggers a rebuild but day-to-day `make install` is a no-op.
ensure-geyser-plugin:
	@if [ -f versions.env ]; then set -a; source versions.env; set +a; fi; \
	if [ "$$(uname -s)" = "Darwin" ]; then \
	    plugin=test_utils/geyser/libyellowstone_grpc_geyser.dylib; \
	    stamp=test_utils/geyser/.dylib-tag; \
	    if [ ! -f "$$plugin" ] || [ "$$(cat "$$stamp" 2>/dev/null)" != "$$YELLOWSTONE_TAG" ]; then \
	        echo "macOS: building Yellowstone Geyser plugin $$YELLOWSTONE_TAG"; \
	        echo "  one-time ~3-5 min build; the prebuilt .dylib was dropped"; \
	        echo "  in the 3.1.13 bump (Linux->macOS cross-compile isn't viable)."; \
	        $(MAKE) yellowstone-build-plugin && echo "$$YELLOWSTONE_TAG" > "$$stamp"; \
	    fi; \
	else \
	    plugin=test_utils/geyser/libyellowstone_grpc_geyser.so; \
	    [ -f "$$plugin" ] || { echo "ERROR: $$plugin missing (should be checked in)"; exit 1; }; \
	fi

# Backward-compatible aliases.
download-yellowstone-grpc: yellowstone-prepare
build-geyser-plugin: yellowstone-build-plugin
clean-geyser: yellowstone-clean

#############
# Common
#############
generate-operator-keypair:
	@./scripts/ensure-operator-keypair.sh keypairs/operator-keypair.json

#############
# Localnet
#############
build-localnet:
	@echo "Building all programs for localnet..."
	@$(MAKE) -C private-channel-escrow-program build-localnet
	@$(MAKE) -C private-channel-withdraw-program build-localnet
	@$(MAKE) generate-operator-keypair
	@./scripts/update-admin-env.sh .env.local keypairs/operator-keypair.json

#############
# Devnet
#############
build-devnet:
	@echo "Building all programs for devnet..."
	@$(MAKE) -C private-channel-escrow-program build-devnet
	@$(MAKE) -C private-channel-withdraw-program build-devnet
	@$(MAKE) generate-operator-keypair
	@./scripts/update-admin-env.sh .env.devnet keypairs/operator-keypair.json

deploy-devnet:
	@echo "Deploying all programs to devnet..."
	@$(MAKE) -C private-channel-escrow-program deploy-devnet DEPLOYER_KEY=$(DEPLOYER_KEY)
	@$(MAKE) -C private-channel-withdraw-program deploy-devnet DEPLOYER_KEY=$(DEPLOYER_KEY)

profile:
	@echo "Generating CU profiling report..."
	@python3 generate_profiling.py
	@echo "CU profiling report generated: profiling_report.md"

#############
# Observability
#############
obs-up: check-buildkit-cache
	@echo "Starting observability stack (docker-compose.yml)..."
	@docker compose -f docker-compose.yml up -d $(OBS_SERVICES)

obs-down:
	@echo "Stopping observability stack (docker-compose.yml)..."
	@docker compose -f docker-compose.yml stop $(OBS_SERVICES)

obs-logs:
	@docker compose -f docker-compose.yml logs -f --tail=200 $(OBS_SERVICES)

obs-devnet-up: check-buildkit-cache
	@echo "Starting observability stack (docker-compose.devnet.yml)..."
	@docker compose -f docker-compose.devnet.yml up -d $(OBS_SERVICES)

obs-devnet-down:
	@echo "Stopping observability stack (docker-compose.devnet.yml)..."
	@docker compose -f docker-compose.devnet.yml stop $(OBS_SERVICES)

obs-devnet-logs:
	@docker compose -f docker-compose.devnet.yml logs -f --tail=200 $(OBS_SERVICES)

#############
# BuildKit cache GC
#############
# Merges buildkit-gc-fragment.json into /etc/docker/daemon.json and reloads
# dockerd so the embedded BuildKit applies the cache caps. Idempotent: jq's
# `.[0] * .[1]` deep-merge means re-running produces the same result. Safe:
# always backs up daemon.json with a timestamp and validates the merged
# output before installing. live-restore=true (already in daemon.json)
# means reload does not interrupt running containers.
DAEMON_JSON := /etc/docker/daemon.json
BUILDKIT_GC_FRAGMENT := $(CURDIR)/buildkit-gc-fragment.json

install-buildkit-cache: check-docker
	@command -v jq >/dev/null 2>&1 || { echo "ERROR: jq required (apt install jq)"; exit 1; }
	@test -r $(BUILDKIT_GC_FRAGMENT) || { echo "ERROR: $(BUILDKIT_GC_FRAGMENT) not found"; exit 1; }
	@if [ "$$(id -u)" -ne 0 ]; then \
	    echo "ERROR: must run as root (writes $(DAEMON_JSON))"; \
	    echo "Re-run with: sudo make install-buildkit-cache"; \
	    exit 1; \
	fi
	@set -e; \
	ts="$$(date +%Y%m%d-%H%M%S)"; \
	if [ -f $(DAEMON_JSON) ]; then \
	    cp -a $(DAEMON_JSON) $(DAEMON_JSON).bak.$$ts; \
	    echo "Backup: $(DAEMON_JSON).bak.$$ts"; \
	    base=$(DAEMON_JSON); \
	else \
	    base="$$(mktemp)"; printf '{}\n' > "$$base"; \
	fi; \
	merged="$$(mktemp)"; \
	jq -s '.[0] * .[1]' "$$base" $(BUILDKIT_GC_FRAGMENT) > "$$merged"; \
	jq empty "$$merged" >/dev/null; \
	mv "$$merged" $(DAEMON_JSON); \
	chmod 0644 $(DAEMON_JSON); \
	echo "Merged builder.gc into $(DAEMON_JSON)"; \
	echo "Reloading dockerd (live-restore=true preserves running containers)..."; \
	systemctl reload docker; \
	sleep 1; \
	docker info >/dev/null 2>&1 || { echo "ERROR: docker info failed after reload — restore from backup"; exit 1; }; \
	echo "Done. Inspect cache usage: docker buildx du"

# Lightweight check used as a prerequisite of compose-driven build targets.
# Greps daemon.json for the marker keys; cheap and avoids spawning docker.
# Fails with an actionable message if the cache caps were never installed.
check-buildkit-cache:
	@if [ ! -f $(DAEMON_JSON) ] || \
	   ! grep -q '"defaultKeepStorage"' $(DAEMON_JSON) 2>/dev/null; then \
	    echo "ERROR: BuildKit GC config not installed in $(DAEMON_JSON)"; \
	    echo "       Run: sudo make install-buildkit-cache"; \
	    echo "       (one-time setup; caps build cache at 50 GB so it doesn't fill the disk)"; \
	    exit 1; \
	fi

#############
# Docker stack — full local + devnet compose orchestration
#############
# These targets save users from remembering the env-file chain on every invocation.
# Load order: versions.env first (toolchain pins), then the env-specific overrides.
# `.env.local` is the developer's machine config (gitignored — copy from .env.example
# and fill in secrets); `.env.devnet` is the tracked devnet preset.
#
# Override the env file chain by passing ENV_FILES_LOCAL / ENV_FILES_DEVNET on the
# command line, e.g. `make docker-up ENV_FILES_LOCAL="--env-file versions.env --env-file .env.staging"`.
#
# Prereq policy:
#   - Daemon-touching targets (build/up/restart/logs/ps) depend on `check-docker`.
#   - Targets that can trigger a build depend on `check-buildkit-cache` so the
#     cache GC config is in place before BuildKit populates it.
#   - `docker-down` and `docker-clean` are recovery paths — no prereqs, so they
#     still run if Docker / BuildKit configuration is in a degraded state.
COMPOSE_LOCAL    := docker-compose.yml
COMPOSE_DEVNET   := docker-compose.devnet.yml
ENV_FILES_LOCAL  ?= --env-file versions.env --env-file .env.local
ENV_FILES_DEVNET ?= --env-file versions.env --env-file .env.devnet

# --- Local stack (local validator) ---

docker-build: check-docker check-buildkit-cache
	@echo "Building all images ($(COMPOSE_LOCAL))..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) build

docker-up: check-docker check-buildkit-cache
	@echo "Starting full stack ($(COMPOSE_LOCAL))..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) up -d

# Like docker-up but preserves the local validator ledger across restarts so
# on-chain state stays consistent with Postgres rows. Caveat: after changing
# escrow/withdraw program code, run `make docker-clean` first or the validator
# will keep running stale bytecode.
docker-up-persist: check-docker check-buildkit-cache
	@echo "Starting full stack with validator persistence ($(COMPOSE_LOCAL))..."
	@VALIDATOR_RESET_FLAG= docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) up -d

docker-rebuild: check-docker check-buildkit-cache
	@echo "Rebuilding and (re)starting full stack ($(COMPOSE_LOCAL))..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) up -d --build

docker-restart: check-docker
	@echo "Restarting full stack ($(COMPOSE_LOCAL))..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) restart

docker-down:
	@echo "Stopping full stack ($(COMPOSE_LOCAL); volumes preserved)..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) down

docker-clean:
	@echo "Stopping full stack and removing volumes ($(COMPOSE_LOCAL))..."
	@docker compose -f $(COMPOSE_LOCAL) $(ENV_FILES_LOCAL) down -v --remove-orphans

docker-logs: check-docker
	@docker compose -f $(COMPOSE_LOCAL) logs -f --tail=200

docker-ps: check-docker
	@docker compose -f $(COMPOSE_LOCAL) ps

# --- Devnet stack (against Solana devnet) ---

docker-devnet-build: check-docker check-buildkit-cache
	@echo "Building all images ($(COMPOSE_DEVNET))..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) build

docker-devnet-up: check-docker check-buildkit-cache
	@echo "Starting devnet stack ($(COMPOSE_DEVNET))..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) up -d

docker-devnet-rebuild: check-docker check-buildkit-cache
	@echo "Rebuilding and (re)starting devnet stack ($(COMPOSE_DEVNET))..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) up -d --build

docker-devnet-restart: check-docker
	@echo "Restarting devnet stack ($(COMPOSE_DEVNET))..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) restart

docker-devnet-down:
	@echo "Stopping devnet stack ($(COMPOSE_DEVNET); volumes preserved)..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) down

docker-devnet-clean:
	@echo "Stopping devnet stack and removing volumes ($(COMPOSE_DEVNET))..."
	@docker compose -f $(COMPOSE_DEVNET) $(ENV_FILES_DEVNET) down -v --remove-orphans

docker-devnet-logs: check-docker
	@docker compose -f $(COMPOSE_DEVNET) logs -f --tail=200

docker-devnet-ps: check-docker
	@docker compose -f $(COMPOSE_DEVNET) ps

help:
	@echo "Solana Private Channels Programs - Available targets:"
	@echo ""
	@echo "Dependencies:"
	@echo "  install              - Install dependencies for all projects"
	@echo ""
	@echo "Build:"
	@echo "  build                - Build all projects"
	@echo "  generate-idl         - Generate IDL for all programs"
	@echo "  generate-clients     - Generate clients for all programs"
	@echo ""
	@echo "Code Quality:"
	@echo "  fmt                  - Format all code"
	@echo ""
	@echo "Testing:"
	@echo "  unit-test            - Run unit tests for all projects"
	@echo "  ci-unit-test         - Run CI unit tests for core + indexer"
	@echo "  ci-unit-coverage     - Run CI unit tests with coverage for core + indexer + gateway"
	@echo "  integration-test     - Run integration tests for all projects"
	@echo "  ci-integration-test  - Build prod artifacts, build test-tree, run CI integration suites"
	@echo "  ci-integration-test-build-test-tree - Run prebuilt test, then test-tree indexer integration"
	@echo "  ci-integration-test-prebuilt - Run Solana Private Channels integration using prebuilt production artifacts"
	@echo "  ci-integration-test-indexer - Build test-tree artifact and run indexer integration only"
	@echo "  all-test             - Run all tests for all projects"
	@echo ""
	@echo "Coverage:"
	@echo "  unit-coverage        - Unit test coverage"
	@echo "  ci-e2e-coverage      - E2E integration test coverage"
	@echo "  coverage-html        - Generate HTML coverage reports"
	@echo "  all-coverage         - Run all coverage tasks"
	@echo ""
	@echo "Integration Test Setup:"
	@echo "  yellowstone-prepare      - Download & patch Yellowstone for Agave 3.0"
	@echo "  yellowstone-build-plugin - Build Yellowstone Geyser plugin"
	@echo "  yellowstone-clean        - Clean Geyser build artifacts"
	@echo ""
	@echo "Devnet:"
	@echo "  build-devnet         - Build programs for devnet"
	@echo "  deploy-devnet        - Deploy programs to devnet (requires DEPLOYER_KEY)"
	@echo ""
	@echo "Profiling:"
	@echo "  profile              - Generate CU profiling report"
	@echo ""
	@echo "Observability:"
	@echo "  obs-up               - Start cadvisor/prometheus/grafana (docker-compose.yml)"
	@echo "  obs-down             - Stop cadvisor/prometheus/grafana (docker-compose.yml)"
	@echo "  obs-logs             - Tail observability logs (docker-compose.yml)"
	@echo "  obs-devnet-up        - Start cadvisor/prometheus/grafana (docker-compose.devnet.yml)"
	@echo "  obs-devnet-down      - Stop cadvisor/prometheus/grafana (docker-compose.devnet.yml)"
	@echo "  obs-devnet-logs      - Tail observability logs (docker-compose.devnet.yml)"
	@echo ""
	@echo "Build host setup:"
	@echo "  install-buildkit-cache - Merge BuildKit GC config into /etc/docker/daemon.json (sudo, one-time)"
	@echo "  check-buildkit-cache   - Verify BuildKit GC config is installed (prereq of obs-up + docker-build/up/rebuild)"
	@echo ""
	@echo "Docker stack (full local — docker-compose.yml, uses .env.local):"
	@echo "  docker-build         - Build all images"
	@echo "  docker-up            - Start full stack in detached mode"
	@echo "  docker-up-persist    - Start full stack and preserve the local validator ledger across restarts"
	@echo "  docker-rebuild       - Rebuild images and (re)start (= build + up in one shot)"
	@echo "  docker-restart       - Restart all services without rebuilding"
	@echo "  docker-down          - Stop services (volumes preserved)"
	@echo "  docker-clean         - Stop and remove volumes / orphans (recovery)"
	@echo "  docker-logs          - Tail logs from all services"
	@echo "  docker-ps            - Show service status"
	@echo ""
	@echo "Docker stack (devnet — docker-compose.devnet.yml, uses .env.devnet):"
	@echo "  docker-devnet-build  - Build all devnet images"
	@echo "  docker-devnet-up     - Start devnet stack in detached mode"
	@echo "  docker-devnet-rebuild - Rebuild and (re)start devnet stack"
	@echo "  docker-devnet-restart - Restart devnet services without rebuilding"
	@echo "  docker-devnet-down   - Stop devnet services (volumes preserved)"
	@echo "  docker-devnet-clean  - Stop and remove devnet volumes / orphans"
	@echo "  docker-devnet-logs   - Tail devnet logs"
	@echo "  docker-devnet-ps     - Show devnet service status"
