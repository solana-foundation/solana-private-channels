SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

.PHONY: install build build-hook-fixture fmt generate-idl generate-clients
.PHONY: unit-test integration-test integration-test-no-build all-test
.PHONY: unit-coverage coverage-html all-coverage verify-program-id

# Install JS deps (codama renderers, tsx, etc.)
install:
	pnpm install

# Build the on-chain program. Regenerates clients first so the workspace
# Rust client crate is up to date before cargo-build-sbf compiles the program.
# cargo-build-sbf must run from program/ so it only sees the on-chain crate
# (the client crate has host-only deps that fail under solana CLI's stricter
# sbf target).
build:
	$(MAKE) generate-clients
	cd program && cargo-build-sbf
	$(MAKE) build-hook-fixture

# Pre-deploy guard: cargo-build-sbf writes a random target/deploy/*-keypair.json
# that does NOT match declare_id!, so deploying with it (or without an explicit
# --program-id) publishes to the wrong address. Run this before `solana program
# deploy` with the real deploy keypair in place. Not wired into `build`/CI: there
# the keypair is a throwaway and would never match. Expected ID is read from the
# generated IDL (single source of truth, derived from declare_id!).
verify-program-id:
	@expected=$$(grep -o '"publicKey": *"[^"]*"' idl/dvp_swap_program.json | head -1 | sed 's/.*"\([^"]*\)"$$/\1/'); \
	actual=$$(solana-keygen pubkey ../target/deploy/dvp_swap_program-keypair.json); \
	if [ "$$expected" != "$$actual" ]; then \
		echo "ERROR: deploy keypair $$actual does not match program ID $$expected"; \
		exit 1; \
	fi; \
	echo "OK: deploy keypair matches program ID $$expected"

# Build the no-op transfer-hook program used only by integration tests.
# Loaded into LiteSVM so hook-bearing Token-2022 mints can be exercised
# end-to-end. The .so lands in the workspace target/deploy/ alongside
# the swap program's .so.
build-hook-fixture:
	cd tests/transfer-hook-fixture && cargo-build-sbf

# Generate the Codama IDL from the program's annotations.
generate-idl:
	@echo "Generating IDL..."
	pnpm run generate-idl

# Generate Rust + TypeScript clients from the IDL.
generate-clients: generate-idl
	@echo "Generating clients..."
	pnpm run generate-clients

# Format and lint.
fmt:
	cargo fmt --all
	@cd program && cargo clippy --all-targets -- -D warnings
	@cd tests/integration-tests && cargo clippy --all-targets -- -D warnings
	pnpm format

# Unit tests: program crate's #[cfg(test)] modules + JS client tests.
unit-test:
	@echo "Running unit tests for swap program..."
	pnpm test:unit
	@cd program && cargo test

# Integration tests (litesvm-based).
integration-test-no-build:
	@echo "Running integration tests for swap program..."
	@cd tests/integration-tests && cargo test -- --nocapture

integration-test: build integration-test-no-build

all-test: unit-test integration-test

# Run unit tests with coverage
unit-coverage:
	@echo "Running unit tests with coverage..."
	@mkdir -p ../coverage
	@cd program && cargo llvm-cov --lib --tests --lcov --output-path ../../coverage/coverage-dvp-swap-unit.lcov

# Generate HTML coverage report
coverage-html:
	@echo "Generating HTML coverage report..."
	@mkdir -p ../coverage
	@cd program && cargo llvm-cov --html --output-dir ../../coverage/coverage-dvp-swap-html

# Run all coverage tasks
all-coverage: unit-coverage coverage-html
