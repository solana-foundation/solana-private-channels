SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

.PHONY: install build fmt generate-idl generate-clients
.PHONY: unit-test integration-test integration-test-no-build all-test

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
