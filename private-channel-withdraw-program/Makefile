SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

PROGRAM_SO := ../target/deploy/private_channel_withdraw_program.so
PROGRAM_KEYPAIR := ../target/deploy/private_channel_withdraw_program-keypair.json
PROGRAM_ENV_KEY := WITHDRAW_PROGRAM_ID

.PHONY: install build fmt generate-idl generate-clients
.PHONY: unit-test integration-test integration-test-no-build all-test
.PHONY: unit-coverage coverage-html all-coverage
.PHONY: build-localnet build-devnet deploy-devnet

# Install dependencies
install:
	pnpm install

# Build the program
# cargo-build-sbf must run from program/ so it sees only the on-chain package.
# From withdraw-program root it also pulls the client crate (with host-only deps
# like tokio/mio/socket2) and fails under solana CLI 3.1.13's stricter sbf target.
build:
	$(MAKE) generate-clients
	cd program && cargo-build-sbf

# Format / lint code
fmt:
	cargo fmt --all
	@cd program && cargo clippy --all-targets -- -D warnings
	@cd tests/integration-tests && cargo clippy --all-targets -- -D warnings
	pnpm format

# Generate IDL from Codama annotations
generate-idl:
	@echo "Generating IDL..."
	pnpm run generate-idl

# Generate clients from IDL using Codama
generate-clients: generate-idl
	@echo "Generating clients..."
	pnpm run generate-clients

### Tests ###

# Run unit tests in program/
# install + generate-clients first so the TS suites can import the gitignored src/generated.
unit-test: install generate-clients
	@echo "Running unit tests for withdraw program..."
	pnpm test:unit
	@cd program && cargo test

# Run integration tests without build (for CI composition)
integration-test-no-build:
	@echo "Running integration tests for withdraw program..."
	@cd tests/integration-tests && cargo test -- --nocapture

# Run integration tests (includes build)
integration-test: build integration-test-no-build

all-test: unit-test integration-test

# Run unit tests with coverage
unit-coverage:
	@echo "Running unit tests with coverage..."
	@mkdir -p ../coverage
	@cd program && cargo llvm-cov --lib --tests --lcov --output-path ../../coverage/coverage-withdraw-unit.lcov

# Generate HTML coverage report
coverage-html:
	@echo "Generating HTML coverage report..."
	@mkdir -p ../coverage
	@cd program && cargo llvm-cov --html --output-dir ../../coverage/coverage-withdraw-html

# Run all coverage tasks
all-coverage: unit-coverage coverage-html

#############
# Localnet
#############
build-localnet:
	$(MAKE) generate-clients
	cd program && cargo-build-sbf
	@# Do NOT patch WITHDRAW_PROGRAM_ID from $(PROGRAM_KEYPAIR): the program ID is fixed by
	@# the program's declare_id! and matched by the indexer's hardcoded constant, so
	@# .env.local carries the canonical ID statically. The build keypair differs from
	@# declare_id, and deploying at it makes the indexer watch the wrong address.

#############
# Devnet
#############

# Build the program with devnet feature
build-devnet:
	$(MAKE) generate-clients
	cd program && cargo-build-sbf

# Deploy to devnet
deploy-devnet: build-devnet
	@echo "Deploying to devnet..."
	@if [ -z "$(DEPLOYER_KEY)" ]; then \
		echo "Error: DEPLOYER_KEY parameter is required. Usage: make deploy-devnet DEPLOYER_KEY=/path/to/deployer-keypair.json"; \
		exit 1; \
	fi
	solana program deploy $(PROGRAM_SO) \
		--program-id $(PROGRAM_KEYPAIR) \
		--keypair $(DEPLOYER_KEY) \
		--url devnet
