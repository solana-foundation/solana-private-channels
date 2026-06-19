.PHONY: build fmt
.PHONY: compose-indexer compose-operator compose-all
.PHONY: start-validator run-yellowstone run-rpc run-operator
.PHONY: unit-test all-test
.PHONY: unit-coverage coverage-html all-coverage
.PHONY: run-mock-transactions
.PHONY: yellowstone-clone build-yellowstone-plugin clean-yellowstone

# Configuration
# Files excluded from coverage — error enums, constants, CLI entrypoints, models, types.
# These inflate the denominator without meaningful test value.
# indexer/indexer.rs, operator/operator.rs — pure orchestration that blocks 
# on signal::ctrl_c(); no logic of their own and covered by integration tests.
COV_IGNORE := --ignore-filename-regex '(error/|constants\.rs|bin/|models\.rs|shutdown_utils\.rs|types\.rs|indexer/indexer\.rs|operator/operator\.rs)'

PRIVATE_CHANNEL_ESCROW_PROGRAM_ID ?= GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83
PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID ?= J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi
PRIVATE_CHANNEL_ESCROW_PROGRAM_SO ?= ../target/deploy/private_channel_escrow_program.so
PRIVATE_CHANNEL_WITHDRAW_PROGRAM_SO ?= ../target/deploy/private_channel_withdraw_program.so
GEYSER_CONFIG ?= src/indexer/datasource/common/yellowstone_grpc/config.json
# No default credential shipped. Set DATABASE_URL when running, e.g.
# DATABASE_URL=postgres://user:pass@localhost:5434/indexer (loopback/internal-only).
DATABASE_URL ?=
ESCROW_INSTANCE_ID ?=

# Yellowstone gRPC plugin configuration
YELLOWSTONE_REPO ?= https://github.com/rpcpool/yellowstone-grpc.git
YELLOWSTONE_DIR ?= .yellowstone-grpc
YELLOWSTONE_BRANCH ?= fix-core-affinity-mac
# cargo emits the plugin as .so on Linux and .dylib on macOS; pick the right source extension.
YELLOWSTONE_LIB_EXT := $(if $(filter Darwin,$(shell uname -s)),dylib,so)
YELLOWSTONE_DYLIB_SRC = $(YELLOWSTONE_DIR)/target/release/libyellowstone_grpc_geyser.$(YELLOWSTONE_LIB_EXT)
# Canonical name config.json references; dlopen loads by file format not extension, so a Linux .so copied here loads fine.
YELLOWSTONE_DYLIB_DEST = src/indexer/datasource/common/yellowstone_grpc/libyellowstone_grpc_geyser.dylib

## Build the indexer
build:
	@echo "Building indexer..."
	@cargo build --release

## Format code
fmt:
	@echo "Formatting code..."
	@cargo fmt
	@cargo clippy --all-targets --all-features -- -D warnings

###################################
# Docker compose services
###################################
compose-indexer:
	@docker compose up -d validator indexer-solana indexer-private-channel

compose-operator:
	@docker compose up -d postgres-indexer operator-solana operator-private-channel

compose-all:
	@docker compose up -d validator indexer-solana indexer-private-channel postgres-indexer operator-solana operator-private-channel

###################################
# Individual service start
###################################
## Start local Solana validator with Yellowstone gRPC plugin
start-validator:
	@echo "Starting Solana test validator with Yellowstone gRPC..."
	@echo "gRPC endpoint will be available at: localhost:10000"
	@echo "Prometheus metrics at: localhost:8999"
	@solana-test-validator -r \
		--bpf-program $(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID) $(PRIVATE_CHANNEL_ESCROW_PROGRAM_SO) \
		--bpf-program $(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID) $(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_SO) \
		--geyser-plugin-config $(GEYSER_CONFIG) \
		--port 18899

## Run the indexer with Yellowstone gRPC datasource
run-yellowstone:
	@echo "Running indexer with Yellowstone gRPC datasource..."
	@if [ -z "$(ESCROW_INSTANCE_ID)" ]; then \
		echo "ERROR: ESCROW_INSTANCE_ID is required for escrow program"; \
		echo "Usage: make run-yellowstone ESCROW_INSTANCE_ID=<your_instance_pubkey>"; \
		exit 1; \
	fi
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "ERROR: DATABASE_URL is required (no default credential shipped)"; \
		echo "Usage: make run-yellowstone DATABASE_URL=postgres://user:pass@localhost:5434/indexer ..."; \
		exit 1; \
	fi
	@COMMON_ESCROW_INSTANCE_ID=$(ESCROW_INSTANCE_ID) \
		DATABASE_URL=$(DATABASE_URL) \
		cargo run --bin private-channel-indexer -- \
		--config config/local/indexer-solana.toml \
		--verbose \
		indexer

## Run the indexer with RPC polling datasource
run-rpc:
	@echo "Running indexer with RPC polling..."
	@if [ -z "$(ESCROW_INSTANCE_ID)" ]; then \
		echo "ERROR: ESCROW_INSTANCE_ID is required for escrow program"; \
		echo "Usage: make run-rpc ESCROW_INSTANCE_ID=<your_instance_pubkey>"; \
		exit 1; \
	fi
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "ERROR: DATABASE_URL is required (no default credential shipped)"; \
		echo "Usage: make run-rpc DATABASE_URL=postgres://user:pass@localhost:5434/indexer ..."; \
		exit 1; \
	fi
	@COMMON_ESCROW_INSTANCE_ID=$(ESCROW_INSTANCE_ID) \
		DATABASE_URL=$(DATABASE_URL) \
		cargo run --bin private-channel-indexer -- \
		--config config/local/indexer-private-channel.toml \
		--verbose \
		indexer

## Run the operator
run-operator:
	@echo "Running operator..."
	@if [ -z "$(ESCROW_INSTANCE_ID)" ]; then \
		echo "ERROR: ESCROW_INSTANCE_ID is required for escrow program"; \
		echo "Usage: make run-operator ESCROW_INSTANCE_ID=<your_instance_pubkey>"; \
		exit 1; \
	fi
	@if [ -z "$(ADMIN_PRIVATE_KEY)" ]; then \
		echo "ERROR: ADMIN_PRIVATE_KEY is required"; \
		echo "Usage: make run-operator ADMIN_PRIVATE_KEY=<key> [OPERATOR_PRIVATE_KEY=<key>]"; \
		exit 1; \
	fi
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "ERROR: DATABASE_URL is required (no default credential shipped)"; \
		echo "Usage: make run-operator DATABASE_URL=postgres://user:pass@localhost:5434/indexer ..."; \
		exit 1; \
	fi
	@COMMON_ESCROW_INSTANCE_ID=$(ESCROW_INSTANCE_ID) \
		DATABASE_URL=$(DATABASE_URL) \
		ADMIN_SIGNER=memory \
		ADMIN_PRIVATE_KEY=$(ADMIN_PRIVATE_KEY) \
		OPERATOR_SIGNER=memory \
		OPERATOR_PRIVATE_KEY=$(or $(OPERATOR_PRIVATE_KEY),$(ADMIN_PRIVATE_KEY)) \
		cargo run --bin private-channel-indexer -- \
		--config config/local/operator-solana.toml \
		--verbose \
		operator

###################################
# Testing commands
###################################
## Run unit tests for all datasources
unit-test:
	@echo "Testing all datasources sequentially..."
	@echo "→ Testing yellowstone datasource..."
	@cargo test --lib --features datasource-yellowstone
	@echo "→ Testing rpc datasource..."
	@cargo test --lib --features datasource-rpc
	@echo "✅ All datasources tested"

## Run unit tests with coverage (accumulates both feature variants)
unit-coverage:
	@echo "Running unit tests with coverage for indexer..."
	@mkdir -p ../coverage
	@cargo llvm-cov clean --workspace
	@echo "→ Coverage: yellowstone datasource..."
	@cargo llvm-cov test --no-report --lib --features datasource-yellowstone
	@echo "→ Coverage: rpc datasource..."
	@cargo llvm-cov test --no-report --lib --features datasource-rpc
	@echo "→ Coverage: postgres integration tests..."
	@cargo llvm-cov test --no-report --test postgres_db_test
	@cargo llvm-cov test --no-report --test reconciliation_db_test
	@cargo llvm-cov report --lcov --output-path ../coverage/coverage-indexer-unit.lcov $(COV_IGNORE)

## Generate HTML coverage report (accumulates both feature variants)
coverage-html:
	@echo "Generating HTML coverage report for indexer..."
	@mkdir -p ../coverage
	@cargo llvm-cov clean --workspace
	@cargo llvm-cov test --no-report --lib --features datasource-yellowstone
	@cargo llvm-cov test --no-report --lib --features datasource-rpc
	@cargo llvm-cov test --no-report --test postgres_db_test
	@cargo llvm-cov test --no-report --test reconciliation_db_test
	@cargo llvm-cov report --html --output-dir ../coverage/coverage-indexer-html $(COV_IGNORE)

## Run all coverage tasks
all-coverage: unit-coverage coverage-html

## Test all
all-test: unit-test

## Run the mock transactions generator
run-mock-transactions:
	@echo "Running mock transactions generator..."
	@cargo run --bin generate_transactions


###################################
# Yellowstone
###################################
## Clone or update Yellowstone gRPC repository
yellowstone-clone:
	@if [ -d "$(YELLOWSTONE_DIR)" ]; then \
		echo "Updating Yellowstone gRPC repository..."; \
		cd $(YELLOWSTONE_DIR) && git pull origin $(YELLOWSTONE_BRANCH); \
	else \
		echo "Cloning Yellowstone gRPC repository..."; \
		git clone $(YELLOWSTONE_REPO) $(YELLOWSTONE_DIR); \
		cd $(YELLOWSTONE_DIR) && git checkout $(YELLOWSTONE_BRANCH); \
	fi

## Build Yellowstone gRPC Geyser plugin
build-yellowstone-plugin: yellowstone-clone
	@echo "Building Yellowstone gRPC Geyser plugin..."
	@cd $(YELLOWSTONE_DIR)/yellowstone-grpc-geyser && cargo build --release --no-default-features
	@echo "Copying plugin ($(YELLOWSTONE_LIB_EXT)) to indexer..."
	@cp $(YELLOWSTONE_DYLIB_SRC) $(YELLOWSTONE_DYLIB_DEST)
	@echo "Plugin built and copied to: $(YELLOWSTONE_DYLIB_DEST)"

## Clean Yellowstone build artifacts
clean-yellowstone:
	@echo "Cleaning Yellowstone build artifacts..."
	@rm -rf $(YELLOWSTONE_DIR)
