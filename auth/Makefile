.PHONY: build fmt unit-test integration-test
.PHONY: unit-coverage coverage-html all-coverage

# Build the private-channel-auth binary
build:
	@echo "Building private-channel-auth..."
	@cargo build --release

# Format and lint code
fmt:
	@echo "Formatting private-channel-auth..."
	@cargo fmt --all
	@cargo clippy --all-targets -- -D warnings

# Run unit tests
unit-test:
	@echo "Running unit tests for private-channel-auth..."
	@cargo test --lib --bins

# Run integration tests (requires Docker for testcontainers)
integration-test:
	@echo "Running integration tests for private-channel-auth..."
	@cargo test --test integration -- --test-threads=1

# Run unit tests with coverage
unit-coverage:
	@echo "Running unit tests with coverage for private-channel-auth..."
	@mkdir -p ../coverage
	@cargo llvm-cov --lib --bins --tests --lcov --output-path ../coverage/coverage-auth-unit.lcov

# Generate HTML coverage report
coverage-html:
	@echo "Generating HTML coverage report for private-channel-auth..."
	@mkdir -p ../coverage
	@cargo llvm-cov --lib --bins --tests --html --output-dir ../coverage/coverage-auth-html

# Run all coverage tasks
all-coverage: unit-coverage coverage-html
