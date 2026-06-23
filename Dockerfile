# syntax=docker/dockerfile:1.7
# Multi-stage Dockerfile for Solana Private Channels blockchain
#
# SOLANA_VERSION is the source of truth in versions.env.
# Build via: `docker compose --env-file versions.env --env-file .env build <service>`
# Standalone build (outside compose): see README "Building a single Dockerfile standalone".
# Requires Docker >= 26.0 (BuildKit + the `--mount=type=cache` directives below).

ARG SOLANA_VERSION
ARG PNPM_VERSION

# Stage 1: Builder
FROM --platform=linux/amd64 rust:bookworm AS builder
ARG SOLANA_VERSION
ARG PNPM_VERSION

# Disable the base image's apt auto-clean so the cache mount below persists downloaded .debs.
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

# Install build dependencies and update to nightly
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y \
    clang \
    cmake \
    libhidapi-dev \
    libprotobuf-dev \
    libssl-dev \
    libudev-dev \
    pkg-config \
    protobuf-compiler
# rustup pulls the channel pinned in rust-toolchain.toml the first time cargo
# runs in the workspace below — no `rustup default` needed.

# Install Node.js and pnpm
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y nodejs \
    && test -n "${PNPM_VERSION}" || (echo "ERROR: PNPM_VERSION build arg is required (use --env-file versions.env)" && exit 1) \
    && npm install -g pnpm@${PNPM_VERSION}

# Install Solana CLI — version driven by versions.env (SOLANA_VERSION).
# Drifting this version from the validator image or from Cargo.toml's solana-* crates
# reproduces the version-matrix bug that motivated consolidating into versions.env.
RUN test -n "${SOLANA_VERSION}" || (echo "ERROR: SOLANA_VERSION build arg is required (use --env-file versions.env)" && exit 1) \
    && sh -c "$(curl -sSfL https://release.anza.xyz/v${SOLANA_VERSION}/install)" \
    && echo 'export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"' >> ~/.bashrc
ENV PATH="/root/.local/share/solana/install/active_release/bin:${PATH}"

# Convention used throughout this builder stage: build artifacts are copied into /out/
# before the next stage references them.
#
# Why: the cargo build steps below mount /usr/src/private_channel/target as a BuildKit cache
# (`--mount=type=cache`), which is *not* visible to later stages' `COPY --from=builder`
# and is also not visible to subsequent RUN steps that don't re-mount it. /out/ is a
# normal image layer, so artifacts placed there persist across RUN steps and are
# reachable from the runtime stage.

# Set working directory
WORKDIR /usr/src/private_channel

# Copy workspace cargo files first for better caching.
# rust-toolchain.toml MUST be present alongside Cargo.toml so rustup picks
# the pinned channel (1.91.0) for cargo invocations inside the workspace,
# matching the host's `make build` behaviour. 
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml ./core/
COPY gateway/Cargo.toml ./gateway/
COPY indexer/Cargo.toml ./indexer/
COPY auth/Cargo.toml ./auth/

# Copy Cargo.toml files for other workspace members (to satisfy workspace references)
COPY private-channel-escrow-program/program/Cargo.toml ./private-channel-escrow-program/program/
COPY private-channel-escrow-program/tests/integration-tests/Cargo.toml ./private-channel-escrow-program/tests/integration-tests/
COPY private-channel-escrow-program/clients/rust/Cargo.toml ./private-channel-escrow-program/clients/rust/
COPY private-channel-withdraw-program/program/Cargo.toml ./private-channel-withdraw-program/program/
COPY private-channel-withdraw-program/tests/integration-tests/Cargo.toml ./private-channel-withdraw-program/tests/integration-tests/
COPY private-channel-withdraw-program/clients/rust/Cargo.toml ./private-channel-withdraw-program/clients/rust/
COPY dvp-swap-program/program/Cargo.toml ./dvp-swap-program/program/
COPY dvp-swap-program/tests/integration-tests/Cargo.toml ./dvp-swap-program/tests/integration-tests/
COPY dvp-swap-program/tests/transfer-hook-fixture/Cargo.toml ./dvp-swap-program/tests/transfer-hook-fixture/
COPY dvp-swap-program/clients/rust/Cargo.toml ./dvp-swap-program/clients/rust/
COPY integration/Cargo.toml ./integration/
COPY test_utils/Cargo.toml ./test_utils/
COPY scripts/devnet/Cargo.toml ./scripts/devnet/
COPY metrics/Cargo.toml ./metrics/
COPY bench-tps/Cargo.toml ./bench-tps/

# Create dummy lib.rs files for workspace members we're not building
RUN mkdir -p private-channel-escrow-program/program/src private-channel-escrow-program/tests/integration-tests/src \
    private-channel-escrow-program/clients/rust/src private-channel-withdraw-program/program/src \
    private-channel-withdraw-program/tests/integration-tests/src \
    integration/src gateway/src indexer/src test_utils/src scripts/devnet/src \
    private-channel-escrow-program/clients/rust/src private-channel-withdraw-program/clients/rust/src \
    dvp-swap-program/program/src dvp-swap-program/tests/integration-tests/src \
    dvp-swap-program/tests/transfer-hook-fixture/src dvp-swap-program/clients/rust/src \
    core/src metrics/src auth/src bench-tps/src
RUN touch private-channel-escrow-program/program/src/lib.rs private-channel-escrow-program/tests/integration-tests/src/lib.rs \
    private-channel-escrow-program/clients/rust/src/lib.rs private-channel-withdraw-program/program/src/lib.rs \
    private-channel-withdraw-program/tests/integration-tests/src/lib.rs \
    integration/src/lib.rs gateway/src/lib.rs indexer/src/lib.rs \
    test_utils/src/lib.rs scripts/devnet/src/lib.rs \
    private-channel-escrow-program/clients/rust/src/lib.rs private-channel-withdraw-program/clients/rust/src/lib.rs \
    dvp-swap-program/program/src/lib.rs dvp-swap-program/tests/integration-tests/src/lib.rs \
    dvp-swap-program/tests/transfer-hook-fixture/src/lib.rs dvp-swap-program/clients/rust/src/lib.rs \
    core/src/lib.rs metrics/src/lib.rs auth/src/lib.rs && \
    printf 'fn main() {}\n' > bench-tps/src/main.rs && \
    printf 'fn main() {}\n' > auth/src/main.rs

# Build the project with the dummy files. We can cache this layer.
# Cache mounts: target/ holds compiled artifacts; cargo registry/git hold downloaded crate sources.
# All three are reused across rebuilds, turning a cold ~30 min build into <2 min when only
# source changes.
RUN --mount=type=cache,target=/usr/src/private_channel/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release

# First, do the real build for the programs
COPY Makefile ./Makefile
COPY private-channel-escrow-program ./private-channel-escrow-program
COPY private-channel-withdraw-program ./private-channel-withdraw-program
COPY dvp-swap-program ./dvp-swap-program
RUN --mount=type=cache,target=/usr/src/private_channel/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    make -C private-channel-escrow-program install build \
    && make -C private-channel-withdraw-program install build \
    && make -C dvp-swap-program install generate-clients \
    && (cd dvp-swap-program/program && cargo-build-sbf) \
    && mkdir -p /out/deploy \
    && cp target/deploy/private_channel_escrow_program.so /out/deploy/ \
    && cp target/deploy/private_channel_withdraw_program.so /out/deploy/ \
    && cp target/deploy/dvp_swap_program.so /out/deploy/

# Next, do the real build for the other components
COPY core ./core
COPY gateway ./gateway
COPY indexer ./indexer
COPY metrics ./metrics
COPY auth ./auth

# core/precompiles/private_channel_withdraw_program.so is a symlink into target/deploy/ (used by
# include_bytes! in core). The cache-mounted target/ isn't reliably available to the next
# build, so swap the symlink for the real .so. rm first — otherwise cp follows the symlink
# and writes to the wrong place.
RUN rm -f core/precompiles/private_channel_withdraw_program.so \
    && cp /out/deploy/private_channel_withdraw_program.so core/precompiles/private_channel_withdraw_program.so \
    && rm -f core/precompiles/dvp_swap_program.so \
    && cp /out/deploy/dvp_swap_program.so core/precompiles/dvp_swap_program.so

# Final build — binaries are copied to /out/ per the convention noted above.
RUN --mount=type=cache,target=/usr/src/private_channel/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release \
        -p private-channel-core \
        -p private-channel-gateway \
        -p private-channel-indexer \
        -p auth \
    && mkdir -p /out \
    && cp target/release/node /out/node \
    && cp target/release/admin /out/admin \
    && cp target/release/gateway /out/gateway \
    && cp target/release/indexer /out/indexer \
    && cp target/release/streamer /out/streamer \
    && cp target/release/auth /out/auth

# Stage 2: Runtime
FROM --platform=linux/amd64 debian:bookworm-slim

# Disable the base image's apt auto-clean so the cache mount below persists downloaded .debs.
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

# Install runtime dependencies. curl is used by compose healthcheck probes
# against the service's /health and /metrics endpoints.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3

# Create a non-root user to run the application
RUN useradd -m -u 1000 -s /bin/bash private_channel

# Copy the binaries from builder. Source paths are /out/ (a normal layer in the builder
# stage), not target/release/ (a cache mount which is not visible across stages).
COPY --from=builder /out/node /usr/local/bin/private-channel-node
COPY --from=builder /out/admin /usr/local/bin/admin
COPY --from=builder /out/gateway /usr/local/bin/gateway
COPY --from=builder /out/indexer /usr/local/bin/indexer
COPY --from=builder /out/streamer /usr/local/bin/streamer
COPY --from=builder /out/auth /usr/local/bin/auth

# Copy indexer/operator config files
COPY indexer/config /etc/private_channel/config

# Create data directory for RocksDB
RUN mkdir -p /data && chown private_channel:private_channel /data

# Switch to non-root user
USER private_channel

# No default entrypoint - let docker-compose specify the command
# This ensures proper signal handling for graceful shutdown
