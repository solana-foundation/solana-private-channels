# syntax=docker/dockerfile:1.7
# Requires Docker >= 26.0 (BuildKit + the `--mount=type=cache` directives below).
#
# NODE_VERSION / PNPM_VERSION: versions.env is the source of truth (consumed by docker
# compose). Standalone builds don't read versions.env, so defaults are set below as
# fallbacks — callers should still set them explicitly via Build Args so a versions.env
# bump doesn't silently drift. Bump these defaults in lockstep with versions.env when
# raising the pins.
# Standalone build (outside compose): see README "Building a single Dockerfile standalone".
ARG NODE_VERSION=24.7.0
FROM node:${NODE_VERSION}-slim AS builder
ARG PNPM_VERSION=10.15.1

# Pin pnpm's content-addressed store to a known path so the cache mount below targets it.
ENV PNPM_HOME=/pnpm
ENV PATH="$PNPM_HOME:$PATH"
RUN npm install -g pnpm@${PNPM_VERSION} \
    && pnpm config set store-dir /pnpm-store --global

WORKDIR /workspace

# Copy program dirs (IDL + codama scripts + package configs)
COPY private-channel-escrow-program ./private-channel-escrow-program
COPY private-channel-withdraw-program ./private-channel-withdraw-program

# Generate TypeScript clients from IDL. The pnpm store is a content-addressed cache;
# node_modules under each project still get materialized as a normal layer.
RUN --mount=type=cache,target=/pnpm-store,sharing=locked \
    cd private-channel-escrow-program && pnpm install && pnpm run generate-clients
RUN --mount=type=cache,target=/pnpm-store,sharing=locked \
    cd private-channel-withdraw-program && pnpm install && pnpm run generate-clients

# Copy and build admin-ui
COPY admin-ui ./admin-ui

WORKDIR /workspace/admin-ui
RUN --mount=type=cache,target=/pnpm-store,sharing=locked \
    pnpm install --frozen-lockfile

ARG PRIVATE_CHANNEL_RPC_URL
ARG PRIVATE_CHANNEL_WS_URL

RUN npx vite build

# Runtime
ARG NODE_VERSION=24.7.0
FROM node:${NODE_VERSION}-slim

WORKDIR /app
COPY --from=builder /workspace/admin-ui/dist ./dist
COPY admin-ui/server.mjs .

USER node

CMD ["node", "server.mjs"]
