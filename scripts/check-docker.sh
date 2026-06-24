#!/usr/bin/env bash
set -euo pipefail

# check-docker.sh — fail closed unless a usable Docker Engine (>= 26) is present.
# Shared guard: the Makefile's `check-docker` target runs this, and deployment
# automation can run the same script so every surface enforces one Docker
# precondition. >= 26 is required for the BuildKit cache mounts the compose
# build targets rely on.

MIN_MAJOR=26

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found on PATH — required for compose-based dev/test stacks (>= ${MIN_MAJOR}.0)" >&2
  exit 1
fi

# Prefer the server (daemon) version; fall back to the client so a stopped
# daemon produces the dedicated "daemon not running?" message below.
ver="$(docker version --format '{{.Server.Version}}' 2>/dev/null \
  || docker version --format '{{.Client.Version}}' 2>/dev/null || true)"
if [ -z "$ver" ]; then
  echo "ERROR: docker present but version could not be read (daemon not running?)" >&2
  exit 1
fi

major="$(echo "$ver" | cut -d. -f1)"
if [ "$major" -lt "$MIN_MAJOR" ] 2>/dev/null; then
  echo "ERROR: docker is $ver; this repo requires >= ${MIN_MAJOR}.0 (BuildKit cache mounts)." >&2
  exit 1
fi
