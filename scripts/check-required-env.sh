#!/usr/bin/env bash
set -euo pipefail

# check-required-env.sh — fail closed if required deployment secrets are blank.
# Usage: check-required-env.sh <env-file> [<env-file> ...]
# Sources each env file in order (later files override earlier ones), then
# asserts POSTGRES_PASSWORD and POSTGRES_REPLICATION_PASSWORD are non-empty.
# No working secrets are shipped; these MUST be set before starting the stack.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <env-file> [<env-file> ...]" >&2
  exit 2
fi

# Required vars that must be non-empty after sourcing the env-file chain.
REQUIRED_VARS=(POSTGRES_PASSWORD POSTGRES_REPLICATION_PASSWORD)

# Source each provided env file so its key=value pairs populate the environment.
for env_file in "$@"; do
  if [[ ! -f "$env_file" ]]; then
    echo "FATAL: env file not found: ${env_file}" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  . "$env_file"
  set +a
done

# Collect any required var that is unset or empty.
missing=()
for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    missing+=("$var")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "FATAL: required secret(s) unset or empty: ${missing[*]}" >&2
  echo "No default is shipped. Set them in your env file before starting the stack." >&2
  echo "Generate a strong value with: openssl rand -hex 32" >&2
  exit 1
fi

exit 0
