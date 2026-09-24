#!/usr/bin/env bash
set -euo pipefail

# check-required-env.sh — fail closed if required deployment secrets are blank.
# Usage: check-required-env.sh <env-file> [<env-file> ...]
# Reads KEY=VALUE literally (no shell sourcing), matching docker compose --env-file:
# an exported process-env value wins even when empty, else the last env-file
# that defines the key.
# No working secrets are shipped; these MUST be set before starting the stack.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <env-file> [<env-file> ...]" >&2
  exit 2
fi

# Required vars that must be non-empty.
REQUIRED_VARS=(
  POSTGRES_PASSWORD
  POSTGRES_REPLICATION_PASSWORD
  # One per service login. init-auth-roles.sql raises on a blank one, which
  # would otherwise surface as postgres-primary failing to initialise.
  POSTGRES_RUNTIME_PASSWORD
  POSTGRES_AUTH_RUNTIME_PASSWORD
  POSTGRES_AUTH_OWNER_PASSWORD
  POSTGRES_GATEWAY_PASSWORD
  POSTGRES_MONITORING_PASSWORD
  # On the indexer cluster, for Grafana's datasource.
  POSTGRES_GRAFANA_PASSWORD
  # The indexer cluster's own superuser.
  POSTGRES_INDEXER_PASSWORD
  # Grafana's administrator login. Its HTTP port reaches a datasource that
  # queries the indexer database.
  GF_ADMIN_PASSWORD
)

# Verify each env file exists up front.
for env_file in "$@"; do
  if [[ ! -f "$env_file" ]]; then
    echo "FATAL: env file not found: ${env_file}" >&2
    exit 1
  fi
done

# Shared with migrate-stack.sh so both resolve a value identically.
# shellcheck source=scripts/env-resolve.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-resolve.sh"

# Collect any required var that resolves to unset or empty.
missing=()
for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "$(resolve_var "$var" "$@")" ]]; then
    missing+=("$var")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "FATAL: required secret(s) unset or empty: ${missing[*]}" >&2
  echo "No default is shipped. Set them in your env file before starting the stack." >&2
  echo "Generate a strong value with: openssl rand -hex 32" >&2
  exit 1
fi

# Values that are non-empty but were once shipped in a tracked preset, so an
# operator who started the stack before they were removed still carries one.
# KEY=VALUE, checked after the non-empty pass.
REJECTED_VALUES=(GF_ADMIN_PASSWORD=admin GF_ADMIN_PASSWORD=admin123)

placeheld=()
for entry in "${REJECTED_VALUES[@]}"; do
  var="${entry%%=*}"
  rejected="${entry#*=}"
  if [[ "$(resolve_var "$var" "$@")" == "$rejected" ]]; then
    placeheld+=("${var} (must not be '${rejected}')")
  fi
done

# Every Postgres login needs its own password. Sharing one hands a compromised
# service the others' rights — and POSTGRES_INDEXER_PASSWORD matching
# POSTGRES_PASSWORD is the sharp case: the indexer processes sit on the same
# network as postgres-primary and the primary's username is in this repository,
# so an equal value lets them authenticate there as superuser.
# The deploy asserts the same thing for the Ansible path.
DISTINCT_VARS=(
  POSTGRES_PASSWORD
  POSTGRES_REPLICATION_PASSWORD
  POSTGRES_RUNTIME_PASSWORD
  POSTGRES_AUTH_RUNTIME_PASSWORD
  POSTGRES_AUTH_OWNER_PASSWORD
  POSTGRES_GATEWAY_PASSWORD
  POSTGRES_MONITORING_PASSWORD
  POSTGRES_GRAFANA_PASSWORD
  POSTGRES_INDEXER_PASSWORD
)

shared=()
for i in "${!DISTINCT_VARS[@]}"; do
  for (( j = i + 1; j < ${#DISTINCT_VARS[@]}; j++ )); do
    left="${DISTINCT_VARS[$i]}"
    right="${DISTINCT_VARS[$j]}"
    if [[ "$(resolve_var "$left" "$@")" == "$(resolve_var "$right" "$@")" ]]; then
      shared+=("${left} == ${right}")
    fi
  done
done

if [[ ${#shared[@]} -gt 0 ]]; then
  echo "FATAL: postgres logins share a password: ${shared[*]}" >&2
  echo "Each needs its own value, or a compromised service holds the others' rights." >&2
  echo "Generate a strong value with: openssl rand -hex 32" >&2
  exit 1
fi

if [[ ${#placeheld[@]} -gt 0 ]]; then
  echo "FATAL: shipped placeholder still in use: ${placeheld[*]}" >&2
  echo "Anyone with the repository knows this value. Replace it, and rotate the" >&2
  echo "credentials of any stack that ran with it." >&2
  echo "Generate a strong value with: openssl rand -hex 32" >&2
  exit 1
fi

exit 0
