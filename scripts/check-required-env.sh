#!/usr/bin/env bash
set -euo pipefail

# check-required-env.sh — fail closed if required deployment secrets are blank.
# Usage: check-required-env.sh <env-file> [<env-file> ...]
# Reads KEY=VALUE literally (no shell sourcing), matching docker compose --env-file:
# a non-empty process-env value wins, else the last env-file that defines the key.
# No working secrets are shipped; these MUST be set before starting the stack.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <env-file> [<env-file> ...]" >&2
  exit 2
fi

# Required vars that must be non-empty.
REQUIRED_VARS=(POSTGRES_PASSWORD POSTGRES_REPLICATION_PASSWORD)

# Verify each env file exists up front.
for env_file in "$@"; do
  if [[ ! -f "$env_file" ]]; then
    echo "FATAL: env file not found: ${env_file}" >&2
    exit 1
  fi
done

# Resolve a key the way compose will, without sourcing (no code execution).
resolve_var() {
  local key="$1"
  shift
  # A non-empty exported process-env value takes precedence over the files.
  if [[ -n "${!key:-}" ]]; then
    printf '%s' "${!key}"
    return 0
  fi
  local val="" line
  for f in "$@"; do
    # Last literal `KEY=` line wins; allow leading space and an `export` prefix.
    line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$f" | tail -n1 || true)"
    [[ -n "$line" ]] || continue
    val="${line#*=}"
    # Drop a trailing CR so CRLF files don't read as non-empty.
    val="${val%$'\r'}"
    # Strip one layer of surrounding quotes so KEY="" reads as empty.
    case "$val" in
      \"*\") val="${val#\"}" && val="${val%\"}" ;;
      \'*\') val="${val#\'}" && val="${val%\'}" ;;
    esac
  done
  printf '%s' "$val"
}

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

exit 0
