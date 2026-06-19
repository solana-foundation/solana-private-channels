#!/usr/bin/env bash
set -euo pipefail

# check-env-contract.sh — enforce the env-variable *contract*: `.env.example` is
# the canonical, documented superset of every key any surface actually uses.
#
# The value source legitimately differs per surface (tracked presets for local
# dev, an Ansible-rendered `.env` from env.j2 for deploy), but the *key set* must
# not drift: if a surface references a key `.env.example` doesn't document, a
# `git clone && make docker-up` user silently gets an unset/empty value. This
# check fails closed on that drift. It compares key *names* only, never values.
#
# Usage: check-env-contract.sh   (run from repo root)

cd "$(dirname "$0")/.."

EXAMPLE=".env.example"
# Surfaces whose keys must all be documented in .env.example.
SURFACES=(
  ".env.local"
  ".env.devnet"
  "private-channel-deploy/templates/env.j2"
)

# Extract the set of variable names (LHS of `KEY=`) from a file, ignoring
# comments and blank lines. Works for both plain env files and the Jinja
# template (whose lines are still `KEY={{ ... }}`).
keys() {
  grep -oE '^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*=' "$1" 2>/dev/null \
    | sed -E 's/^[[:space:]]*//; s/=$//' | sort -u
}

if [[ ! -f "$EXAMPLE" ]]; then
  echo "FATAL: $EXAMPLE not found (run from repo root)." >&2
  exit 1
fi

example_keys="$(keys "$EXAMPLE")"
status=0

for surface in "${SURFACES[@]}"; do
  if [[ ! -f "$surface" ]]; then
    echo "WARN: surface not found, skipping: $surface" >&2
    continue
  fi
  # Keys present in the surface but absent from .env.example.
  undocumented="$(comm -23 <(keys "$surface") <(printf '%s\n' "$example_keys"))"
  if [[ -n "$undocumented" ]]; then
    echo "FAIL: $surface references keys not documented in $EXAMPLE:" >&2
    echo "$undocumented" | sed 's/^/  - /' >&2
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "" >&2
  echo "Add the missing key(s) to $EXAMPLE (the canonical contract) or remove" >&2
  echo "them from the surface above. Values may differ per surface; keys must not." >&2
  exit 1
fi

echo "env contract OK: every surface key is documented in $EXAMPLE."
exit 0
