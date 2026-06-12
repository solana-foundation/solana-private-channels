#!/usr/bin/env bash
set -euo pipefail

# Tests for update-admin-env.sh: the private key must never land in the tracked
# template; it must go only to the gitignored runtime env file.
#
# Requires `solana-keygen` on PATH. Run from the repo root:
#   ./scripts/tests/update-admin-env.test.sh

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/scripts/update-admin-env.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

admin_keypair="$workdir/admin.json"
solana-keygen new -o "$admin_keypair" -s --no-bip39-passphrase >/dev/null

tracked_env="$workdir/.env.tracked"
runtime_env="$workdir/.env.runtime"
: > "$tracked_env"

"$script" "$tracked_env" "$admin_keypair" "$runtime_env" >/dev/null

# 1. The tracked template must carry the PUBLIC admin key and NO secret.
grep -q '^PRIVATE_CHANNEL_ADMIN_KEYS=' "$tracked_env" \
  || fail "tracked file missing PRIVATE_CHANNEL_ADMIN_KEYS"
if grep -qE '^ADMIN_PRIVATE_KEY=.+' "$tracked_env"; then
  fail "tracked file leaked the admin private key"
fi

# 2. The runtime file must carry the private key, non-empty.
admin_priv="$(grep '^ADMIN_PRIVATE_KEY=' "$runtime_env" | tail -n1 | cut -d= -f2-)"
[[ -n "$admin_priv" ]] || fail "ADMIN_PRIVATE_KEY is empty in runtime file"

echo "PASS: the admin private key stays out of the tracked template"
