#!/usr/bin/env bash
set -euo pipefail

# Tests for update-admin-env.sh: the private key must never land in the tracked
# template; it must go only to the gitignored runtime env file, readable by its owner only.
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

ROOT="$workdir"
# shellcheck source=scripts/tests/env-file-asserts.sh
source "$repo_root/scripts/tests/env-file-asserts.sh"

# Same setup as the audit reproduction: a permissive umask, and no usable TMPDIR.
run_script() {
  (umask 022 && TMPDIR="$NO_TMPDIR" "$script" "$@" >/dev/null)
}

admin_keypair="$workdir/admin.json"
solana-keygen new -o "$admin_keypair" -s --no-bip39-passphrase >/dev/null

tracked_env="$workdir/.env.tracked"
runtime_env="$workdir/.env.runtime"
seed "$tracked_env" ""

run_script "$tracked_env" "$admin_keypair" "$runtime_env"

# 1. The tracked template must carry the PUBLIC admin key and NO secret.
grep -q '^PRIVATE_CHANNEL_ADMIN_KEYS=' "$tracked_env" \
  || fail "tracked file missing PRIVATE_CHANNEL_ADMIN_KEYS"
if grep -qE '^ADMIN_PRIVATE_KEY=.+' "$tracked_env"; then
  fail "tracked file leaked the admin private key"
fi

# 2. The runtime file must carry the private key, non-empty.
admin_priv="$(grep '^ADMIN_PRIVATE_KEY=' "$runtime_env" | tail -n1 | cut -d= -f2-)"
[[ -n "$admin_priv" ]] || fail "ADMIN_PRIVATE_KEY is empty in runtime file"

# 3. A newly created runtime file, and the template, are private.
assert_private "$runtime_env"
assert_private "$tracked_env"

# 4. Appending to an existing world-readable runtime file makes it private and keeps its other lines.
appended_env="$workdir/.env.appended"
seed "$appended_env" $'OTHER=1\n'
run_script "$tracked_env" "$admin_keypair" "$appended_env"
assert_private "$appended_env"
grep -qx 'OTHER=1' "$appended_env" || fail "append dropped an existing line"
grep -q '^ADMIN_PRIVATE_KEY=.' "$appended_env" || fail "append did not write the key"

# 5. Re-running replaces the key in place and keeps the file private.
run_script "$tracked_env" "$admin_keypair" "$runtime_env"
assert_private "$runtime_env"
[[ "$(grep -c '^ADMIN_PRIVATE_KEY=' "$runtime_env")" -eq 1 ]] || fail "key duplicated on re-run"

# 6. No temp file is left next to any written file.
for f in "$tracked_env" "$runtime_env" "$appended_env"; do
  assert_no_temp "$workdir" "$(basename "$f")"
done

echo "PASS: the admin private key stays out of the tracked template and in a private runtime file"
