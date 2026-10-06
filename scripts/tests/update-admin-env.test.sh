#!/usr/bin/env bash
set -euo pipefail

# Tests for update-admin-env.sh: the private key must never land in the tracked
# template; it must go only to the gitignored runtime env file, readable by its owner only,
# and never through a child process argv.
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
  (umask 022 && PATH="$shim_dir" TMPDIR="$NO_TMPDIR" "$script" "$@" >/dev/null)
}

admin_keypair="$workdir/admin.json"
solana-keygen new -o "$admin_keypair" -s --no-bip39-passphrase >/dev/null

tracked_env="$workdir/.env.tracked"
runtime_env="$workdir/.env.runtime"
seed "$tracked_env" ""

# Record the argv of every command run through PATH. PATH holds only the shims,
# so a command nobody shimmed fails with "command not found" instead of running
# unrecorded; one called by absolute path is not seen. The bash shim also records
# the scripts themselves, which run via `#!/usr/bin/env bash`.
argv_log="$workdir/argv.log"
shim_dir="$workdir/bin"
mkdir -p "$shim_dir"
real_bash="$(command -v bash)"
for command_name in awk basename bash cat chmod dirname grep id mkdir mktemp mv rm solana-keygen tr; do
  real_path="$(command -v "$command_name")" || continue
  cat > "$shim_dir/$command_name" <<EOF
#!$real_bash
printf '%s\n' "$command_name \$*" >> "$argv_log"
exec "$real_path" "\$@"
EOF
  chmod +x "$shim_dir/$command_name"
done

run_script "$tracked_env" "$admin_keypair" "$runtime_env"

# 1. The tracked template must carry the PUBLIC admin key and NO secret.
grep -q '^PRIVATE_CHANNEL_ADMIN_KEYS=' "$tracked_env" \
  || fail "tracked file missing PRIVATE_CHANNEL_ADMIN_KEYS"
if grep -qE '^ADMIN_PRIVATE_KEY=.+' "$tracked_env"; then
  fail "tracked file leaked the admin private key"
fi

# 2. The runtime file must carry exactly one key line, holding the full key.
admin_key_bytes="$(tr -d '\n' < "$admin_keypair")"
[[ "$(grep -c '^ADMIN_PRIVATE_KEY=' "$runtime_env")" == 1 ]] \
  || fail "runtime file must hold exactly one ADMIN_PRIVATE_KEY line"
admin_priv="$(grep '^ADMIN_PRIVATE_KEY=' "$runtime_env" | cut -d= -f2-)"
[[ "$admin_priv" == "$admin_key_bytes" ]] || fail "ADMIN_PRIVATE_KEY does not match the keypair"

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

# 7. No command run through PATH may carry the private key in its argv. The
# check means nothing unless the upsert-env.sh child was recorded at all.
grep -qF "upsert-env.sh $runtime_env ADMIN_PRIVATE_KEY" "$argv_log" \
  || fail "upsert-env.sh child never observed"
if grep -qF "$admin_key_bytes" "$argv_log"; then
  fail "admin private key appeared in a child process argv"
fi

echo "PASS: the admin private key stays out of the tracked template and off every PATH command line, in a private runtime file"
