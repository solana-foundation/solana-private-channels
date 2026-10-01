#!/usr/bin/env bash
set -euo pipefail

# Tests for update-admin-env.sh: the private key must never land in the tracked
# template; it must go only to the gitignored runtime env file, and never through
# a child process argv.
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

# Record every child argv. PATH holds only the shims, so a command nobody
# shimmed fails with "command not found" instead of running unrecorded. The bash
# shim also records the scripts themselves, which run via `#!/usr/bin/env bash`.
argv_log="$workdir/argv.log"
shim_dir="$workdir/bin"
mkdir -p "$shim_dir"
real_bash="$(command -v bash)"
for command_name in awk bash cat chmod dirname grep mkdir mktemp mv solana-keygen tr; do
  real_path="$(command -v "$command_name")" || continue
  cat > "$shim_dir/$command_name" <<EOF
#!$real_bash
printf '%s\n' "$command_name \$*" >> "$argv_log"
exec "$real_path" "\$@"
EOF
  chmod +x "$shim_dir/$command_name"
done

# A freshly created runtime file must be readable by its owner only.
PATH="$shim_dir" "$script" "$tracked_env" "$admin_keypair" "$runtime_env" >/dev/null
runtime_mode="$(stat -c %a "$runtime_env" 2>/dev/null || stat -f %Lp "$runtime_env")"
[[ "$runtime_mode" == "600" ]] || fail "runtime file created with mode $runtime_mode, expected 600"

# So must a world-readable runtime file the key gets appended to.
printf 'OTHER=1\n' > "$runtime_env"
chmod 644 "$runtime_env"
PATH="$shim_dir" "$script" "$tracked_env" "$admin_keypair" "$runtime_env" >/dev/null
runtime_mode="$(stat -c %a "$runtime_env" 2>/dev/null || stat -f %Lp "$runtime_env")"
[[ "$runtime_mode" == "600" ]] || fail "appended runtime file kept mode $runtime_mode, expected 600"

# Last run rewrites the existing key line.
PATH="$shim_dir" "$script" "$tracked_env" "$admin_keypair" "$runtime_env" >/dev/null

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

# 3. No spawned process may carry the private key in its argv. The check means
# nothing unless the upsert-env.sh child was recorded at all.
grep -qF "upsert-env.sh $runtime_env ADMIN_PRIVATE_KEY" "$argv_log" \
  || fail "upsert-env.sh child never observed"
if grep -qF "$admin_key_bytes" "$argv_log"; then
  fail "admin private key appeared in a child process argv"
fi

echo "PASS: the admin private key stays out of the tracked template and every argv, in an owner-only file"
