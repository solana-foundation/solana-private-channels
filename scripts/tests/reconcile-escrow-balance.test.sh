#!/usr/bin/env bash
set -euo pipefail

# Tests for reconcile-escrow-balance.sh: the DB password may never reach any
# process argv.
#
# Requires `jq` on PATH; spl-token and psql are faked. Run from the repo root:
#   ./scripts/tests/reconcile-escrow-balance.test.sh

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/scripts/reconcile-escrow-balance.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

db_host="localhost"
db_port="5432"
db_name="private_channel"
db_user="indexer"
db_password="reconcile-test-password"
db_url="postgresql://${db_user}:${db_password}@${db_host}:${db_port}/${db_name}"
expected_balance="1000"

pgpass_file="$workdir/pgpass"
printf '%s:%s:%s:%s:%s\n' "$db_host" "$db_port" "$db_name" "$db_user" "$db_password" > "$pgpass_file"
chmod 600 "$pgpass_file"

# Record every child argv. PATH holds only the shims, so a command nobody
# shimmed fails with "command not found" instead of running unrecorded. The fake
# spl-token and psql both report expected_balance, so the balances match.
argv_log="$workdir/argv.log"
shim_dir="$workdir/bin"
mkdir -p "$shim_dir"
real_bash="$(command -v bash)"
cat > "$shim_dir/spl-token" <<EOF
#!$real_bash
printf '%s\n' "spl-token \$*" >> "$argv_log"
printf '{"amount":"%s"}\n' "$expected_balance"
EOF
cat > "$shim_dir/psql" <<EOF
#!$real_bash
printf '%s\n' "psql \$*" >> "$argv_log"
printf '%s\n' "$expected_balance"
EOF
chmod +x "$shim_dir/spl-token" "$shim_dir/psql"
for command_name in date jq tr; do
  real_path="$(command -v "$command_name")" || continue
  cat > "$shim_dir/$command_name" <<EOF
#!$real_bash
printf '%s\n' "$command_name \$*" >> "$argv_log"
exec "$real_path" "\$@"
EOF
  chmod +x "$shim_dir/$command_name"
done

# A legacy caller still passing a DB URL is refused before any child sees it.
legacy_status=0
PATH="$shim_dir" "$real_bash" "$script" EscrowOwnerPda111 MintAddress111 "$db_url" \
  > /dev/null || legacy_status=$?
[[ "$legacy_status" == 2 ]] || fail "legacy DB URL argument exited $legacy_status, expected 2"

# Operators export DATABASE_URL, so the password sits in the environment the
# script inherits; nothing may forward it into an argv.
PATH="$shim_dir" PGHOST="$db_host" PGPORT="$db_port" PGDATABASE="$db_name" \
  PGUSER="$db_user" PGPASSFILE="$pgpass_file" DATABASE_URL="$db_url" \
  "$real_bash" "$script" EscrowOwnerPda111 MintAddress111 > /dev/null \
  || fail "reconciliation did not pass on matching balances"
grep -q '^psql ' "$argv_log" || fail "psql was never invoked"

# 1. The DB password may not appear in any spawned process argv.
if grep -qF "$db_password" "$argv_log"; then
  fail "the DB password appeared in a child process argv"
fi

echo "PASS: the DB password stays out of every process argv"
