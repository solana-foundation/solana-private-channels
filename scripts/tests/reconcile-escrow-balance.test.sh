#!/usr/bin/env bash
set -euo pipefail

# Tests for reconcile-escrow-balance.sh: the DB password may never reach any
# process argv, and psql gets the query on stdin and its connection from the
# libpq env.
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

db_name="private_channel"
db_password="reconcile-test-password"
db_url="postgresql://indexer:${db_password}@localhost:5432/${db_name}"
expected_balance="1000"
escrow_owner="EscrowOwnerPda111"
mint="MintAddress111"

pgpass_file="$workdir/pgpass"
printf 'localhost:5432:%s:indexer:%s\n' "$db_name" "$db_password" > "$pgpass_file"
chmod 600 "$pgpass_file"

# Record the argv of every command run through PATH. PATH holds only the shims,
# so a command nobody shimmed fails with "command not found" instead of running
# unrecorded; one called by absolute path is not seen. The fake spl-token and
# psql both report expected_balance, so the balances match. The fake psql also
# saves its args, stdin and libpq env so the query and connection can be checked.
argv_log="$workdir/argv.log"
psql_args="$workdir/psql.args"
psql_stdin="$workdir/psql.stdin"
psql_env="$workdir/psql.env"
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
printf '%s\n' "\$@" > "$psql_args"
printf '%s\n' "\$(</dev/stdin)" > "$psql_stdin"
printf 'PGDATABASE=%s\nPGPASSFILE=%s\n' "\${PGDATABASE:-}" "\${PGPASSFILE:-}" > "$psql_env"
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
# PGDATABASE is set so the argument count is the only reason left to refuse.
legacy_status=0
PATH="$shim_dir" PGDATABASE="$db_name" "$real_bash" "$script" "$escrow_owner" "$mint" "$db_url" \
  > /dev/null || legacy_status=$?
[[ "$legacy_status" == 2 ]] || fail "legacy DB URL argument exited $legacy_status, expected 2"

# psql is fake, so nothing logs in: this run checks what the script hands psql,
# not that real psql authenticates. stdin is /dev/null so a query that skips
# stdin shows up as empty instead of blocking on a terminal.
PATH="$shim_dir" PGDATABASE="$db_name" PGPASSFILE="$pgpass_file" \
  "$real_bash" "$script" "$escrow_owner" "$mint" < /dev/null > /dev/null \
  || fail "reconciliation did not pass on matching balances"
grep -q '^psql ' "$argv_log" || fail "psql was never invoked"

# 1. The DB password may not appear in the argv of any command run through PATH.
if grep -qF "$db_password" "$argv_log"; then
  fail "the DB password appeared in a child process argv"
fi

# 2. The query goes in on stdin, the only place psql fills in :'mint'.
grep -qF "WHERE mint = :'mint'" "$psql_stdin" || fail "the reconcile query did not reach psql on stdin"
if grep -qxF -e '-c' "$psql_args"; then
  fail "the query was passed with -c, where psql never fills in :'mint'"
fi
grep -qxF "mint=$mint" "$psql_args" || fail "psql was not given -v mint=$mint"

# 3. psql binds the mint; the script may not splice it into the SQL text.
if grep -qF "$mint" "$psql_stdin"; then
  fail "the mint was spliced into the SQL text"
fi

# 4. psql connects from the libpq env, with no connection string on its command line.
grep -qxF "PGDATABASE=$db_name" "$psql_env" || fail "psql did not see PGDATABASE"
grep -qxF "PGPASSFILE=$pgpass_file" "$psql_env" || fail "psql did not see PGPASSFILE"
if grep -qE -e '^(-d|--dbname(=.*)?)$|://|password=' "$psql_args"; then
  fail "psql was given a connection string"
fi

echo "PASS: the DB password stays off every PATH command line, and psql gets the query on stdin with the mint bound and its connection from the libpq env"
