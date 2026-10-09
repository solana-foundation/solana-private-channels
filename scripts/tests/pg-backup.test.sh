#!/usr/bin/env bash
set -euo pipefail

# Tests for pg-backup.sh with stubbed pg_isready, pg_basebackup, pg_archivecleanup and sleep.
# The sleep stub fails, which ends the script after one backup cycle.
#
# Needs only bash, sh, tar and coreutils. Run from the repo root:
#   ./scripts/tests/pg-backup.test.sh

ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/pg-backup.sh"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# new_case NAME [BACKUP_MODE]: builds a stub bin dir. BACKUP_MODE "complete" (default)
# writes a base.tar.gz with a backup_label, "incomplete" leaves the directory empty.
new_case() {
  local d="$ROOT/$1" mode="${2:-complete}"
  mkdir -p "$d/bin" "$d/backups" "$d/wal"
  : >"$d/calls"
  cat >"$d/bin/pg_isready" <<STUB
#!/bin/sh
echo pg_isready >>"$d/calls"
exit 0
STUB
  cat >"$d/bin/pg_basebackup" <<STUB
#!/bin/sh
echo pg_basebackup >>"$d/calls"
while [ \$# -gt 0 ]; do
  if [ "\$1" = "-D" ]; then dest="\$2"; fi
  shift
done
mkdir -p "\$dest"
if [ "$mode" = "complete" ]; then
  tmp="\$(mktemp -d)"
  echo "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)" >"\$tmp/backup_label"
  tar -czf "\$dest/base.tar.gz" -C "\$tmp" backup_label
  rm -rf "\$tmp"
fi
STUB
  cat >"$d/bin/pg_archivecleanup" <<STUB
#!/bin/sh
echo "pg_archivecleanup \$*" >>"$d/calls"
STUB
  cat >"$d/bin/sleep" <<STUB
#!/bin/sh
echo "sleep \$*" >>"$d/calls"
exit 1
STUB
  chmod +x "$d/bin/"*
  echo "$d"
}

# run_script DIR [VAR=VALUE ...]: runs the script, leaves stdout+stderr in DIR/out, status in DIR/status.
run_script() {
  local d="$1"
  shift
  set +e
  env -i PATH="$d/bin:$PATH" PGHOST=h PGUSER=u PGPASSWORD=p \
    BACKUP_DIR="$d/backups" WAL_ARCHIVE_DIR="$d/wal" "$@" \
    sh "$SCRIPT" >"$d/out" 2>&1
  echo $? >"$d/status"
  set -e
}

assert_rejected() {
  local d="$1" var="$2"
  [ "$(cat "$d/status")" -ne 0 ] || fail "$var: expected a non-zero exit"
  grep -q "$var" "$d/out" || fail "$var: the error must name the variable"
  ! grep -q pg_isready "$d/calls" || fail "$var: validation must run before waiting for Postgres"
  ! grep -q pg_basebackup "$d/calls" || fail "$var: no backup may start on a bad value"
}

test_rejects_bad_retention() {
  local v d
  for v in 0 00 08 -1 3.5 abc " 3" 12345; do
    d="$(new_case "retention-$(echo "$v" | tr -c 'a-z0-9' '_')")"
    run_script "$d" PG_BACKUP_RETENTION_COUNT="$v"
    assert_rejected "$d" PG_BACKUP_RETENTION_COUNT
  done
}

test_rejects_bad_interval() {
  local v d
  for v in 0 00 08 -1 3.5 abc " 6" 12345; do
    d="$(new_case "interval-$(echo "$v" | tr -c 'a-z0-9' '_')")"
    run_script "$d" PG_BACKUP_INTERVAL_HOURS="$v"
    assert_rejected "$d" PG_BACKUP_INTERVAL_HOURS
  done
}

test_valid_values_complete_one_cycle() {
  local d
  d="$(new_case valid)"
  run_script "$d" PG_BACKUP_RETENTION_COUNT=1 PG_BACKUP_INTERVAL_HOURS=2
  grep -q "Backup complete" "$d/out" || fail "valid: backup must run"
  grep -q "Retention verified" "$d/out" || fail "valid: the post-prune check must pass and say so"
  grep -q "pg_archivecleanup" "$d/calls" || fail "valid: WAL pruning must still run"
  grep -qx "sleep 7200" "$d/calls" || fail "valid: interval must be converted to seconds"
  [ "$(find "$d/backups" -maxdepth 1 -type d -name 'base_*' | wc -l)" -eq 1 ] || fail "valid: one backup must remain"
}

test_defaults_apply_when_unset_or_empty() {
  local d
  d="$(new_case defaults)"
  run_script "$d" PG_BACKUP_RETENTION_COUNT= PG_BACKUP_INTERVAL_HOURS=
  grep -q "Retention verified" "$d/out" || fail "defaults: an empty value must mean the default"
  grep -qx "sleep 21600" "$d/calls" || fail "defaults: six hours expected"
}

test_missing_complete_backup_skips_wal_pruning() {
  local d
  d="$(new_case incomplete incomplete)"
  run_script "$d" PG_BACKUP_RETENTION_COUNT=1
  grep -q "ERROR" "$d/out" || fail "incomplete: the missing backup must be logged as an error"
  ! grep -q "Retention verified" "$d/out" || fail "incomplete: must not claim retention was verified"
  ! grep -q pg_archivecleanup "$d/calls" || fail "incomplete: WAL must not be pruned without a base backup"
}

test_rejects_bad_retention
test_rejects_bad_interval
test_valid_values_complete_one_cycle
test_defaults_apply_when_unset_or_empty
test_missing_complete_backup_skips_wal_pruning
echo "pg-backup tests passed"
