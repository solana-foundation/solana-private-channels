#!/usr/bin/env bash
set -euo pipefail

# A rotated replicator password must reconnect the replica. PG 16 refuses
# pg_terminate_backend on the walreceiver (an auxiliary process, not a backend), so
# tasks/replica_passfile.yml must signal its pid from pg_stat_wal_receiver instead.
# The end-to-end proof is scripts/e2e/deploy-secrets-e2e.sh step 6.
# Run from the repo root: ./scripts/tests/replica-passfile.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
f="$REPO/private-channel-deploy/tasks/replica_passfile.yml"
failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }
grep -q 'pg_terminate_backend' "$f" && fail "replica_passfile.yml still uses pg_terminate_backend on the walreceiver"
grep -qE 'pg_stat_wal_receiver' "$f" || fail "replica_passfile.yml does not read the walreceiver pid"
grep -qE 'kill -TERM' "$f" || fail "replica_passfile.yml does not SIGTERM the walreceiver"
if command -v ansible-playbook >/dev/null; then
  ansible-playbook -i localhost, --syntax-check /dev/stdin >/dev/null 2>&1 <<EOF2 || fail "replica_passfile.yml does not parse"
- hosts: localhost
  gather_facts: false
  tasks:
  - ansible.builtin.import_tasks: $f
EOF2
fi
# Behaviour, with a stub docker: psql's failure fails the task; a walreceiver pid gets SIGTERM.
run_task() {
  local dir; dir="$(mktemp -d)"
  cat >"$dir/docker" <<'D'
#!/bin/bash
echo "$*" >>"$STUB_LOG"
case "$*" in
  *pg_isready*) exit 0 ;;
  *cmp*) exit 1 ;;
  *psql*) cat >/dev/null; [[ "$STUB_PSQL_RC" == 0 ]] && echo "PC_WALRECEIVER_PID 4242"; exit "$STUB_PSQL_RC" ;;
  *"sh -c"*) cat >/dev/null ;;
esac
exit 0
D
  chmod +x "$dir/docker"
  STUB_LOG="$dir/log" STUB_PSQL_RC="$1" PATH="$dir:$PATH" ansible-playbook -i localhost, -c local /dev/stdin >"$dir/out" 2>&1 <<EOF3
- hosts: localhost
  gather_facts: false
  vars: { postgres_user: u, postgres_replication_user: r, postgres_replication_password: pw }
  tasks:
  - ansible.builtin.import_tasks: $f
EOF3
  local rc=$?
  RUN_LOG="$(cat "$dir/log")"
  rm -rf "$dir"
  return $rc
}
if command -v ansible-playbook >/dev/null; then
  run_task 3 && fail "a failing psql did not fail the task"
  run_task 0 || fail "the task failed with a healthy psql"
  grep -q 'kill -TERM 4242' <<<"$RUN_LOG" || fail "the walreceiver pid was not sent SIGTERM"
fi
[[ $failures -gt 0 ]] && { echo "$failures failure(s)" >&2; exit 1; }
echo "replica passfile tests passed"
