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
[[ $failures -gt 0 ]] && { echo "$failures failure(s)" >&2; exit 1; }
echo "replica passfile tests passed"
