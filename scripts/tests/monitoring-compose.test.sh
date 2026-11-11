#!/usr/bin/env bash
set -euo pipefail

# The monitoring compose file keeps every DB password out of the postgres exporters'
# env, args and DSN: each exporter gets a password-free DATA_SOURCE_NAME and builds a
# 0600 libpq passfile from its secret file at start, because the exporter logs its
# DSN on a refused connection. Needs ansible-playbook; skips without it, except in CI.
# Run from the repo root: ./scripts/tests/monitoring-compose.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if ! command -v ansible-playbook >/dev/null; then
  echo "skip: monitoring compose test (ansible-playbook missing)"
  [[ "${CI:-}" == true ]] && exit 1
  exit 0
fi
# shellcheck source=scripts/tests/deploy-test-lib.sh
. "$REPO/scripts/tests/deploy-test-lib.sh"
deploy_test_setup

sentinel_secrets "$D/secrets.yml"
render_template "$REPO/monitoring/monitoring.compose.yml.j2" "$TMP/mon.yml" -e "repo_root=$REPO" \
  -e "_secrets_rev={}" -e monitoring_dir=/m
assert_no_sentinel "rendered monitoring compose" "$TMP/mon.yml"
python3 - "$TMP/mon.yml" <<'PY' || fail "exporter contract"
import sys, yaml
svcs = yaml.safe_load(open(sys.argv[1]))["services"]
bad = []
for name in ("postgres-exporter-primary", "postgres-exporter-indexer"):
    s = svcs[name]
    env = dict(e.split("=", 1) for e in s.get("environment", []))
    if any(k.startswith("DATA_SOURCE_PASS") or k == "DATA_SOURCE_URI" for k in env):
        bad.append(f"{name}: password env {sorted(env)}")
    dsn = env.get("DATA_SOURCE_NAME", "")
    userinfo = dsn.split("://", 1)[-1].split("@", 1)[0] if "@" in dsn else ""
    if not dsn or ":" in userinfo:
        bad.append(f"{name}: DATA_SOURCE_NAME missing or carries a password")
    if env.get("PGPASSFILE") != "/tmp/pgpass":
        bad.append(f"{name}: no PGPASSFILE")
    cmd = " ".join(s.get("entrypoint", []) + s.get("command", []))
    if "/run/secrets/DATA_SOURCE_PASS" not in cmd or "umask 077" not in cmd or "exec /bin/postgres_exporter" not in cmd:
        bad.append(f"{name}: entrypoint does not build the passfile")
for b in bad:
    print("FAIL:", b)
sys.exit(1 if bad else 0)
PY
# As compose reads it: an unescaped $( in the wrapper would fail interpolation here.
if docker compose version >/dev/null 2>&1; then
  docker compose -f "$TMP/mon.yml" config --format json >"$TMP/mon.json" 2>"$TMP/mon.err" || fail "docker compose config: $(tail -1 "$TMP/mon.err")"
  jq -r '.services["postgres-exporter-primary"].entrypoint[2]' "$TMP/mon.json" | grep -q 'exec /bin/postgres_exporter' \
    || fail "compose dropped the exporter wrapper"
fi
[[ $failures -gt 0 ]] && { echo "$failures failure(s)" >&2; exit 1; }
echo "monitoring compose tests passed"
