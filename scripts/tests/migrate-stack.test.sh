#!/usr/bin/env bash
set -euo pipefail

# migrate-stack.sh never puts a secret on a child's argv: each one reaches
# `docker exec` through the environment (bare `-e NAME`), with its value intact.
# Run from the repo root: ./scripts/tests/migrate-stack.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

mkdir -p "$TMP/bin"
# Records every docker argv, plus the value of each bare `-e NAME` it would forward.
cat >"$TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$SHIM_LOG/argv.log"
case "$1" in
  inspect) echo true; exit 0 ;;
  exec)
    shift
    stdin=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -e) if [[ "$2" != *=* ]]; then printf '%s=%s\n' "$2" "${!2-}" >>"$SHIM_LOG/env.log"; fi; shift 2 ;;
        -i) stdin=1; shift ;;
        *) break ;;
      esac
    done
    if [[ $stdin == 1 ]]; then { cat; echo; } >>"$SHIM_LOG/stdin.log"; fi
    exit 0 ;;
esac
exit 0
EOF
chmod +x "$TMP/bin/docker"

i=0
: >"$TMP/sentinels"
{
  for key in POSTGRES_PASSWORD POSTGRES_RUNTIME_PASSWORD POSTGRES_AUTH_RUNTIME_PASSWORD \
    POSTGRES_AUTH_OWNER_PASSWORD POSTGRES_GATEWAY_PASSWORD POSTGRES_MONITORING_PASSWORD \
    POSTGRES_GRAFANA_PASSWORD POSTGRES_INDEXER_PASSWORD GF_ADMIN_PASSWORD; do
    i=$((i + 1))
    echo "$key=SNTLmig${i}x"
    echo "SNTLmig${i}x" >>"$TMP/sentinels"
  done
  echo "POSTGRES_DB=private_channel"
  echo "POSTGRES_USER=private_channel"
  echo "POSTGRES_REPLICATION_USER=replicator"
  echo "POSTGRES_INDEXER_USER=private_channel_indexer"
  echo "POSTGRES_INDEXER_DB=indexer"
} >"$TMP/test.env"

mkdir -p "$TMP/log"
(cd "$REPO" && SHIM_LOG="$TMP/log" PATH="$TMP/bin:$PATH" ./scripts/migrate-stack.sh "$TMP/test.env") \
  >"$TMP/out.log" 2>&1 || { fail "migrate-stack.sh failed"; cat "$TMP/out.log" >&2; }

if grep -F -f "$TMP/sentinels" "$TMP/log/argv.log" "$TMP/out.log" >/dev/null; then
  fail "a secret reached a docker argv or the output: $(grep -ohF -f "$TMP/sentinels" "$TMP/log/argv.log" "$TMP/out.log" | sort -u | tr '\n' ' ')"
fi
# The Grafana admin password goes on stdin (grafana cli --password-from-stdin).
grep -qx 'SNTLmig9x' "$TMP/log/stdin.log" 2>/dev/null || fail "the Grafana password did not reach grafana cli on stdin"
for want in PGPASSWORD=SNTLmig1x POSTGRES_RUNTIME_PASSWORD=SNTLmig2x TARGET_PW=SNTLmig8x \
  POSTGRES_GRAFANA_PASSWORD=SNTLmig7x; do
  grep -qx "$want" "$TMP/log/env.log" 2>/dev/null || fail "docker exec did not receive ${want%%=*} through the environment"
done

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "migrate-stack tests passed"
