#!/usr/bin/env bash
set -euo pipefail

# The init SQL reads each password from X_FILE when set, else from X, so the same
# scripts work under the Ansible deploy (secret files) and raw compose (env).
# Runs every backtick snippet under busybox sh (the postgres-alpine shell), or dash.
# Run from the repo root: ./scripts/tests/init-sql.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

if command -v busybox >/dev/null; then SH=(busybox sh); else SH=(dash); fi

# file:var pairs every secret password must be read through.
PAIRS=(
  init-primary.sql:POSTGRES_REPLICATION_PASSWORD
  init-auth-roles.sql:POSTGRES_RUNTIME_PASSWORD
  init-auth-roles.sql:POSTGRES_AUTH_RUNTIME_PASSWORD
  init-auth-roles.sql:POSTGRES_AUTH_OWNER_PASSWORD
  init-auth-roles.sql:POSTGRES_GATEWAY_PASSWORD
  init-auth-roles.sql:POSTGRES_MONITORING_PASSWORD
  init-indexer-roles.sql:POSTGRES_GRAFANA_PASSWORD
)

printf 'fromfile\n' >"$TMP/pw"
for pair in "${PAIRS[@]}"; do
  file="${pair%%:*}" var="${pair#*:}"
  snippet="$(grep -E "^\\\\set [a-z_]+ \`.*\\\$${var}[\"_]" "$REPO/$file" | sed -E 's/^[^`]*`(.*)`$/\1/' || true)"
  if [[ -z "$snippet" ]]; then
    fail "$file: no backtick snippet reads $var"
    continue
  fi
  got="$(env -i "${var}_FILE=$TMP/pw" "$var=fromenv" "${SH[@]}" -c "$snippet")"
  [[ "$got" == fromfile ]] || fail "$file $var: X_FILE must win (got '$got')"
  got="$(env -i "$var=fromenv" "${SH[@]}" -c "$snippet")"
  [[ "$got" == fromenv ]] || fail "$file $var: X alone must be used (got '$got')"
  got="$(env -i "${SH[@]}" -c "$snippet")"
  [[ -z "$got" ]] || fail "$file $var: neither set must be empty (got '$got')"
done

# An empty value must still be refused by the SQL itself.
grep -q "RAISE EXCEPTION 'no password supplied for %'" "$REPO/init-auth-roles.sql" \
  || fail "init-auth-roles.sql lost its blank-password refusal"
grep -q "RAISE EXCEPTION 'no password supplied for private_channel_grafana'" "$REPO/init-indexer-roles.sql" \
  || fail "init-indexer-roles.sql lost its blank-password refusal"

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "init-sql tests passed"
