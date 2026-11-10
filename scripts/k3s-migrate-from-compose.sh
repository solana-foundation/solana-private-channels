#!/usr/bin/env bash
# Streams pg_dump of both compose databases into the CNPG primaries (nothing on disk or
# network-exposed), then compares row counts. Run after k3s.yml -e k3s_stop_after=database,
# with KUBECONFIG from k3s-connect.sh: k3s-migrate-from-compose.sh <compose ssh target> [--dry-run]
set -euo pipefail

target="${1:?usage: k3s-migrate-from-compose.sh <ssh target> [--dry-run]}"
dry="${2:-}"
ns="${K8S_NAMESPACE:-private-channel}"
read -r -a ssh_opts <<<"${COMPOSE_SSH_OPTS:-}"
# The services that write; Postgres stays up for the dump.
writers="write-node read-node gateway auth indexer-solana indexer-private-channel operator-solana operator-private-channel streamer"

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"; }
# Arguments are plain words (names, flags); SQL always travels on stdin.
# shellcheck disable=SC2029
remote() { ssh "${ssh_opts[@]}" "$target" "$@"; }
primary_pod() {
  kubectl -n "$ns" get pods -l "private-channel.io/db=$1,cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}'
}

# Exact row counts for every table, one "schema.table count" line each.
counts_sql="SELECT format('SELECT %L || '' '' || count(*) FROM %I.%I', schemaname || '.' || relname, schemaname, relname)
  FROM pg_stat_user_tables ORDER BY 1"
# SQL goes on stdin: over ssh an argument would be re-split by the remote shell.
counts() {
  printf '%s;\n' "$counts_sql" | "$@" psql -v ON_ERROR_STOP=1 -tA -f - | sed 's/$/;/' | "$@" psql -v ON_ERROR_STOP=1 -tA -f -
}

ledger_pod="$(primary_pod ledger)"
indexer_pod="$(primary_pod indexer)"
[[ -n "$ledger_pod" && -n "$indexer_pod" ]] || { echo "CNPG primaries not found in $ns; run k3s.yml -e k3s_stop_after=database first" >&2; exit 1; }
log "k3s primaries: $ledger_pod $indexer_pod"

if [[ "$dry" == --dry-run ]]; then
  remote docker ps --format '{{.Names}}' | grep -E '^private-channel-(postgres|write|gateway)' || true
  log "dry run: nothing stopped or restored"
  exit 0
fi

start=$(date +%s)
log "stopping compose writers: $writers"
# One call stops them in parallel: gateway and auth ignore SIGTERM and wait out the 60 s.
names=""; for s in $writers; do names+=" private-channel-$s"; done
existing="$(remote docker ps --format '{{.Names}}' | grep -xF -f <(tr ' ' '\n' <<<"$names") | tr '\n' ' ' || true)"
# shellcheck disable=SC2086
[[ -n "$existing" ]] && remote docker stop -t 60 $existing >/dev/null

restore() {
  local name="$1" src_container="$2" src_user="$3" db="$4" pod="$5" err
  err="$(mktemp)"
  log "dump and restore $name ($db)"
  # --clean --if-exists: the grants hook already made the auth schema; the dump's own wins.
  if ! remote docker exec "$src_container" pg_dump -Fc -U "$src_user" -d "$db" \
      | kubectl -n "$ns" exec -i "$pod" -c postgres -- pg_restore --clean --if-exists --no-password \
          --exit-on-error -U postgres -d "$db" 2>"$err"; then
    echo "pg_restore failed for $name:" >&2; cat "$err" >&2; exit 1
  fi
  # Any warning fails the migration too.
  if [[ -s "$err" ]]; then echo "pg_restore reported for $name:" >&2; cat "$err" >&2; exit 1; fi
  rm -f "$err"
}
restore ledger private-channel-postgres-primary private_channel private_channel "$ledger_pod"
restore indexer private-channel-postgres-indexer private_channel_indexer indexer "$indexer_pod"
log "restore done in $(( $(date +%s) - start ))s"

compare() {
  local name="$1" src_container="$2" src_user="$3" db="$4" pod="$5" a b
  a="$(counts remote docker exec -i "$src_container" env PGUSER="$src_user" PGDATABASE="$db" | sort)"
  b="$(counts kubectl -n "$ns" exec -i "$pod" -c postgres -- env PGUSER=postgres PGDATABASE="$db" | sort)"
  if [[ "$a" != "$b" ]]; then
    echo "row counts differ for $name:" >&2
    diff <(echo "$a") <(echo "$b") >&2 || true
    exit 1
  fi
  log "row counts match for $name ($(wc -l <<<"$a") tables)"
}
compare ledger private-channel-postgres-primary private_channel private_channel "$ledger_pod"
compare indexer private-channel-postgres-indexer private_channel_indexer indexer "$indexer_pod"

log "downtime so far $(( $(date +%s) - start ))s. Next: stop compose Postgres, then run k3s.yml in full."
