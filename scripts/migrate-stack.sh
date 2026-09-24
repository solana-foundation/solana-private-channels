#!/usr/bin/env bash
set -euo pipefail

# migrate-stack.sh — apply the steps a stack upgraded over existing volumes needs.
# Usage: migrate-stack.sh [--scope=all|databases|grafana] <env-file> [<env-file> ...]
#
# Three things only ever happen on a first boot:
#   * the Postgres entrypoint runs /docker-entrypoint-initdb.d, so a cluster with
#     an existing data directory never gets the service roles;
#   * it also creates POSTGRES_USER only at initdb, so pointing the indexer at a
#     new login does not bring that login into being on an existing volume;
#   * Grafana applies GF_SECURITY_ADMIN_PASSWORD when it creates the admin user,
#     so an existing grafana volume keeps whatever password it was built with.
#
# Every step is idempotent — on a fresh stack this is a no-op. It is the
# Make/Compose equivalent of what the Ansible deploy does on every run.
#
# --scope exists because `make docker-up` has to create the database roles before
# the services that log in with them start, and can only touch Grafana once it is
# running. A container that a scope needs but that is not running is an error,
# never a silent skip.

SCOPE=all
case "${1:-}" in
  --scope=*) SCOPE="${1#--scope=}"; shift ;;
esac
case "$SCOPE" in
  all|databases|grafana) ;;
  *) echo "FATAL: unknown scope '${SCOPE}' (all|databases|grafana)" >&2; exit 2 ;;
esac

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--scope=all|databases|grafana] <env-file> [<env-file> ...]" >&2
  exit 2
fi

for env_file in "$@"; do
  if [[ ! -f "$env_file" ]]; then
    echo "FATAL: env file not found: ${env_file}" >&2
    exit 1
  fi
done

# Values are resolved exactly as check-required-env.sh and compose resolve them.
# Sourcing the files instead would overwrite an exported secret with the blank
# that ships in the tracked preset.
# shellcheck source=scripts/env-resolve.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-resolve.sh"

for key in POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD POSTGRES_REPLICATION_USER \
           POSTGRES_RUNTIME_PASSWORD POSTGRES_AUTH_RUNTIME_PASSWORD \
           POSTGRES_AUTH_OWNER_PASSWORD POSTGRES_GATEWAY_PASSWORD \
           POSTGRES_MONITORING_PASSWORD POSTGRES_GRAFANA_PASSWORD \
           POSTGRES_INDEXER_USER POSTGRES_INDEXER_PASSWORD POSTGRES_INDEXER_DB \
           GF_ADMIN_PASSWORD; do
  declare "V_${key}=$(resolve_var "$key" "$@")"
done

require_running() {
  local container="$1"
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo false)" != "true" ]]; then
    echo "FATAL: ${container} is not running; bring the stack up and re-run." >&2
    exit 1
  fi
}

if [[ "$SCOPE" == "all" || "$SCOPE" == "databases" ]]; then
  require_running private-channel-postgres-primary
  require_running private-channel-postgres-indexer

  echo "Applying init-auth-roles.sql to postgres-primary..."
  docker exec -i \
    -e PGPASSWORD="${V_POSTGRES_PASSWORD}" \
    -e POSTGRES_REPLICATION_USER="${V_POSTGRES_REPLICATION_USER}" \
    -e POSTGRES_RUNTIME_PASSWORD="${V_POSTGRES_RUNTIME_PASSWORD}" \
    -e POSTGRES_AUTH_RUNTIME_PASSWORD="${V_POSTGRES_AUTH_RUNTIME_PASSWORD}" \
    -e POSTGRES_AUTH_OWNER_PASSWORD="${V_POSTGRES_AUTH_OWNER_PASSWORD}" \
    -e POSTGRES_GATEWAY_PASSWORD="${V_POSTGRES_GATEWAY_PASSWORD}" \
    -e POSTGRES_MONITORING_PASSWORD="${V_POSTGRES_MONITORING_PASSWORD}" \
    -e POSTGRES_DB="${V_POSTGRES_DB}" \
    private-channel-postgres-primary \
    psql -v ON_ERROR_STOP=1 -q -U "${V_POSTGRES_USER}" -d "${V_POSTGRES_DB}" -f - < init-auth-roles.sql

  # The indexer cluster's superuser is created at initdb and nowhere else, so on
  # a volume that predates the split it is still POSTGRES_USER. Find whichever
  # login works before assuming the new one is there.
  #
  # Over the container's local socket, which pg_hba trusts. That is what lets a
  # rotated POSTGRES_INDEXER_PASSWORD be applied at all: the role's stored
  # password is the old one by definition, so any password-authenticated probe
  # would reject exactly the case this has to fix.
  indexer_login=""
  for candidate in "${V_POSTGRES_INDEXER_USER}" "${V_POSTGRES_USER}"; do
    case "$candidate" in
      "${V_POSTGRES_INDEXER_USER}") candidate_pw="${V_POSTGRES_INDEXER_PASSWORD}" ;;
      *) candidate_pw="${V_POSTGRES_PASSWORD}" ;;
    esac
    if docker exec -e PGPASSWORD="$candidate_pw" private-channel-postgres-indexer \
         psql -U "$candidate" -d "${V_POSTGRES_INDEXER_DB}" -tAc 'SELECT 1' >/dev/null 2>&1; then
      indexer_login="$candidate"
      indexer_pw="$candidate_pw"
      break
    fi
  done

  if [[ -z "$indexer_login" ]]; then
    echo "FATAL: no working login for postgres-indexer. Tried ${V_POSTGRES_INDEXER_USER} and ${V_POSTGRES_USER}." >&2
    echo "Check POSTGRES_INDEXER_PASSWORD and POSTGRES_PASSWORD against the cluster's data directory." >&2
    exit 1
  fi

  if [[ "$indexer_login" != "${V_POSTGRES_INDEXER_USER}" ]]; then
    echo "Creating ${V_POSTGRES_INDEXER_USER} on postgres-indexer (connected as ${indexer_login})..."
  fi

  # Same SQL the Ansible deploy runs, so both upgrade paths behave identically.
  docker exec -i \
    -e PGPASSWORD="$indexer_pw" \
    private-channel-postgres-indexer \
    psql -v ON_ERROR_STOP=1 -q -U "$indexer_login" -d "${V_POSTGRES_INDEXER_DB}" \
      -v target_user="${V_POSTGRES_INDEXER_USER}" \
      -v target_pw="${V_POSTGRES_INDEXER_PASSWORD}" \
      -v prior_user="${V_POSTGRES_USER}" -f - < init-indexer-login.sql

  echo "Applying init-indexer-roles.sql to postgres-indexer..."
  docker exec -i \
    -e PGPASSWORD="${V_POSTGRES_INDEXER_PASSWORD}" \
    -e POSTGRES_GRAFANA_PASSWORD="${V_POSTGRES_GRAFANA_PASSWORD}" \
    -e POSTGRES_DB="${V_POSTGRES_INDEXER_DB}" \
    private-channel-postgres-indexer \
    psql -v ON_ERROR_STOP=1 -q -U "${V_POSTGRES_INDEXER_USER}" -d "${V_POSTGRES_INDEXER_DB}" -f - < init-indexer-roles.sql
fi

if [[ "$SCOPE" == "all" || "$SCOPE" == "grafana" ]]; then
  require_running private-channel-grafana

  # grafana-cli edits the database on disk, which does not exist until Grafana
  # has finished its own first-run setup.
  for _ in $(seq 1 60); do
    docker exec private-channel-grafana test -f /var/lib/grafana/grafana.db >/dev/null 2>&1 && break
    sleep 1
  done
  if ! docker exec private-channel-grafana test -f /var/lib/grafana/grafana.db >/dev/null 2>&1; then
    echo "FATAL: Grafana has not created its database yet; re-run once it is ready." >&2
    exit 1
  fi

  echo "Resetting the stored Grafana admin password..."
  docker exec private-channel-grafana \
    grafana-cli --homepath /usr/share/grafana admin reset-admin-password "${V_GF_ADMIN_PASSWORD}" >/dev/null
fi

echo "Migration complete (scope: ${SCOPE})."
