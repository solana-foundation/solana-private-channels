#!/bin/sh
set -eu

# pg-backup.sh — periodic pg_basebackup with retention pruning + WAL cleanup
# Used by pg-backup-primary and pg-backup-indexer sidecar containers.
#
# Required env vars:
#   PGHOST          — Postgres host to back up
#   PGUSER          — Postgres user (needs replication or superuser)
#   PGPASSWORD      — Postgres password
# Optional:
#   PG_BACKUP_INTERVAL_HOURS  — hours between backups (default: 6)
#   PG_BACKUP_RETENTION_COUNT — number of backups to keep (default: 3)
#   BACKUP_DIR                — base directory for backups (default: /backups)
#   WAL_ARCHIVE_DIR           — WAL archive directory to prune (default: /wal_archive)

# Validate required env vars
for var in PGHOST PGUSER PGPASSWORD; do
  eval val="\${${var}:-}"
  if [ -z "${val}" ]; then
    echo "FATAL: Required environment variable ${var} is not set or is empty" >&2
    exit 1
  fi
done

INTERVAL_HOURS="${PG_BACKUP_INTERVAL_HOURS:-6}"
RETENTION="${PG_BACKUP_RETENTION_COUNT:-3}"

# Canonical positive integer, at most 4 digits so the seconds arithmetic cannot overflow.
# A zero retention would prune the backup just taken, and a zero interval would loop
# full backups back to back. A leading zero is refused because sh reads "08" as invalid octal.
require_positive_int() {
  case "$2" in
    ''|*[!0-9]*|0*)
      echo "FATAL: $1 must be a positive integer without a sign or leading zero (got '$2')" >&2
      exit 1
      ;;
  esac
  if [ "${#2}" -gt 4 ]; then
    echo "FATAL: $1 must be at most 4 digits (got '$2')" >&2
    exit 1
  fi
}
require_positive_int PG_BACKUP_INTERVAL_HOURS "${INTERVAL_HOURS}"
require_positive_int PG_BACKUP_RETENTION_COUNT "${RETENTION}"

BACKUP_DIR="${BACKUP_DIR:-/backups}"
WAL_ARCHIVE_DIR="${WAL_ARCHIVE_DIR:-/wal_archive}"
MAX_ATTEMPTS=3
RETRY_BACKOFF=30

mkdir -p "${BACKUP_DIR}"

take_backup() {
  local ts dest attempt

  ts="$(date +%Y%m%d_%H%M%S)"
  dest="${BACKUP_DIR}/base_${ts}"
  attempt=1

  while [ "${attempt}" -le "${MAX_ATTEMPTS}" ]; do
    echo "[$(date -Iseconds)] Starting base backup to ${dest} (attempt ${attempt}/${MAX_ATTEMPTS}) ..."
    if pg_basebackup \
        -h "${PGHOST}" \
        -U "${PGUSER}" \
        -D "${dest}" \
        -Ft -z -X stream -P; then
      echo "[$(date -Iseconds)] Backup complete: ${dest}"
      return 0
    fi
    echo "[$(date -Iseconds)] ERROR: pg_basebackup failed (attempt ${attempt}/${MAX_ATTEMPTS})" >&2
    rm -rf "${dest}"
    attempt=$((attempt + 1))
    if [ "${attempt}" -le "${MAX_ATTEMPTS}" ]; then
      sleep "${RETRY_BACKOFF}"
    fi
  done

  echo "[$(date -Iseconds)] FATAL: base backup failed after ${MAX_ATTEMPTS} attempts" >&2
  return 1
}

prune_old_backups() {
  local count to_remove

  count=0
  for d in "${BACKUP_DIR}"/base_*; do
    [ -d "$d" ] && count=$((count + 1))
  done

  if [ "${count}" -le "${RETENTION}" ]; then
    return
  fi

  to_remove=$((count - RETENTION))
  # Lexicographic sort on YYYYMMDD_HHMMSS = chronological order
  for dir in $(ls -1d "${BACKUP_DIR}"/base_* | sort | head -n "${to_remove}"); do
    echo "[$(date -Iseconds)] Pruning old backup: ${dir}"
    rm -rf "${dir}" || echo "[$(date -Iseconds)] WARNING: Failed to remove ${dir}" >&2
  done
}

# True when at least one complete base backup is left. WAL older than the oldest kept
# backup is deleted, so pruning it with no backup left would destroy the only recovery path.
backup_remains() {
  for d in "${BACKUP_DIR}"/base_*; do
    [ -f "${d}/base.tar.gz" ] && return 0
  done
  return 1
}

prune_old_wal() {
  if [ ! -d "${WAL_ARCHIVE_DIR}" ]; then
    return
  fi

  # Find the oldest retained base backup's start WAL segment
  local oldest_backup wal_file
  oldest_backup="$(ls -1d "${BACKUP_DIR}"/base_* 2>/dev/null | sort | head -n 1)"
  if [ -z "${oldest_backup}" ]; then
    return
  fi

  # backup_label is inside base.tar.gz when using -Ft format
  if [ -f "${oldest_backup}/base.tar.gz" ]; then
    wal_file="$(tar -xzOf "${oldest_backup}/base.tar.gz" backup_label 2>/dev/null \
      | grep 'START WAL LOCATION' \
      | sed 's/.*file \([^ )]*\).*/\1/')"
    if [ -n "${wal_file}" ]; then
      echo "[$(date -Iseconds)] Pruning WAL segments older than ${wal_file} ..."
      pg_archivecleanup "${WAL_ARCHIVE_DIR}" "${wal_file}" || \
        echo "[$(date -Iseconds)] WARNING: pg_archivecleanup failed" >&2
    fi
  fi
}

# Wait for Postgres to become ready
echo "[$(date -Iseconds)] Waiting for ${PGHOST} to become ready ..."
until pg_isready -h "${PGHOST}" -U "${PGUSER}" -q 2>/dev/null; do
  sleep 5
done
echo "[$(date -Iseconds)] ${PGHOST} is ready"

echo "pg-backup: host=${PGHOST} interval=${INTERVAL_HOURS}h retention=${RETENTION} dir=${BACKUP_DIR} wal_archive=${WAL_ARCHIVE_DIR}"

while true; do
  if take_backup; then
    prune_old_backups
    # Logged, not fatal: exiting would make the container restart and take another full backup.
    if backup_remains; then
      echo "[$(date -Iseconds)] Retention verified: ${BACKUP_DIR} holds a complete base backup"
      prune_old_wal
    else
      echo "[$(date -Iseconds)] ERROR: no complete base backup remains after pruning, skipping WAL pruning" >&2
    fi
  else
    echo "[$(date -Iseconds)] Skipping pruning due to backup failure"
  fi
  echo "[$(date -Iseconds)] Next backup in ${INTERVAL_HOURS}h"
  sleep "$((INTERVAL_HOURS * 3600))"
done
