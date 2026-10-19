#!/bin/bash
set -e

# Let the pg-backup-indexer sidecar take base backups as the cluster superuser.
echo "host replication $POSTGRES_USER 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"

pg_ctl reload -D "$PGDATA"
