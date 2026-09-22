-- Create replication user
-- Password is provided via the POSTGRES_REPLICATION_PASSWORD env var rendered
-- through psql variable substitution. The Postgres entrypoint exports all
-- POSTGRES_* env vars; we forward it via PSQL_OPTIONS / -v to make it visible
-- here as :'replication_password'.
\set replication_user `echo "$POSTGRES_REPLICATION_USER"`
\set replication_password `echo "$POSTGRES_REPLICATION_PASSWORD"`
CREATE USER :"replication_user" WITH REPLICATION ENCRYPTED PASSWORD :'replication_password';

-- Grant necessary permissions for replication. The default privileges that keep
-- this true for tables created later live in init-auth-roles.sql, keyed to the
-- role that creates them; setting them here would only cover this one.
GRANT USAGE ON SCHEMA public TO :"replication_user";

-- Create replication slot for the replica
SELECT pg_create_physical_replication_slot('replica_slot', true);

-- The service logins and their privileges live in init-auth-roles.sql, which the
-- entrypoint runs next. POSTGRES_USER is the bootstrap superuser: granting to it
-- would be a no-op, and no service connects as it.
--
-- Note: Tables will be created by the application on first startup via postgres.rs