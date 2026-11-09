-- Create replication user
-- Password comes from the POSTGRES_REPLICATION_PASSWORD_FILE secret file when set
-- (the Ansible deploy), else from POSTGRES_REPLICATION_PASSWORD (raw compose).
\set replication_user `echo "$POSTGRES_REPLICATION_USER"`
\set replication_password `if [ -n "$POSTGRES_REPLICATION_PASSWORD_FILE" ]; then cat "$POSTGRES_REPLICATION_PASSWORD_FILE"; else printf %s "$POSTGRES_REPLICATION_PASSWORD"; fi`
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