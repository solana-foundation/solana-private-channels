-- Database logins, one per trust level. Runs after init-primary.sql on a fresh
-- cluster; safe to re-run by hand against a cluster that predates it.
--
--   private_channel_runtime       write-node, read-node, streamer. Owns public,
--                                 has nothing in private_channel_auth.
--   private_channel_auth_runtime  auth service. Its grants on the auth tables
--                                 come from the auth service's schema init.
--   private_channel_gateway       gateway. Reads only: the same schema init
--                                 grants it SELECT on two auth tables, and core's
--                                 accountsdb init the two ledger tables it reads.
--   private_channel_auth_owner    owns private_channel_auth. Held only by the
--                                 migration one-shot and the admin CLI, never by
--                                 a long-lived service.
--   private_channel_monitoring    postgres_exporter. pg_monitor and nothing else.
--
-- POSTGRES_USER stays the bootstrap superuser, for the init scripts, the backup
-- sidecars and the replica's startup wait. A service that serves requests
-- connecting as it would bypass every grant below.
--
-- Passwords arrive as env vars the Postgres entrypoint exports. They are staged
-- through set_config because psql substitutes :'vars' only outside quoted
-- strings, and the role statements below are built inside dollar-quoted blocks.
\set runtime_password `echo "$POSTGRES_RUNTIME_PASSWORD"`
\set auth_runtime_password `echo "$POSTGRES_AUTH_RUNTIME_PASSWORD"`
\set auth_owner_password `echo "$POSTGRES_AUTH_OWNER_PASSWORD"`
\set gateway_password `echo "$POSTGRES_GATEWAY_PASSWORD"`
\set replication_user `echo "$POSTGRES_REPLICATION_USER"`
\set monitoring_password `echo "$POSTGRES_MONITORING_PASSWORD"`
\set app_db `echo "$POSTGRES_DB"`

-- set_config returns what it was given, and the entrypoint runs psql without
-- -q, so without this the passwords land in the postgres log.
\o /dev/null
SELECT set_config('init.runtime_password', :'runtime_password', false);
SELECT set_config('init.auth_runtime_password', :'auth_runtime_password', false);
SELECT set_config('init.auth_owner_password', :'auth_owner_password', false);
SELECT set_config('init.gateway_password', :'gateway_password', false);
SELECT set_config('init.monitoring_password', :'monitoring_password', false);
\o

-- Create each role, or reset its password if a re-run finds it already there.
DO $$
DECLARE
    role_spec record;
    role_password text;
BEGIN
    FOR role_spec IN
        SELECT *
        FROM (VALUES
            ('private_channel_runtime', 'init.runtime_password'),
            ('private_channel_auth_runtime', 'init.auth_runtime_password'),
            ('private_channel_auth_owner', 'init.auth_owner_password'),
            ('private_channel_gateway', 'init.gateway_password'),
            ('private_channel_monitoring', 'init.monitoring_password')
        ) AS specs(role_name, password_setting)
    LOOP
        role_password := current_setting(role_spec.password_setting, true);
        IF coalesce(role_password, '') = '' THEN
            RAISE EXCEPTION 'no password supplied for %', role_spec.role_name;
        END IF;

        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_spec.role_name) THEN
            EXECUTE format(
                'ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
                role_spec.role_name, role_password
            );
        ELSE
            EXECUTE format(
                'CREATE ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
                role_spec.role_name, role_password
            );
        END IF;
    END LOOP;
END $$;

GRANT CONNECT ON DATABASE :"app_db" TO private_channel_runtime, private_channel_auth_runtime, private_channel_auth_owner, private_channel_gateway, private_channel_monitoring;

-- Covers every default postgres_exporter collector: pg_stat_*, pg_locks,
-- pg_settings. No table privileges, and not a superuser, so the audit trigger
-- binds it like any other login.
GRANT pg_monitor TO private_channel_monitoring;

-- Channel runtime owns the ledger schema: the node creates its own tables on
-- first start, and on an older cluster they still belong to the bootstrap user.
-- Ownership has to land here for core's accountsdb init to grant the gateway
-- its read of token_account_owner_change and metadata.
GRANT CREATE, USAGE ON SCHEMA public TO private_channel_runtime;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO private_channel_runtime;

DO $$
DECLARE
    ledger_table text;
BEGIN
    FOR ledger_table IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    LOOP
        EXECUTE format('ALTER TABLE public.%I OWNER TO private_channel_runtime', ledger_table);
    END LOOP;
END $$;

-- Physical replication ignores table grants, so this is for a later move to
-- logical. Keyed to the runtime because it now creates every table here.
ALTER DEFAULT PRIVILEGES FOR ROLE private_channel_runtime IN SCHEMA public
    GRANT SELECT ON TABLES TO :"replication_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"replication_user";

-- The two ledger tables the gateway reads. core's accountsdb init grants these
-- when it creates them; this covers a cluster where they already exist, which
-- that init would not revisit until the node restarts.
DO $$
BEGIN
    IF to_regclass('public.token_account_owner_change') IS NOT NULL THEN
        GRANT SELECT ON public.token_account_owner_change TO private_channel_gateway;
    END IF;

    IF to_regclass('public.metadata') IS NOT NULL THEN
        GRANT SELECT ON public.metadata TO private_channel_gateway;
    END IF;
END $$;

-- Auth schema. CREATE on the database lets the owner run the service's schema
-- init, which opens with CREATE SCHEMA IF NOT EXISTS.
GRANT CREATE ON DATABASE :"app_db" TO private_channel_auth_owner;
CREATE SCHEMA IF NOT EXISTS private_channel_auth AUTHORIZATION private_channel_auth_owner;
ALTER SCHEMA private_channel_auth OWNER TO private_channel_auth_owner;
REVOKE ALL ON SCHEMA private_channel_auth FROM PUBLIC;
-- Who may do what inside this schema is the auth service's schema init to say,
-- alongside the tables it grants on. This file stops at the schema boundary.

-- On a cluster where the auth service already ran, its tables belong to the
-- bootstrap user. The owner role has to hold them to grant on them.
DO $$
DECLARE
    auth_table text;
BEGIN
    FOR auth_table IN SELECT tablename FROM pg_tables WHERE schemaname = 'private_channel_auth'
    LOOP
        EXECUTE format(
            'ALTER TABLE private_channel_auth.%I OWNER TO private_channel_auth_owner',
            auth_table
        );
    END LOOP;

    IF EXISTS (
        SELECT 1 FROM pg_type
        JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
        WHERE pg_namespace.nspname = 'private_channel_auth' AND pg_type.typname = 'user_role'
    ) THEN
        ALTER TYPE private_channel_auth.user_role OWNER TO private_channel_auth_owner;
    END IF;
END $$;
