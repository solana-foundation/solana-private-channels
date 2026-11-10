-- Grants and schema for the ledger on CloudNativePG. The roles and their passwords
-- are CNPG managed roles; this is the grant half of init-auth-roles.sql, unchanged,
-- plus the replication user's schema usage from init-primary.sql.
SET search_path = pg_catalog, pg_temp;

GRANT USAGE ON SCHEMA public TO :"replication_user";

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
-- alongside the tables it grants on. The gateway's read is the one exception,
-- reasserted at the end of this file.

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

-- The gateway's two reads. `auth-admin migrate` grants these, but it runs in the
-- auth profile and the gateway is deployed without it, so on an upgrade nothing
-- else would. Reassert what exists, skip what doesn't.
DO $$
BEGIN
    GRANT USAGE ON SCHEMA private_channel_auth TO private_channel_gateway;

    IF to_regclass('private_channel_auth.users') IS NOT NULL THEN
        GRANT SELECT ON private_channel_auth.users TO private_channel_gateway;
    END IF;

    IF to_regclass('private_channel_auth.verified_wallets') IS NOT NULL THEN
        GRANT SELECT ON private_channel_auth.verified_wallets TO private_channel_gateway;
    END IF;
END $$;
