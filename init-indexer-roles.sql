-- Grafana's login on the indexer cluster. Runs from the Postgres entrypoint on
-- a fresh cluster; safe to re-run by hand against one that predates it.
--
--   private_channel_grafana  the Grafana datasource. SELECT on the monitoring
--                            view the indexer creates, and nothing else.
--
-- POSTGRES_USER stays the bootstrap superuser, for the init scripts and the
-- backup sidecar. Grafana connecting as it would put every column of
-- `transactions` — signature, initiator, recipient, mint, amount, memo,
-- withdrawal_nonce — behind a single HTTP login.
--
-- Runs as superuser, so unqualified calls resolve to the built-ins only, never
-- to a function some other login created in public.
SET search_path = pg_catalog, pg_temp;

\set grafana_password `echo "$POSTGRES_GRAFANA_PASSWORD"`
\set app_db `echo "$POSTGRES_DB"`

-- set_config returns what it was given, and the entrypoint runs psql without
-- -q, so without this the password lands in the postgres log.
\o /dev/null
SELECT set_config('init.grafana_password', :'grafana_password', false);
\o

DO $$
DECLARE
    grafana_password text := current_setting('init.grafana_password', true);
BEGIN
    IF coalesce(grafana_password, '') = '' THEN
        RAISE EXCEPTION 'no password supplied for private_channel_grafana';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'private_channel_grafana') THEN
        EXECUTE format(
            'ALTER ROLE private_channel_grafana WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
            grafana_password
        );
    ELSE
        EXECUTE format(
            'CREATE ROLE private_channel_grafana WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
            grafana_password
        );
    END IF;
END $$;

GRANT CONNECT ON DATABASE :"app_db" TO private_channel_grafana;

-- The view is the indexer's to create, and it grants on it there. This covers a
-- cluster where it already exists, which that init would not revisit until the
-- indexer restarts.
DO $$
BEGIN
    IF to_regclass('public.transaction_monitoring') IS NOT NULL THEN
        GRANT SELECT ON public.transaction_monitoring TO private_channel_grafana;
    END IF;
END $$;
