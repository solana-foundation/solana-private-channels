-- Grants for the indexer database on CloudNativePG. The Grafana login and its
-- password are a CNPG managed role; this is the grant half of init-indexer-roles.sql.
SET search_path = pg_catalog, pg_temp;

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
