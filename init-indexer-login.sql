-- Bring the indexer cluster's login into being on a data directory that
-- predates it. Run with psql -v target_user=… -v target_pw=… -v prior_user=…,
-- connected as whichever login currently works.
--
-- Not an entrypoint script: the entrypoint only ever runs on an empty data
-- directory, which is exactly the case this file does not have to handle. It is
-- driven by `make docker-migrate` and by the Ansible deploy.
--
-- Runs as superuser, so unqualified calls resolve to the built-ins only, never
-- to a function some other login created in public.
SET search_path = pg_catalog, pg_temp;

\o /dev/null
SELECT set_config('init.target_user', :'target_user', false);
SELECT set_config('init.target_pw', :'target_pw', false);
SELECT set_config('init.prior_user', :'prior_user', false);
\o

DO $$
DECLARE
    target_user text := current_setting('init.target_user');
    target_pw   text := current_setting('init.target_pw');
    prior_user  text := current_setting('init.prior_user');
BEGIN
    IF coalesce(target_pw, '') = '' THEN
        RAISE EXCEPTION 'no password supplied for %', target_user;
    END IF;

    -- SUPERUSER for parity with a fresh cluster, where this role is the one
    -- initdb created. It also means no ownership transfer is needed to
    -- administer the tables the prior login created.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_user) THEN
        EXECUTE format('ALTER ROLE %I WITH LOGIN SUPERUSER PASSWORD %L', target_user, target_pw);
    ELSE
        EXECUTE format('CREATE ROLE %I WITH LOGIN SUPERUSER PASSWORD %L', target_user, target_pw);
    END IF;

    -- The prior login here is a copy of the primary's superuser credential.
    -- Nothing connects as it once the services are switched, so take its LOGIN
    -- away rather than leave that copy usable. It keeps whatever it owns;
    -- ownership does not need LOGIN.
    IF prior_user <> target_user
       AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = prior_user AND rolcanlogin) THEN
        EXECUTE format('ALTER ROLE %I NOLOGIN', prior_user);
        RAISE NOTICE 'revoked LOGIN from % on this cluster', prior_user;
    END IF;
END $$;
