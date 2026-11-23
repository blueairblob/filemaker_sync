-- supabase_stubs.sql  (TEST-ONLY -- never apply to a real target)
--
-- Minimal stand-ins for the Supabase-provided roles and objects that
-- supabase/schema/bootstrap_rat_schema.sql and add_data_fix_tables.sql
-- reference, so both can run on a plain postgres:<major> Docker container.
-- Discovered by running the bootstrap on a bare postgres:17: the only missing
-- object it hits is the `anon` role; add_data_fix_tables.sql additionally needs
-- `authenticated`, the `auth` schema, auth.users and auth.uid(). It also
-- reproduces live oci's default ACL on schema rat (see below).
--
-- auth.uid() mirrors Supabase's own definition closely enough for tests: it
-- reads the JWT `sub` claim PostgREST puts in request.jwt.claim.sub.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;
    -- Not referenced by either DDL file today; created so the test cluster has
    -- the same role set PostgREST/Supabase does (service_role bypasses RLS).
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role NOLOGIN BYPASSRLS;
    END IF;
    -- Exists on live oci (picaloco_agent's DB role) and is in rat's default ACL.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'picaloco_agent') THEN
        CREATE ROLE picaloco_agent NOLOGIN;
    END IF;
END $$;

-- Reproduce live oci's default ACL on schema rat (pg_default_acl, role postgres:
-- {anon=r/postgres,authenticated=r/postgres,picaloco_agent=arw/postgres}), so
-- every table the bootstrap and add_data_fix_tables.sql create gets the same
-- automatic grants it would get live. Needs the schema to exist first; the
-- bootstrap's own CREATE SCHEMA IF NOT EXISTS rat is then a no-op.
CREATE SCHEMA IF NOT EXISTS rat;
ALTER DEFAULT PRIVILEGES IN SCHEMA rat GRANT SELECT ON TABLES TO anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA rat GRANT SELECT, INSERT, UPDATE ON TABLES TO picaloco_agent;

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.users (
    id uuid PRIMARY KEY,
    email text,
    created_at timestamptz DEFAULT now()
);

CREATE OR REPLACE FUNCTION auth.uid() RETURNS uuid
LANGUAGE sql STABLE
AS $$ SELECT nullif(current_setting('request.jwt.claim.sub', true), '')::uuid $$;

GRANT USAGE ON SCHEMA auth TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION auth.uid() TO anon, authenticated, service_role;
