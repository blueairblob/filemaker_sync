-- bootstrap_rat_schema.sql
--
-- One-shot, idempotent bootstrap of the `rat` schema on a FRESH target (no data,
-- no prior migration). Assembled from rat_schema_original.sql (a reference dump,
-- "not meant to be run" per its own header) with the three supabase/schema/fix_*.sql
-- corrections this project already learned the hard way baked in directly, so a
-- fresh target never needs those patches applied afterward:
--   * fix_rat_builder_key.sql      -- builder's real key is `code`, NOT `name`.
--       Do not add UNIQUE(name) on builder (that was fix_rat_idempotency.sql's
--       mistake, reverted on the cloud target -- never introduce it here).
--   * fix_rat_constraints.sql      -- catalog_metadata needs UNIQUE(catalog_id)
--       (the loader's ON CONFLICT (catalog_id) depends on it); picture_metadata
--       should have exactly ONE FK on catalog_id, not the duplicate the original
--       dump has (picture_metadata_catalog_id_fkey / ..._fkey1).
--   * fix_rat_idempotency.sql      -- catalog_builder's real key is
--       (catalog_id, builder_id, builder_order), NOT a bare id. NEVER dedupe on
--       (catalog_id, builder_id) alone -- legitimate multi-build rows differ only
--       in builder_order/payload (5,927 such pairs on the cloud target).
--
-- Run once against a fresh target (psql or the Supabase SQL editor). Safe to
-- re-run: every statement is guarded (CREATE ... IF NOT EXISTS).
--
-- Does NOT create rat_migration.sync_manifest -- that DDL's single source of
-- truth is the SYNC_MANIFEST_DDL string in scripts/db_sync_manifest.py; create it
-- with `python scripts/db_sync_manifest.py --init --target-profile <profile>`.
-- Does NOT create the rat_migration staging tables (ratcatalogue, ratbuilders,
-- ratroutes, ratcollections, prompts) -- those are self-generated from live
-- FileMaker field metadata by `filemaker_extract.py --db-exp --ddl`.
--
-- Also creates (added 2026-09-07, Session 17 -- picaloco_web's dependency,
-- previously only ever applied ad hoc via psql and NOT captured here until
-- now, a real gap this closes): rat.mobile_catalog_view, and anon's grants
-- (read-only, scoped to that view plus the small lookup tables -- never given
-- direct access to rat.catalog or the other base tables). Does NOT touch
-- PGRST_DB_SCHEMAS or any other Docker Compose / PostgREST config needed to
-- actually expose the `rat` schema over the REST API -- that's host-level
-- infra config, not database DDL; confirm it separately on a truly new host.
-- Does NOT set up Tailscale Funnel or DNS -- also host-level, not DDL. A
-- genuinely NEW host (not just restored data on the existing one) will get a
-- new Tailscale hostname, needing Funnel re-enabled and picaloco_web's
-- VITE_SUPABASE_URL/VITE_IMAGES_BASE_URL updated to match.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS rat;
CREATE SCHEMA IF NOT EXISTS rat_migration;

-- =============================================================================
-- Lookup / dimension tables (no FK dependencies within rat.*, or depend only on
-- tables created earlier in this file).
-- =============================================================================

CREATE TABLE IF NOT EXISTS rat.country (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT country_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rat.location (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  country_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT location_pkey PRIMARY KEY (id),
  CONSTRAINT location_country_id_fkey FOREIGN KEY (country_id) REFERENCES rat.country(id)
);

CREATE TABLE IF NOT EXISTS rat.route (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  start_location_id uuid,
  end_location_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  -- organisation_id/country_id/remarks added Session 23 (see
  -- add_collection_route_columns.sql) -- FK constraints added further below,
  -- after rat.organisation exists (organisation is created after route in
  -- this file, so an inline FK here would fail).
  organisation_id uuid,
  country_id uuid,
  remarks text,
  CONSTRAINT route_pkey PRIMARY KEY (id),
  CONSTRAINT route_start_location_id_fkey FOREIGN KEY (start_location_id) REFERENCES rat.location(id),
  CONSTRAINT route_end_location_id_fkey FOREIGN KEY (end_location_id) REFERENCES rat.location(id)
);

CREATE TABLE IF NOT EXISTS rat.organisation (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying UNIQUE,
  type character varying,
  country_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT organisation_pkey PRIMARY KEY (id),
  CONSTRAINT organisation_country_id_fkey FOREIGN KEY (country_id) REFERENCES rat.country(id)
);

CREATE TABLE IF NOT EXISTS rat.collection (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  owner character varying,
  donor character varying,
  storage_location character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  -- photographer_id/print_sales/internet_use/publications_use/
  -- accession_number/contact/remarks added Session 23 (see
  -- add_collection_route_columns.sql). photographer_id's FK constraint is
  -- added further below, after rat.photographer exists. contact holds real
  -- people's names/contact details (donors/collectors) -- anon already has
  -- full SELECT on rat.collection (same as owner/donor, already public), so
  -- this makes contact public too; a deliberate call, not an oversight --
  -- see devlog/worksheet.md Session 23.
  photographer_id uuid,
  print_sales boolean,
  internet_use boolean,
  publications_use boolean,
  accession_number numeric,
  contact text,
  remarks text,
  CONSTRAINT collection_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rat.photographer (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT photographer_pkey PRIMARY KEY (id)
);

-- builder's real natural key is `code` (already NOT NULL UNIQUE below).
-- Deliberately NOT adding UNIQUE(name) -- name is nullable and not the identity.
CREATE TABLE IF NOT EXISTS rat.builder (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  code character varying NOT NULL UNIQUE,
  name character varying,
  location_id uuid,
  plant_code character varying,
  builder_plant character varying,
  remarks character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT builder_pkey PRIMARY KEY (id),
  CONSTRAINT builder_location_id_fkey FOREIGN KEY (location_id) REFERENCES rat.location(id)
);

-- =============================================================================
-- Catalog (the archive spine) and everything keyed off it.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rat.catalog (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  image_no character varying NOT NULL UNIQUE,
  accession_no numeric,
  category character varying,
  date_taken date,
  circa character varying,
  imprecise_date character varying,
  description text,
  condition character varying,
  valuation numeric CHECK (valuation >= 0::numeric),
  entry_date date CHECK (entry_date <= CURRENT_DATE),
  owners_ref character varying,
  cd_no character varying,
  cd_no_hr character varying,
  bw_image_no character varying,
  bw_cd_no character varying,
  gauge character varying,
  works_number character varying,
  year_built character varying,
  plant_code character varying,
  picture character varying,
  active_area character varying,
  corporate_body character varying,
  facility character varying,
  parent_folder character varying,
  imgref_stem character varying,
  website character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_pkey PRIMARY KEY (id)
);

-- UNIQUE(catalog_id) added inline (fix_rat_constraints.sql): the loader's
-- batch_upsert conflicts on ON CONFLICT (catalog_id) for this table.
CREATE TABLE IF NOT EXISTS rat.catalog_metadata (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid UNIQUE,
  organisation_id uuid,
  location_id uuid,
  route_id uuid,
  collection_id uuid,
  photographer_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_metadata_pkey PRIMARY KEY (id),
  CONSTRAINT catalog_metadata_route_id_fkey FOREIGN KEY (route_id) REFERENCES rat.route(id),
  CONSTRAINT catalog_metadata_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
  CONSTRAINT catalog_metadata_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES rat.collection(id),
  CONSTRAINT catalog_metadata_location_id_fkey FOREIGN KEY (location_id) REFERENCES rat.location(id),
  CONSTRAINT catalog_metadata_organisation_id_fkey FOREIGN KEY (organisation_id) REFERENCES rat.organisation(id),
  CONSTRAINT catalog_metadata_photographer_id_fkey FOREIGN KEY (photographer_id) REFERENCES rat.photographer(id)
);

-- Real key is (catalog_id, builder_id, builder_order) -- added inline
-- (fix_rat_idempotency.sql). NEVER dedupe on (catalog_id, builder_id) alone.
CREATE TABLE IF NOT EXISTS rat.catalog_builder (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid,
  builder_id uuid,
  builder_order integer NOT NULL,
  plant_code character varying,
  works_number character varying,
  year_built character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_builder_pkey PRIMARY KEY (id),
  CONSTRAINT catalog_builder_builder_id_fkey FOREIGN KEY (builder_id) REFERENCES rat.builder(id),
  CONSTRAINT catalog_builder_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
  UNIQUE (catalog_id, builder_id, builder_order)
);

-- No `id`/PK by design (matches the only real reference we have -- the live
-- cloud schema dump). catalog_id doubles as the row's natural key.
CREATE TABLE IF NOT EXISTS rat.usage (
  catalog_id uuid UNIQUE,
  prints_allowed boolean,
  internet_use boolean,
  publications_use boolean,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT usage_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);

-- Single catalog_id FK only -- the original dump has a duplicate
-- (picture_metadata_catalog_id_fkey / ..._fkey1), dropped by fix_rat_constraints.sql;
-- never created here in the first place. `tags` was a lossy `ARRAY` in the
-- reference dump (element type wasn't captured); text[] matches ai_description's
-- free-text sibling column -- revisit if the real source turns out different.
CREATE TABLE IF NOT EXISTS rat.picture_metadata (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid UNIQUE,
  file_name character varying,
  file_location character varying,
  file_type character varying,
  file_size bigint,
  width integer,
  height integer,
  resolution character varying,
  colour_space character varying CHECK (colour_space IS NULL OR (colour_space::text = ANY (ARRAY['sRGB'::character varying::text, 'Adobe RGB'::character varying::text, 'ProPhoto RGB'::character varying::text, 'CMYK'::character varying::text, 'LAB'::character varying::text, 'Grayscale'::character varying::text]))),
  colour_mode character varying CHECK (colour_mode IS NULL OR (colour_mode::text = ANY (ARRAY['colour'::character varying::text, 'black_and_white'::character varying::text, 'grayscale'::character varying::text, 'sepia'::character varying::text]))),
  ai_description text,
  tags text[],
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT picture_metadata_pkey PRIMARY KEY (id),
  CONSTRAINT picture_metadata_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);

-- Deferred FK constraints for the Session 23 columns above (route.organisation_id/
-- collection.photographer_id) -- added here, after every CREATE TABLE, because
-- the referenced tables (organisation, photographer) are created later in this
-- file than the tables that reference them. Guarded/idempotent, same pattern as
-- fix_rat_idempotency.sql's constraint-adding. See add_collection_route_columns.sql
-- for the equivalent standalone migration (this block mirrors it for fresh installs).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'route_organisation_id_fkey') THEN
        ALTER TABLE rat.route ADD CONSTRAINT route_organisation_id_fkey
            FOREIGN KEY (organisation_id) REFERENCES rat.organisation(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'route_country_id_fkey') THEN
        ALTER TABLE rat.route ADD CONSTRAINT route_country_id_fkey
            FOREIGN KEY (country_id) REFERENCES rat.country(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'collection_photographer_id_fkey') THEN
        ALTER TABLE rat.collection ADD CONSTRAINT collection_photographer_id_fkey
            FOREIGN KEY (photographer_id) REFERENCES rat.photographer(id);
    END IF;
END $$;

-- =============================================================================
-- rat_migration support tables with NO discoverable DDL anywhere in the repo --
-- reconstructed from code usage (scripts/db_dml_loader.py get_or_create_user(),
-- get_last_migrated_id(), update_migration_log()), NOT a live capture. Worth
-- reconciling against the cloud project's actual DDL later.
-- =============================================================================

-- Required: get_or_create_user() is called from main() for every run's audit
-- columns (created_by/modified_by), looked up/inserted by username.
CREATE TABLE IF NOT EXISTS rat_migration.users (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  username text NOT NULL UNIQUE,
  email text
);

-- Only required so main()'s existence-check gate passes; get_last_migrated_id()/
-- update_migration_log() are defined but never actually called anywhere today.
CREATE TABLE IF NOT EXISTS rat_migration.migration_log (
  table_name text PRIMARY KEY,
  last_migrated_id text,
  updated_at timestamptz DEFAULT now()
);

-- =============================================================================
-- Public-facing view + anon grants (added 2026-09-07, Session 17 -- these were
-- created ad hoc via psql in Sessions 14/15 and never folded back into this
-- bootstrap script until now, which meant a fresh rebuild from this file alone
-- would have left picaloco_web with nothing to query and anon's grants
-- undefined. This is now the single source of truth for both.)
--
-- CREATE VIEW has no IF NOT EXISTS in Postgres -- CREATE OR REPLACE is the
-- idempotent equivalent (safe to re-run; will not touch any table's data).
-- =============================================================================

-- rat.mobile_catalog_view itself is defined in the "Data Fixes" section at the
-- end of this file (added 2026-11-14): it now joins rat.data_fix_value for the
-- loco_numbers/loco_names columns, so it must be created after those tables.

-- anon is read-only, and only through this view + the small lookup tables below --
-- never given direct access to rat.catalog itself (valuation/owners_ref are not
-- public columns) or to catalog_metadata/catalog_builder/usage/picture_metadata
-- (no public need, and picture_metadata.file_location would leak internal detail).
GRANT USAGE ON SCHEMA rat TO anon;
-- (GRANT SELECT ON rat.mobile_catalog_view TO anon is in the Data Fixes section.)
GRANT SELECT ON rat.photographer, rat.location, rat.organisation,
                rat.collection, rat.country, rat.route, rat.builder TO anon;

-- =============================================================================
-- Data Fixes (added 2026-11-14) -- copied verbatim from add_data_fix_tables.sql
-- (lines 34-end; see that file's header for the design). Also redefines
-- rat.mobile_catalog_view with the loco_numbers/loco_names columns, revokes
-- anon/authenticated from the private base tables, and grants the view to
-- anon. Keep the two files in step: change add_data_fix_tables.sql first,
-- then re-copy its body here.
-- =============================================================================

BEGIN;

-- =============================================================================
-- Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS rat.data_fix (
  id text PRIMARY KEY,
  title text NOT NULL,
  summary text,
  instructions text NOT NULL DEFAULT '',
  sort_order integer NOT NULL
);

CREATE TABLE IF NOT EXISTS rat.data_fix_value (
  image_no text NOT NULL,
  fix_id text NOT NULL REFERENCES rat.data_fix(id),
  "values" text[] NOT NULL DEFAULT '{}',
  suggested_values text[] NOT NULL DEFAULT '{}',
  confidence text NOT NULL
    CHECK (confidence IN ('none', 'low', 'med', 'high')),
  status text NOT NULL DEFAULT 'extracted'
    CHECK (status IN ('extracted', 'verified', 'corrected', 'none_visible', 'no_loco')),
  needs_recheck boolean NOT NULL DEFAULT false,
  description_hash text NOT NULL,
  extractor_version text NOT NULL,
  reviewed_by uuid,
  reviewed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (image_no, fix_id)
);

CREATE INDEX IF NOT EXISTS data_fix_value_fix_conf_status_idx
  ON rat.data_fix_value (fix_id, confidence, status);
CREATE INDEX IF NOT EXISTS data_fix_value_recheck_idx
  ON rat.data_fix_value (fix_id) WHERE needs_recheck;

-- Append-only. No FKs on image_no/fix_id: history must outlive anything.
CREATE TABLE IF NOT EXISTS rat.data_fix_history (
  id bigserial PRIMARY KEY,
  image_no text NOT NULL,
  fix_id text NOT NULL,
  old_values text[],
  new_values text[],
  old_status text,
  new_status text,
  changed_by uuid,
  changed_at timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL CHECK (source IN ('extractor', 'review', 'model_check'))
);

CREATE INDEX IF NOT EXISTS data_fix_history_image_fix_idx
  ON rat.data_fix_history (image_no, fix_id, changed_at);

CREATE TABLE IF NOT EXISTS rat.admin_user (
  user_id uuid PRIMARY KEY REFERENCES auth.users(id)
);

-- =============================================================================
-- Triggers on data_fix_value
-- =============================================================================

CREATE OR REPLACE FUNCTION rat.data_fix_value_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
  IF OLD.status <> 'extracted'
     AND (NEW."values" IS DISTINCT FROM OLD."values"
          OR NEW.status IS DISTINCT FROM OLD.status)
     AND coalesce(current_setting('rat.review_context', true), '') <> 'on' THEN
    RAISE EXCEPTION 'reviewed value is protected'
      USING DETAIL = format('image_no=%s fix_id=%s status=%s', OLD.image_no, OLD.fix_id, OLD.status),
            HINT = 'Reviewed values change only through rat.review_fix_value/rat.confirm_fix_values.';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION rat.data_fix_value_history()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
  IF TG_OP = 'INSERT'
     OR NEW."values" IS DISTINCT FROM OLD."values"
     OR NEW.status IS DISTINCT FROM OLD.status THEN
    INSERT INTO rat.data_fix_history
      (image_no, fix_id, old_values, new_values, old_status, new_status, changed_by, source)
    VALUES
      (NEW.image_no, NEW.fix_id,
       CASE WHEN TG_OP = 'UPDATE' THEN OLD."values" END, NEW."values",
       CASE WHEN TG_OP = 'UPDATE' THEN OLD.status END, NEW.status,
       auth.uid(),
       CASE WHEN coalesce(current_setting('rat.review_context', true), '') = 'on'
            THEN CASE WHEN coalesce(current_setting('rat.review_source', true), '') = 'model_check'
                      THEN 'model_check' ELSE 'review' END
            ELSE 'extractor' END);
  END IF;
  RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER data_fix_value_guard
  BEFORE UPDATE ON rat.data_fix_value
  FOR EACH ROW EXECUTE FUNCTION rat.data_fix_value_guard();

CREATE OR REPLACE TRIGGER data_fix_value_history
  AFTER INSERT OR UPDATE ON rat.data_fix_value
  FOR EACH ROW EXECUTE FUNCTION rat.data_fix_value_history();

-- =============================================================================
-- Functions (RPCs). All SECURITY DEFINER with an empty search_path, so every
-- reference is schema-qualified. All but is_admin() raise 42501 for non-admins.
-- =============================================================================

CREATE OR REPLACE FUNCTION rat.is_admin()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  SELECT EXISTS (SELECT 1 FROM rat.admin_user a WHERE a.user_id = auth.uid());
$$;

CREATE OR REPLACE FUNCTION rat.review_fix_value(
  p_image_no text,
  p_fix_id text,
  p_values text[],
  p_status text,
  p_expected_updated_at timestamptz
)
RETURNS rat.data_fix_value
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_current timestamptz;
  v_row rat.data_fix_value;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  IF p_status IS NULL
     OR p_status NOT IN ('verified', 'corrected', 'none_visible', 'no_loco') THEN
    RAISE EXCEPTION 'invalid review status: %', p_status USING ERRCODE = '22023';
  END IF;

  SELECT v.updated_at INTO v_current
    FROM rat.data_fix_value v
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id
     FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no data_fix_value for image_no=% fix_id=%', p_image_no, p_fix_id
      USING ERRCODE = 'P0002';
  END IF;
  IF v_current IS DISTINCT FROM p_expected_updated_at THEN
    -- Not 40001: PostgREST retries a transaction that fails with serialization_failure, without
    -- limit, so a stale save would loop on the server and hold a pooled connection. PT409 makes
    -- PostgREST answer HTTP 409 at once.
    RAISE EXCEPTION 'record changed since it was loaded' USING ERRCODE = 'PT409';
  END IF;

  PERFORM pg_catalog.set_config('rat.review_context', 'on', true);
  UPDATE rat.data_fix_value v
     SET "values" = CASE WHEN p_status IN ('none_visible', 'no_loco') THEN '{}'::text[]
                         ELSE coalesce(p_values, '{}'::text[]) END,
         status = p_status,
         reviewed_by = auth.uid(),
         reviewed_at = now(),
         needs_recheck = false,
         description_hash = pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
           coalesce((SELECT c.description FROM rat.catalog c
                      WHERE c.image_no = p_image_no), ''), 'UTF8')), 'hex')
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id
  RETURNING v.* INTO v_row;
  -- Close the window again: only this UPDATE runs in review context.
  PERFORM pg_catalog.set_config('rat.review_context', '', true);

  RETURN v_row;
END;
$$;

CREATE OR REPLACE FUNCTION rat.confirm_fix_values(p_fix_id text, p_items jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_count integer;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;

  PERFORM pg_catalog.set_config('rat.review_context', 'on', true);
  UPDATE rat.data_fix_value v
     SET "values" = v.suggested_values,
         status = 'verified',
         reviewed_by = auth.uid(),
         reviewed_at = now(),
         needs_recheck = false,
         description_hash = pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
           coalesce((SELECT c.description FROM rat.catalog c
                      WHERE c.image_no = v.image_no), ''), 'UTF8')), 'hex')
    FROM (SELECT DISTINCT i.image_no, i.updated_at
            FROM pg_catalog.jsonb_to_recordset(coalesce(p_items, '[]'::jsonb))
                 AS i(image_no text, updated_at timestamptz)) AS item
   WHERE v.fix_id = p_fix_id
     AND v.image_no = item.image_no
     AND v.updated_at = item.updated_at    -- stale items simply don't match
     AND v.status = 'extracted';           -- never overwrite a person's review
                                           -- (e.g. a needs-recheck correction)
  GET DIAGNOSTICS v_count = ROW_COUNT;
  PERFORM pg_catalog.set_config('rat.review_context', '', true);

  RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION rat.data_fix_progress()
RETURNS TABLE (fix_id text, status text, confidence text, needs_recheck boolean, n bigint)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    SELECT v.fix_id, v.status, v.confidence, v.needs_recheck, count(*)
      FROM rat.data_fix_value v
     GROUP BY v.fix_id, v.status, v.confidence, v.needs_recheck
     ORDER BY v.fix_id, v.status, v.confidence, v.needs_recheck;
END;
$$;

CREATE OR REPLACE FUNCTION rat.data_fix_queue(
  p_fix_id text,
  p_confidence text[] DEFAULT NULL,
  p_status text[] DEFAULT ARRAY['extracted'],
  p_include_recheck boolean DEFAULT true,
  p_collection text DEFAULT NULL,
  p_text text DEFAULT NULL,
  p_after text DEFAULT NULL,
  p_limit integer DEFAULT 25
)
RETURNS TABLE (
  image_no text,
  description text,
  collection text,
  date_taken date,
  cd_no character varying,
  "values" text[],
  suggested_values text[],
  confidence text,
  status text,
  needs_recheck boolean,
  updated_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    SELECT v.image_no, c.description, col.name::text, c.date_taken, c.cd_no,
           v."values", v.suggested_values, v.confidence, v.status,
           v.needs_recheck, v.updated_at
      FROM rat.data_fix_value v
      JOIN rat.catalog c ON c.image_no = v.image_no
      LEFT JOIN rat.catalog_metadata cm ON cm.catalog_id = c.id
      LEFT JOIN rat.collection col ON col.id = cm.collection_id
     WHERE v.fix_id = p_fix_id
       AND (v.status = ANY (p_status) OR (p_include_recheck AND v.needs_recheck))
       AND (p_confidence IS NULL OR v.confidence = ANY (p_confidence))
       AND (p_collection IS NULL OR col.name = p_collection)
       AND (p_text IS NULL OR c.description ILIKE '%' || p_text || '%' OR v.image_no = p_text)
       AND v.image_no > coalesce(p_after, '')
     ORDER BY v.image_no
     LIMIT least(greatest(coalesce(p_limit, 25), 1), 500);
END;
$$;

CREATE OR REPLACE FUNCTION rat.data_fix_orphans()
RETURNS TABLE (image_no text, fix_id text, status text)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    SELECT v.image_no, v.fix_id, v.status
      FROM rat.data_fix_value v
     WHERE v.status <> 'extracted'
       AND NOT EXISTS (SELECT 1 FROM rat.catalog c WHERE c.image_no = v.image_no)
     ORDER BY v.image_no, v.fix_id;
END;
$$;

-- =============================================================================
-- Grants and RLS
-- =============================================================================

ALTER TABLE rat.data_fix ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.data_fix_value ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.data_fix_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.admin_user ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS data_fix_admin_select ON rat.data_fix;
CREATE POLICY data_fix_admin_select ON rat.data_fix
  FOR SELECT TO authenticated USING (rat.is_admin());

DROP POLICY IF EXISTS data_fix_value_admin_select ON rat.data_fix_value;
CREATE POLICY data_fix_value_admin_select ON rat.data_fix_value
  FOR SELECT TO authenticated USING (rat.is_admin());

DROP POLICY IF EXISTS data_fix_history_admin_select ON rat.data_fix_history;
CREATE POLICY data_fix_history_admin_select ON rat.data_fix_history
  FOR SELECT TO authenticated USING (rat.is_admin());

DROP POLICY IF EXISTS admin_user_self_select ON rat.admin_user;
CREATE POLICY admin_user_self_select ON rat.admin_user
  FOR SELECT TO authenticated USING (user_id = auth.uid());

-- Live oci's default ACL on schema rat ({anon=r, authenticated=r,
-- picaloco_agent=arw}) auto-grants on every new table, so revoke explicitly.
-- picaloco_agent only exists on oci, hence the guard.
REVOKE ALL ON rat.data_fix, rat.data_fix_value, rat.data_fix_history, rat.admin_user
  FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SEQUENCE rat.data_fix_history_id_seq FROM anon, authenticated, PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'picaloco_agent') THEN
    REVOKE ALL ON rat.data_fix, rat.data_fix_value, rat.data_fix_history, rat.admin_user
      FROM picaloco_agent;
    REVOKE ALL ON SEQUENCE rat.data_fix_history_id_seq FROM picaloco_agent;
  END IF;
END $$;

-- This feature makes authenticated sessions real (admin sign-in), and live oci
-- has authenticated holding SELECT on the private base tables (from the same
-- default ACL). Close them to authenticated, matching anon: rat.catalog's
-- valuation/owners_ref and picture_metadata.file_location are never public.
-- Also revoked from anon, so a fresh bootstrap under the same default ACL ends
-- in the same state. The public surface (mobile_catalog_view, the lookup
-- tables, sync_status) keeps its SELECT for both roles.
REVOKE ALL ON rat.catalog, rat.catalog_metadata, rat.catalog_builder,
              rat.usage, rat.picture_metadata
  FROM anon, authenticated;

-- authenticated needs schema USAGE to reach anything in rat (anon already has
-- it from bootstrap_rat_schema.sql, for mobile_catalog_view).
GRANT USAGE ON SCHEMA rat TO authenticated;
GRANT SELECT ON rat.data_fix, rat.data_fix_value, rat.data_fix_history TO authenticated;

REVOKE EXECUTE ON FUNCTION
  rat.is_admin(),
  rat.review_fix_value(text, text, text[], text, timestamptz),
  rat.confirm_fix_values(text, jsonb),
  rat.data_fix_progress(),
  rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer),
  rat.data_fix_orphans(),
  rat.data_fix_value_guard(),
  rat.data_fix_value_history()
  FROM PUBLIC, anon;
REVOKE EXECUTE ON FUNCTION rat.data_fix_value_guard(), rat.data_fix_value_history()
  FROM authenticated;
GRANT EXECUTE ON FUNCTION
  rat.is_admin(),
  rat.review_fix_value(text, text, text[], text, timestamptz),
  rat.confirm_fix_values(text, jsonb),
  rat.data_fix_progress(),
  rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer),
  rat.data_fix_orphans()
  TO authenticated;

-- =============================================================================
-- Seed
-- =============================================================================

INSERT INTO rat.data_fix (id, title, summary, sort_order) VALUES
  ('loco_number', 'Loco number', 'Running number(s) carried by the loco', 1),
  ('loco_name', 'Loco name', 'Name(s) carried by the loco', 2)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- Public view: bootstrap_rat_schema.sql's rat.mobile_catalog_view, verbatim,
-- with four columns appended at the end (CREATE OR REPLACE VIEW only allows
-- appending). The view runs with its owner's rights, so anon reads these
-- values through it without any grant on data_fix_value.
--   loco_numbers / loco_names: '{}' for none_visible/no_loco, else "values";
--     NULL when the photo has no row.
--   *_text: NULL when there is no row, or the row is still 'extracted' with no
--     values (public search falls back to the description for these); '' for
--     none_visible/no_loco (no fallback); else array_to_string(values, '; ').
-- =============================================================================

CREATE OR REPLACE VIEW rat.mobile_catalog_view AS
SELECT c.image_no,
    c.category,
    c.date_taken,
    c.circa,
    c.imprecise_date,
    c.description,
    c.gauge,
    concat('https://cdn.example.com/thumbnails/', c.image_no, '.webp') AS thumbnail_url,
    cnt.name AS country,
    org.name AS organisation,
    org.type AS organisation_type,
    l.name AS location,
    r.name AS route,
    col.name AS collection,
    p.name AS photographer,
    u.prints_allowed,
    u.internet_use,
    u.publications_use,
    array_agg(DISTINCT jsonb_build_object('builder_name', b.name, 'builder_code', b.code, 'works_number', cb.works_number, 'year_built', cb.year_built, 'plant_code', cb.plant_code, 'builder_order', cb.builder_order)) FILTER (WHERE b.id IS NOT NULL) AS builders,
    pm.file_type,
    pm.width,
    pm.height,
    pm.resolution,
    pm.colour_space,
    pm.colour_mode,
    c.cd_no,
    c.cd_no_hr,
    c.bw_image_no,
    c.bw_cd_no,
    c.active_area,
    c.corporate_body,
    c.facility,
    c.modified_date AS last_updated,
    CASE WHEN dfn.status IN ('none_visible', 'no_loco') THEN '{}'::text[]
         ELSE dfn."values" END AS loco_numbers,
    CASE WHEN dfn.status IS NULL THEN NULL::text
         WHEN dfn.status IN ('none_visible', 'no_loco') THEN ''
         WHEN dfn.status = 'extracted' AND cardinality(dfn."values") = 0 THEN NULL::text
         ELSE array_to_string(dfn."values", '; ') END AS loco_numbers_text,
    CASE WHEN dfa.status IN ('none_visible', 'no_loco') THEN '{}'::text[]
         ELSE dfa."values" END AS loco_names,
    CASE WHEN dfa.status IS NULL THEN NULL::text
         WHEN dfa.status IN ('none_visible', 'no_loco') THEN ''
         WHEN dfa.status = 'extracted' AND cardinality(dfa."values") = 0 THEN NULL::text
         ELSE array_to_string(dfa."values", '; ') END AS loco_names_text
   FROM rat.catalog c
     LEFT JOIN rat.catalog_metadata cm ON c.id = cm.catalog_id
     LEFT JOIN rat.organisation org ON cm.organisation_id = org.id
     LEFT JOIN rat.country cnt ON org.country_id = cnt.id
     LEFT JOIN rat.location l ON cm.location_id = l.id
     LEFT JOIN rat.route r ON cm.route_id = r.id
     LEFT JOIN rat.collection col ON cm.collection_id = col.id
     LEFT JOIN rat.photographer p ON cm.photographer_id = p.id
     LEFT JOIN rat.usage u ON c.id = u.catalog_id
     LEFT JOIN rat.catalog_builder cb ON c.id = cb.catalog_id
     LEFT JOIN rat.builder b ON cb.builder_id = b.id
     LEFT JOIN rat.picture_metadata pm ON c.id = pm.catalog_id
     LEFT JOIN rat.data_fix_value dfn ON dfn.image_no = c.image_no AND dfn.fix_id = 'loco_number'
     LEFT JOIN rat.data_fix_value dfa ON dfa.image_no = c.image_no AND dfa.fix_id = 'loco_name'
  GROUP BY c.id, c.image_no, c.category, c.date_taken, c.circa, c.imprecise_date, c.description, c.gauge, cnt.name, org.name, org.type, l.name, r.name, col.name, p.name, u.prints_allowed, u.internet_use, u.publications_use, pm.file_type, pm.width, pm.height, pm.resolution, pm.colour_space, pm.colour_mode, c.cd_no, c.cd_no_hr, c.bw_image_no, c.bw_cd_no, c.active_area, c.corporate_body, c.facility, c.modified_date, dfn.status, dfn."values", dfa.status, dfa."values";

GRANT SELECT ON rat.mobile_catalog_view TO anon;

COMMIT;

-- =============================================================================
-- Model check (added 2026-11-16) -- copied verbatim from
-- add_model_check_tables.sql (from its first BEGIN; to the end; see that file's
-- header for the design). Keep the two files in step: change
-- add_model_check_tables.sql first, then re-copy its body here.
-- =============================================================================

BEGIN;

-- =============================================================================
-- Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS rat.model_task (
  id text PRIMARY KEY,
  question text NOT NULL,
  -- [{"key":"1","id":"steam_loco","label":"Steam loco","loco":true}, ...]
  answers jsonb NOT NULL,
  sure_threshold real NOT NULL DEFAULT 0.9
    CHECK (sure_threshold > 0 AND sure_threshold <= 1),
  links jsonb,
  sort_order integer NOT NULL DEFAULT 0,
  client text,
  note text
);

CREATE TABLE IF NOT EXISTS rat.model_run (
  id bigserial PRIMARY KEY,
  task_id text NOT NULL REFERENCES rat.model_task(id),
  model text NOT NULL,
  version text NOT NULL,
  notes text,
  answers jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  n_predictions integer NOT NULL DEFAULT 0,
  UNIQUE (task_id, version)
);

CREATE TABLE IF NOT EXISTS rat.model_prediction (
  run_id bigint NOT NULL REFERENCES rat.model_run(id) ON DELETE CASCADE,
  image_no text NOT NULL,
  answer text NOT NULL,
  p real NOT NULL CHECK (p >= 0 AND p <= 1),
  probs real[] NOT NULL,
  PRIMARY KEY (run_id, image_no)
);

CREATE INDEX IF NOT EXISTS model_prediction_run_answer_p_idx
  ON rat.model_prediction (run_id, answer, p);

CREATE TABLE IF NOT EXISTS rat.model_answer (
  id bigserial PRIMARY KEY,
  task_id text NOT NULL REFERENCES rat.model_task(id),
  image_no text NOT NULL,
  answer text NOT NULL,
  shown_run_id bigint REFERENCES rat.model_run(id),
  shown_answer text,
  mode text NOT NULL CHECK (mode IN ('sure', 'unsure', 'sample')),
  answered_by uuid,
  answered_at timestamptz NOT NULL DEFAULT now(),
  -- A blind sample answer must not record what the model said.
  CHECK (mode <> 'sample' OR (shown_run_id IS NULL AND shown_answer IS NULL))
);

CREATE INDEX IF NOT EXISTS model_answer_task_image_answered_idx
  ON rat.model_answer (task_id, image_no, answered_at DESC);

-- =============================================================================
-- Append-only guard for rat.model_answer
-- =============================================================================

CREATE OR REPLACE FUNCTION rat.model_answer_append_only()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
  RAISE EXCEPTION 'model answers are append-only'
    USING ERRCODE = 'restrict_violation';
END;
$$;

DROP TRIGGER IF EXISTS model_answer_no_change ON rat.model_answer;
CREATE TRIGGER model_answer_no_change
  BEFORE UPDATE OR DELETE ON rat.model_answer
  FOR EACH ROW EXECUTE FUNCTION rat.model_answer_append_only();

DROP TRIGGER IF EXISTS model_answer_no_truncate ON rat.model_answer;
CREATE TRIGGER model_answer_no_truncate
  BEFORE TRUNCATE ON rat.model_answer
  FOR EACH STATEMENT EXECUTE FUNCTION rat.model_answer_append_only();

-- =============================================================================
-- Functions
-- =============================================================================
-- (added by the next change: queue, save and score functions)

-- =============================================================================
-- Grants and RLS
-- =============================================================================

ALTER TABLE rat.model_task ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.model_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.model_prediction ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.model_answer ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS model_task_admin_select ON rat.model_task;
CREATE POLICY model_task_admin_select ON rat.model_task
  FOR SELECT TO authenticated USING (rat.is_admin());

DROP POLICY IF EXISTS model_run_admin_select ON rat.model_run;
CREATE POLICY model_run_admin_select ON rat.model_run
  FOR SELECT TO authenticated USING (rat.is_admin());

-- Live oci's default ACL on schema rat auto-grants on every new table, so
-- revoke explicitly. picaloco_agent only exists on oci, hence the guard.
REVOKE ALL ON rat.model_task, rat.model_run, rat.model_prediction, rat.model_answer
  FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SEQUENCE rat.model_run_id_seq, rat.model_answer_id_seq
  FROM anon, authenticated, PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'picaloco_agent') THEN
    REVOKE ALL ON rat.model_task, rat.model_run, rat.model_prediction, rat.model_answer
      FROM picaloco_agent;
    REVOKE ALL ON SEQUENCE rat.model_run_id_seq, rat.model_answer_id_seq
      FROM picaloco_agent;
  END IF;
END $$;

GRANT USAGE ON SCHEMA rat TO authenticated;
GRANT SELECT ON rat.model_task, rat.model_run TO authenticated;

COMMIT;
