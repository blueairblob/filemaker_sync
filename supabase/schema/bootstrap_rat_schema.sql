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
    c.modified_date AS last_updated
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
  GROUP BY c.id, c.image_no, c.category, c.date_taken, c.circa, c.imprecise_date, c.description, c.gauge, cnt.name, org.name, org.type, l.name, r.name, col.name, p.name, u.prints_allowed, u.internet_use, u.publications_use, pm.file_type, pm.width, pm.height, pm.resolution, pm.colour_space, pm.colour_mode, c.cd_no, c.cd_no_hr, c.bw_image_no, c.bw_cd_no, c.active_area, c.corporate_body, c.facility, c.modified_date;

-- anon is read-only, and only through this view + the small lookup tables below --
-- never given direct access to rat.catalog itself (valuation/owners_ref are not
-- public columns) or to catalog_metadata/catalog_builder/usage/picture_metadata
-- (no public need, and picture_metadata.file_location would leak internal detail).
GRANT USAGE ON SCHEMA rat TO anon;
GRANT SELECT ON rat.mobile_catalog_view TO anon;
GRANT SELECT ON rat.photographer, rat.location, rat.organisation,
                rat.collection, rat.country, rat.route, rat.builder TO anon;
