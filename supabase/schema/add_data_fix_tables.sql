-- add_data_fix_tables.sql
--
-- "Data Fixes": per-photo values extracted from rat.catalog.description (first
-- fixes: loco_number, loco_name) that the client then reviews in picaloco_web's
-- admin area. Spec: picaloco_web/docs/superpowers/specs/
-- 2026-11-14-data-fixes-loco-number-name-design.md (sections 4, 5 and 8).
--
-- The core promise: a value a person has reviewed can never be overwritten by
-- the extractor or a sync. Enforced here, in SQL:
--   * The FileMaker -> Postgres pipeline (db_dml_loader.py) never writes these
--     tables. They are keyed on image_no and have NO FK to rat.catalog, so a
--     catalog change can never cascade into them (orphans are listed instead,
--     see rat.data_fix_orphans()).
--   * data_fix_value_guard (BEFORE UPDATE) rejects any change to "values" or
--     status on a row whose status is no longer 'extracted', unless the change
--     comes from a review RPC (which sets rat.review_context = 'on' for the
--     duration of its own UPDATE only). The extractor, running as the owner
--     role, can still refresh suggested_values/confidence/extractor_version/
--     description_hash/needs_recheck on reviewed rows.
--   * data_fix_value_history (AFTER INSERT OR UPDATE) appends every change of
--     "values"/status to rat.data_fix_history, tagged 'extractor', 'review' or 'model_check'.
--   * Reviews go through SECURITY DEFINER RPCs that check rat.is_admin() and an
--     optimistic-lock updated_at. authenticated gets SELECT only (RLS: admins
--     only), never INSERT/UPDATE/DELETE; anon gets nothing on these tables.
--     The public sees the values only through rat.mobile_catalog_view.
--
-- Requires the Supabase roles anon/authenticated and auth.users/auth.uid()
-- (present on oci; tests/sql/supabase_stubs.sql provides them for tests).
--
-- Idempotent: safe to re-run (IF NOT EXISTS / CREATE OR REPLACE /
-- DROP POLICY IF EXISTS / ON CONFLICT DO NOTHING). Validated against a local
-- postgres:17 container by tests/test_data_fix_schema.py, never the live DB.

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
       AND (p_text IS NULL OR c.description ILIKE '%' || p_text || '%' OR v.image_no ILIKE p_text)
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
