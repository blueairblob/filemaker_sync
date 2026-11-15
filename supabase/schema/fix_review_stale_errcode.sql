-- fix_review_stale_errcode.sql (2026-11-15)
--
-- rat.review_fix_value raised SQLSTATE 40001 on a stale save. PostgREST
-- retries 40001 (serialization_failure) without limit, so a stale save
-- looped on the server, timed out the browser after 30s and kept holding
-- a pooled connection. PT409 makes PostgREST answer HTTP 409 at once.
--
-- Already applied to add_data_fix_tables.sql and bootstrap_rat_schema.sql.
-- This file is only for an existing database. Idempotent; grants are kept
-- by CREATE OR REPLACE.

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
     OR p_status NOT IN ('verified', 'corrected', 'none_visible', 'no_loco')
  THEN
    RAISE EXCEPTION 'invalid review status: %', p_status
      USING ERRCODE = '22023';
  END IF;

  SELECT v.updated_at INTO v_current
    FROM rat.data_fix_value v
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id
     FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no data_fix_value for image_no=% fix_id=%',
      p_image_no, p_fix_id
      USING ERRCODE = 'P0002';
  END IF;
  IF v_current IS DISTINCT FROM p_expected_updated_at THEN
    RAISE EXCEPTION 'record changed since it was loaded'
      USING ERRCODE = 'PT409';
  END IF;

  PERFORM pg_catalog.set_config('rat.review_context', 'on', true);
  UPDATE rat.data_fix_value v
     SET "values" = CASE WHEN p_status IN ('none_visible', 'no_loco')
                         THEN '{}'::text[]
                         ELSE coalesce(p_values, '{}'::text[]) END,
         status = p_status,
         reviewed_by = auth.uid(),
         reviewed_at = now(),
         needs_recheck = false,
         description_hash = pg_catalog.encode(pg_catalog.sha256(
           pg_catalog.convert_to(
             coalesce((SELECT c.description FROM rat.catalog c
                        WHERE c.image_no = p_image_no), ''),
             'UTF8')), 'hex')
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id
  RETURNING v.* INTO v_row;
  PERFORM pg_catalog.set_config('rat.review_context', '', true);

  RETURN v_row;
END;
$$;
