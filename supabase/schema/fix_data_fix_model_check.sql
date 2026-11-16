-- fix_data_fix_model_check.sql (2026-11-16)
--
-- Data Fixes history may now be tagged 'model_check' (a model-check answer
-- filling "No loco" inside review context sets rat.review_source), and
-- rat.data_fix_queue's p_text also matches image_no exactly.
--
-- Already applied to add_data_fix_tables.sql and bootstrap_rat_schema.sql.
-- This file is only for an existing database. Idempotent; grants are kept
-- by CREATE OR REPLACE. Touches no data_fix_value row.

ALTER TABLE rat.data_fix_history
  DROP CONSTRAINT IF EXISTS data_fix_history_source_check,
  ADD CONSTRAINT data_fix_history_source_check
    CHECK (source IN ('extractor', 'review', 'model_check'));

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
