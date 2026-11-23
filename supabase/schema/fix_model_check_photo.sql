-- WARNING: Re-running this file after add_volunteer_tables.sql reverts that file's changes to the
-- functions/policies/checks it shares (e.g. volunteers lose access, the 'undo' history source breaks).
-- Re-apply add_volunteer_tables.sql afterwards.
--
-- fix_model_check_photo.sql (2026-11-17)
--
-- Adds rat.model_check_photo: one photo with its latest model-check answer,
-- so a conflict can be answered again from the Model check page.
--
-- Already applied to add_model_check_tables.sql and bootstrap_rat_schema.sql.
-- This file is only for an existing database. Idempotent. Read-only function:
-- touches no row.

BEGIN;

-- One photo, to answer again (the conflict list's "Model check" link). Returns the queue's columns
-- with the latest answer filled in, plus how it was answered and the run whose prediction is shown
-- (p_run_id, or the task's newest run). A photo last answered in the random sample stays blind: no
-- prediction, so a new answer can be saved blind and still count for accuracy. No row when the
-- photo is not in the catalog.
CREATE OR REPLACE FUNCTION rat.model_check_photo(
  p_task_id text,
  p_image_no text,
  p_run_id bigint DEFAULT NULL
)
RETURNS TABLE (
  image_no text,
  sort_key text,
  description text,
  collection text,
  date_taken date,
  cd_no character varying,
  model_answer text,
  model_p real,
  checked_answer text,
  checked_at timestamptz,
  fix_state jsonb,
  checked_mode text,
  run_id bigint
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task rat.model_task;
  v_fix_ids text[];
  v_run_id bigint := p_run_id;
  v_last rat.model_answer;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model task %', p_task_id USING ERRCODE = 'P0002';
  END IF;
  IF v_run_id IS NULL THEN
    SELECT r.id INTO v_run_id FROM rat.model_run r
     WHERE r.task_id = p_task_id ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
  ELSIF NOT EXISTS (SELECT 1 FROM rat.model_run r
                     WHERE r.id = v_run_id AND r.task_id = p_task_id) THEN
    RAISE EXCEPTION 'run % is not a run of task %', v_run_id, p_task_id
      USING ERRCODE = '22023';
  END IF;
  IF v_task.links IS NOT NULL THEN
    v_fix_ids := ARRAY(SELECT jsonb_array_elements_text(
                          coalesce(v_task.links -> 'fix_ids', '[]'::jsonb)));
  END IF;
  SELECT a.* INTO v_last FROM rat.model_answer a
   WHERE a.task_id = p_task_id AND a.image_no = p_image_no
   ORDER BY a.answered_at DESC, a.id DESC LIMIT 1;

  RETURN QUERY
    SELECT c.image_no::text, c.image_no::text, c.description, col.name::text,
           c.date_taken, c.cd_no,
           CASE WHEN v_last.mode = 'sample' THEN NULL ELSE pr.answer END,
           CASE WHEN v_last.mode = 'sample' THEN NULL ELSE pr.p END,
           v_last.answer, v_last.answered_at,
           CASE WHEN v_fix_ids IS NULL THEN NULL::jsonb ELSE coalesce(
             (SELECT jsonb_object_agg(f.fix_id, jsonb_build_object(
                       'status', f.status, 'confidence', f.confidence,
                       'values', to_jsonb(f."values"),
                       'suggested_values', to_jsonb(f.suggested_values)))
                FROM rat.data_fix_value f
               WHERE f.image_no = c.image_no AND f.fix_id = ANY (v_fix_ids)),
             '{}'::jsonb) END,
           v_last.mode, v_run_id
      FROM rat.catalog c
      LEFT JOIN rat.catalog_metadata cm ON cm.catalog_id = c.id
      LEFT JOIN rat.collection col ON col.id = cm.collection_id
      LEFT JOIN rat.model_prediction pr ON pr.run_id = v_run_id AND pr.image_no = c.image_no
     WHERE c.image_no = p_image_no;
END;
$$;

REVOKE EXECUTE ON FUNCTION rat.model_check_photo(text, text, bigint) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION rat.model_check_photo(text, text, bigint) TO authenticated;

COMMIT;
