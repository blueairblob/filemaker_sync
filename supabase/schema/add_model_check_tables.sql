-- WARNING: Re-running this file after add_volunteer_tables.sql reverts that file's changes to the
-- functions/policies/checks it shares (e.g. volunteers lose access, the 'undo' history source breaks).
-- Re-apply add_volunteer_tables.sql afterwards.
--
-- add_model_check_tables.sql
--
-- "Model check": volunteers check an offline AI model's per-photo answers in
-- picaloco_web's admin area. A task is a multiple-choice question about a photo
-- (e.g. "Is a loco the main subject?"); a run holds one model version's
-- predictions for the task; an answer is one volunteer's reply to one photo.
--
--   * rat.model_task        the question and its answer list (jsonb).
--   * rat.model_run         one model version's run over a task (UNIQUE task+version).
--   * rat.model_prediction  one row per photo per run (answer, p, probs).
--   * rat.model_answer      volunteers' answers. APPEND-ONLY: a trigger rejects
--                           UPDATE, DELETE and TRUNCATE, so a checked answer can
--                           never be altered or lost. A 'sample' answer is blind:
--                           it records no shown run/answer.
--
-- Like Data Fixes, none of these tables has an FK to rat.catalog (keyed on
-- image_no), and the FileMaker -> Postgres pipeline never writes them.
-- authenticated gets SELECT on model_task/model_run only (RLS: admins only);
-- predictions and answers are reached only through SECURITY DEFINER functions.
-- anon gets nothing.
--
-- RPCs (admin-only, EXECUTE for authenticated): model_check_queue,
-- save_model_answer, model_check_progress, model_check_confusion,
-- model_check_conflicts. save_model_answer is the only writer of
-- rat.data_fix_value here, and only fills rows still 'extracted' with
-- confidence 'none' (Golden Rule 7; see the function's comment).
--
-- Requires add_data_fix_tables.sql first (rat.is_admin()) and the Supabase roles
-- anon/authenticated (tests/sql/supabase_stubs.sql provides them for tests).
--
-- Idempotent: safe to re-run (IF NOT EXISTS / CREATE OR REPLACE /
-- DROP POLICY|TRIGGER IF EXISTS). Validated against a local postgres:17
-- container by tests/test_model_check_schema.py, never the live DB.

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
-- Functions (RPCs). All SECURITY DEFINER with an empty search_path, so every
-- reference is schema-qualified; all raise 42501 for non-admins (the
-- rat.review_fix_value pattern). Bad input raises 22023; a missing task, run
-- or photo raises P0002.
--
-- "Latest answer" for a photo = its rat.model_answer row with the greatest
-- (answered_at, id): answers saved in one transaction share answered_at, so
-- the id breaks the tie.
-- =============================================================================

-- One page of photos to check. sure/unsure: the run's predictions with
-- p >= / < the task's sure_threshold, photos not yet answered for the task,
-- in image_no order. sample: every rat.catalog photo not yet answered blind
-- for the task, in md5(image_no) order, and BLIND: no prediction is returned
-- (model_answer/model_p NULL) and no filters are taken (they would bias the
-- sample). checked_answer/checked_at are always NULL here (answered photos
-- are left out, and sample mode must not pre-select another volunteer's
-- answer); the page fills them in locally after a save.
-- fix_state: the photo's Data Fixes rows for the task's linked fix_ids,
-- {fix_id: {status, confidence, values, suggested_values}}; NULL when the
-- task has no link.
CREATE OR REPLACE FUNCTION rat.model_check_queue(
  p_task_id text,
  p_mode text,
  p_run_id bigint DEFAULT NULL,
  p_answer text DEFAULT NULL,
  p_collection text DEFAULT NULL,
  p_after text DEFAULT NULL,
  p_limit integer DEFAULT 25
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
  fix_state jsonb
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task rat.model_task;
  v_fix_ids text[];
  v_limit integer := least(greatest(coalesce(p_limit, 25), 1), 500);
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model task %', p_task_id USING ERRCODE = 'P0002';
  END IF;
  IF p_mode IS NULL OR p_mode NOT IN ('sure', 'unsure', 'sample') THEN
    RAISE EXCEPTION 'invalid mode: %', p_mode USING ERRCODE = '22023';
  END IF;
  IF p_mode = 'sample' THEN
    IF p_answer IS NOT NULL OR p_collection IS NOT NULL THEN
      RAISE EXCEPTION 'the random sample takes no answer or collection filter'
        USING ERRCODE = '22023';
    END IF;
  ELSIF p_run_id IS NULL THEN
    RAISE EXCEPTION 'mode % needs a run', p_mode USING ERRCODE = '22023';
  END IF;
  IF p_run_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM rat.model_run r
                      WHERE r.id = p_run_id AND r.task_id = p_task_id) THEN
    RAISE EXCEPTION 'run % is not a run of task %', p_run_id, p_task_id
      USING ERRCODE = '22023';
  END IF;
  IF v_task.links IS NOT NULL THEN
    v_fix_ids := ARRAY(SELECT jsonb_array_elements_text(
                          coalesce(v_task.links -> 'fix_ids', '[]'::jsonb)));
  END IF;

  IF p_mode = 'sample' THEN
    RETURN QUERY
      SELECT c.image_no::text, md5(c.image_no::text), c.description, col.name::text,
             c.date_taken, c.cd_no,
             NULL::text, NULL::real,             -- blind: never the prediction
             NULL::text, NULL::timestamptz,
             CASE WHEN v_fix_ids IS NULL THEN NULL::jsonb ELSE coalesce(
               (SELECT jsonb_object_agg(f.fix_id, jsonb_build_object(
                         'status', f.status, 'confidence', f.confidence,
                         'values', to_jsonb(f."values"),
                         'suggested_values', to_jsonb(f.suggested_values)))
                  FROM rat.data_fix_value f
                 WHERE f.image_no = c.image_no AND f.fix_id = ANY (v_fix_ids)),
               '{}'::jsonb) END
        FROM rat.catalog c
        LEFT JOIN rat.catalog_metadata cm ON cm.catalog_id = c.id
        LEFT JOIN rat.collection col ON col.id = cm.collection_id
       WHERE md5(c.image_no::text) > coalesce(p_after, '')
         AND NOT EXISTS (SELECT 1 FROM rat.model_answer a
                          WHERE a.task_id = p_task_id AND a.image_no = c.image_no
                            AND a.mode = 'sample')
       ORDER BY md5(c.image_no::text)
       LIMIT v_limit;
  ELSE
    RETURN QUERY
      SELECT pr.image_no, pr.image_no, c.description, col.name::text,
             c.date_taken, c.cd_no,
             pr.answer, pr.p,
             NULL::text, NULL::timestamptz,
             CASE WHEN v_fix_ids IS NULL THEN NULL::jsonb ELSE coalesce(
               (SELECT jsonb_object_agg(f.fix_id, jsonb_build_object(
                         'status', f.status, 'confidence', f.confidence,
                         'values', to_jsonb(f."values"),
                         'suggested_values', to_jsonb(f.suggested_values)))
                  FROM rat.data_fix_value f
                 WHERE f.image_no = pr.image_no AND f.fix_id = ANY (v_fix_ids)),
               '{}'::jsonb) END
        FROM rat.model_prediction pr
        JOIN rat.catalog c ON c.image_no = pr.image_no
        LEFT JOIN rat.catalog_metadata cm ON cm.catalog_id = c.id
        LEFT JOIN rat.collection col ON col.id = cm.collection_id
       WHERE pr.run_id = p_run_id
         AND CASE WHEN p_mode = 'sure' THEN pr.p >= v_task.sure_threshold
                  ELSE pr.p < v_task.sure_threshold END
         AND (p_answer IS NULL OR pr.answer = p_answer)
         AND (p_collection IS NULL OR col.name = p_collection)
         AND pr.image_no > coalesce(p_after, '')
         AND NOT EXISTS (SELECT 1 FROM rat.model_answer a
                          WHERE a.task_id = p_task_id AND a.image_no = pr.image_no)
       ORDER BY pr.image_no
       LIMIT v_limit;
  END IF;
END;
$$;

-- Saves one volunteer answer (append-only) and, when the task has a Data
-- Fixes link, applies it in the same transaction. Golden Rule 7: only rows
-- still 'extracted' with confidence 'none' are ever changed, and only all of
-- the linked fields at once; every other case changes nothing and is
-- reported as a conflict. The rows are locked FOR UPDATE before they are
-- checked, so a review committed between queue load and save is seen here.
-- Returns {"answer_id", "filled": [fix_id...], "conflicts": [{fix_id, status,
-- confidence, values, suggested_values}...]}.
CREATE OR REPLACE FUNCTION rat.save_model_answer(
  p_task_id text,
  p_image_no text,
  p_answer text,
  p_shown_run_id bigint,
  p_shown_answer text,
  p_mode text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task rat.model_task;
  v_answer jsonb;
  v_answer_id bigint;
  v_flag boolean;
  v_fix_ids text[];
  v_n_fill integer;
  v_n_updated integer;
  v_filled text[] := '{}';
  v_conflicts jsonb := '[]'::jsonb;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model task %', p_task_id USING ERRCODE = 'P0002';
  END IF;
  IF p_mode IS NULL OR p_mode NOT IN ('sure', 'unsure', 'sample') THEN
    RAISE EXCEPTION 'invalid mode: %', p_mode USING ERRCODE = '22023';
  END IF;
  SELECT e INTO v_answer
    FROM jsonb_array_elements(v_task.answers) AS e
   WHERE e ->> 'id' = p_answer
   LIMIT 1;
  IF v_answer IS NULL THEN
    RAISE EXCEPTION 'answer % is not an answer of task %', p_answer, p_task_id
      USING ERRCODE = '22023';
  END IF;
  IF p_mode = 'sample' THEN
    IF p_shown_run_id IS NOT NULL OR p_shown_answer IS NOT NULL THEN
      RAISE EXCEPTION 'a random-sample answer is blind: nothing may be shown'
        USING ERRCODE = '22023';
    END IF;
  ELSIF p_shown_run_id IS NULL
        OR NOT EXISTS (SELECT 1 FROM rat.model_run r
                        WHERE r.id = p_shown_run_id AND r.task_id = p_task_id) THEN
    RAISE EXCEPTION 'mode % needs the shown run of task %', p_mode, p_task_id
      USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM rat.catalog c WHERE c.image_no = p_image_no) THEN
    RAISE EXCEPTION 'no catalog photo %', p_image_no USING ERRCODE = 'P0002';
  END IF;

  INSERT INTO rat.model_answer
    (task_id, image_no, answer, shown_run_id, shown_answer, mode, answered_by)
  VALUES (p_task_id, p_image_no, p_answer, p_shown_run_id, p_shown_answer, p_mode,
          auth.uid())
  RETURNING id INTO v_answer_id;

  -- The Data Fixes link. No link, no flag on the answer, or no fields: nothing to do.
  IF v_task.links IS NOT NULL THEN
    v_flag := (v_answer ->> (v_task.links ->> 'no_answer_flag'))::boolean;
    -- In the task's order. A repeated fix_id can never satisfy the fill
    -- check below (fewer rows than ids), so a bad config fills nothing.
    v_fix_ids := ARRAY(SELECT x.fix_id
                         FROM jsonb_array_elements_text(
                                coalesce(v_task.links -> 'fix_ids', '[]'::jsonb))
                              WITH ORDINALITY AS x(fix_id, ord)
                        ORDER BY x.ord);
  END IF;

  IF v_flag IS NOT NULL AND cardinality(v_fix_ids) > 0 THEN
    -- Lock first, then decide on what is committed now.
    PERFORM 1
       FROM rat.data_fix_value v
      WHERE v.image_no = p_image_no AND v.fix_id = ANY (v_fix_ids)
      ORDER BY v.fix_id
        FOR UPDATE;

    IF v_flag = false THEN
      SELECT count(*) INTO v_n_fill
        FROM rat.data_fix_value v
       WHERE v.image_no = p_image_no AND v.fix_id = ANY (v_fix_ids)
         AND v.status = 'extracted' AND v.confidence = 'none';

      IF v_n_fill = cardinality(v_fix_ids) THEN
        PERFORM pg_catalog.set_config('rat.review_context', 'on', true);
        PERFORM pg_catalog.set_config('rat.review_source', 'model_check', true);
        UPDATE rat.data_fix_value v
           SET "values" = '{}'::text[],
               status = 'no_loco',
               reviewed_by = auth.uid(),
               reviewed_at = now(),
               needs_recheck = false,
               description_hash = pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                 coalesce((SELECT c.description FROM rat.catalog c
                            WHERE c.image_no = p_image_no), ''), 'UTF8')), 'hex')
         WHERE v.image_no = p_image_no AND v.fix_id = ANY (v_fix_ids)
           AND v.status = 'extracted' AND v.confidence = 'none';   -- never a reviewed row
        GET DIAGNOSTICS v_n_updated = ROW_COUNT;
        -- Close the window again: only this UPDATE runs in review context.
        PERFORM pg_catalog.set_config('rat.review_context', '', true);
        PERFORM pg_catalog.set_config('rat.review_source', '', true);
        IF v_n_updated <> v_n_fill THEN
          -- Cannot happen under the locks above; fail rather than fill part.
          RAISE EXCEPTION 'model check fill changed % of % rows for %',
            v_n_updated, v_n_fill, p_image_no;
        END IF;
        v_filled := v_fix_ids;
      ELSE
        SELECT coalesce(jsonb_agg(jsonb_build_object(
                 'fix_id', v.fix_id, 'status', v.status, 'confidence', v.confidence,
                 'values', to_jsonb(v."values"),
                 'suggested_values', to_jsonb(v.suggested_values))
                 ORDER BY pg_catalog.array_position(v_fix_ids, v.fix_id)), '[]'::jsonb)
          INTO v_conflicts
          FROM rat.data_fix_value v
         WHERE v.image_no = p_image_no AND v.fix_id = ANY (v_fix_ids)
           AND ((v.status <> 'extracted' AND cardinality(v."values") > 0)
                OR (v.status = 'extracted' AND v.confidence <> 'none'));
      END IF;
    ELSE
      SELECT coalesce(jsonb_agg(jsonb_build_object(
               'fix_id', v.fix_id, 'status', v.status, 'confidence', v.confidence,
               'values', to_jsonb(v."values"),
               'suggested_values', to_jsonb(v.suggested_values))
               ORDER BY pg_catalog.array_position(v_fix_ids, v.fix_id)), '[]'::jsonb)
        INTO v_conflicts
        FROM rat.data_fix_value v
       WHERE v.image_no = p_image_no AND v.fix_id = ANY (v_fix_ids)
         AND v.status = 'no_loco';
    END IF;
  END IF;

  RETURN jsonb_build_object('answer_id', v_answer_id,
                            'filled', to_jsonb(v_filled),
                            'conflicts', v_conflicts);
END;
$$;

-- Per task and run: photos predicted, checked (latest answer, any mode) and
-- agreeing with the run, and the blind sample: photos whose latest blind
-- ('sample') answer exists, and how many of those the run got right. The same
-- blind answers score every run of the task. Only photos the run predicted
-- count.
CREATE OR REPLACE FUNCTION rat.model_check_progress()
RETURNS TABLE (
  task_id text,
  run_id bigint,
  model text,
  version text,
  created_at timestamptz,
  n_predictions integer,
  n_checked bigint,
  n_agree bigint,
  n_blind bigint,
  n_blind_correct bigint
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
    WITH latest AS (
      SELECT DISTINCT ON (a.task_id, a.image_no) a.task_id AS t_id, a.image_no AS img, a.answer AS ans
        FROM rat.model_answer a
       ORDER BY a.task_id, a.image_no, a.answered_at DESC, a.id DESC
    ), latest_blind AS (
      SELECT DISTINCT ON (a.task_id, a.image_no) a.task_id AS t_id, a.image_no AS img, a.answer AS ans
        FROM rat.model_answer a
       WHERE a.mode = 'sample'
       ORDER BY a.task_id, a.image_no, a.answered_at DESC, a.id DESC
    )
    SELECT r.task_id, r.id, r.model, r.version, r.created_at, r.n_predictions,
           ck.n_checked, ck.n_agree, bl.n_blind, bl.n_blind_correct
      FROM rat.model_run r
     CROSS JOIN LATERAL (
       SELECT count(*) AS n_checked, count(*) FILTER (WHERE l.ans = p.answer) AS n_agree
         FROM latest l
         JOIN rat.model_prediction p ON p.run_id = r.id AND p.image_no = l.img
        WHERE l.t_id = r.task_id) ck
     CROSS JOIN LATERAL (
       SELECT count(*) AS n_blind, count(*) FILTER (WHERE l.ans = p.answer) AS n_blind_correct
         FROM latest_blind l
         JOIN rat.model_prediction p ON p.run_id = r.id AND p.image_no = l.img
        WHERE l.t_id = r.task_id) bl
     ORDER BY r.task_id, r.created_at DESC, r.id DESC;
END;
$$;

-- Model answer x checked answer for one run (latest blind answer per photo by
-- default; latest answer of any mode with p_blind_only = false).
CREATE OR REPLACE FUNCTION rat.model_check_confusion(
  p_run_id bigint,
  p_blind_only boolean DEFAULT true
)
RETURNS TABLE (model_answer text, checked_answer text, n bigint)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task_id text;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  SELECT r.task_id INTO v_task_id FROM rat.model_run r WHERE r.id = p_run_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model run %', p_run_id USING ERRCODE = 'P0002';
  END IF;
  RETURN QUERY
    WITH latest AS (
      SELECT DISTINCT ON (a.image_no) a.image_no AS img, a.answer AS ans
        FROM rat.model_answer a
       WHERE a.task_id = v_task_id
         AND (a.mode = 'sample' OR NOT coalesce(p_blind_only, true))
       ORDER BY a.image_no, a.answered_at DESC, a.id DESC
    )
    SELECT p.answer, l.ans, count(*)
      FROM latest l
      JOIN rat.model_prediction p ON p.run_id = p_run_id AND p.image_no = l.img
     GROUP BY p.answer, l.ans
     ORDER BY p.answer, l.ans;
END;
$$;

-- Photos that need a person (one row per photo and linked field): latest
-- answer not-a-loco while a linked field is still 'extracted' with a
-- description-derived value (confidence other than 'none'); or latest answer
-- a loco while a linked field is saved 'no_loco'. A not-a-loco photo whose
-- field a volunteer reviewed with a value is consistent (a loco can be in
-- the photo without being its main subject) and is not listed. Empty for a
-- task without a Data Fixes link.
CREATE OR REPLACE FUNCTION rat.model_check_conflicts(p_task_id text)
RETURNS TABLE (
  image_no text,
  answer text,
  fix_id text,
  status text,
  confidence text,
  "values" text[],
  suggested_values text[]
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task rat.model_task;
  v_fix_ids text[];
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model task %', p_task_id USING ERRCODE = 'P0002';
  END IF;
  IF v_task.links IS NULL THEN
    RETURN;
  END IF;
  v_fix_ids := ARRAY(SELECT x.fix_id
                       FROM jsonb_array_elements_text(
                              coalesce(v_task.links -> 'fix_ids', '[]'::jsonb))
                            WITH ORDINALITY AS x(fix_id, ord)
                      ORDER BY x.ord);
  RETURN QUERY
    WITH latest AS (
      SELECT DISTINCT ON (a.image_no) a.image_no AS img, a.answer AS ans
        FROM rat.model_answer a
       WHERE a.task_id = p_task_id
       ORDER BY a.image_no, a.answered_at DESC, a.id DESC
    ), flagged AS (
      SELECT l.img, l.ans, (e ->> (v_task.links ->> 'no_answer_flag'))::boolean AS is_loco
        FROM latest l
        JOIN LATERAL jsonb_array_elements(v_task.answers) AS e ON e ->> 'id' = l.ans
    )
    SELECT f.img, f.ans, v.fix_id, v.status, v.confidence, v."values", v.suggested_values
      FROM flagged f
      JOIN rat.data_fix_value v ON v.image_no = f.img AND v.fix_id = ANY (v_fix_ids)
     WHERE (f.is_loco = false AND v.status = 'extracted' AND v.confidence <> 'none')
        OR (f.is_loco = true AND v.status = 'no_loco')
     ORDER BY f.img, pg_catalog.array_position(v_fix_ids, v.fix_id);
END;
$$;

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

REVOKE EXECUTE ON FUNCTION
  rat.model_answer_append_only(),
  rat.model_check_queue(text, text, bigint, text, text, text, integer),
  rat.save_model_answer(text, text, text, bigint, text, text),
  rat.model_check_progress(),
  rat.model_check_confusion(bigint, boolean),
  rat.model_check_conflicts(text),
  rat.model_check_photo(text, text, bigint)
  FROM PUBLIC, anon;
REVOKE EXECUTE ON FUNCTION rat.model_answer_append_only() FROM authenticated;
GRANT EXECUTE ON FUNCTION
  rat.model_check_queue(text, text, bigint, text, text, text, integer),
  rat.save_model_answer(text, text, text, bigint, text, text),
  rat.model_check_progress(),
  rat.model_check_confusion(bigint, boolean),
  rat.model_check_conflicts(text),
  rat.model_check_photo(text, text, bigint)
  TO authenticated;

COMMIT;
