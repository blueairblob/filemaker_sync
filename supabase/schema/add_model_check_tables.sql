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
