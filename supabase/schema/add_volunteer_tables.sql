-- add_volunteer_tables.sql
--
-- "AI volunteer": a human volunteer role for the RAT photo archive, sitting
-- beside the Data Fixes admins. Volunteers review Data Fixes / Model check work;
-- a volunteer who is "in training" has their answers held for approval.
--
--   * rat.volunteer   one row per volunteer (active when removed_at IS NULL).
--                     RLS is on with NO policy: reached only through the
--                     SECURITY DEFINER functions below.
--
-- Functions (EXECUTE for authenticated): is_volunteer, is_trainee, can_review,
-- my_role, add_volunteer, set_volunteer, require_password_change,
-- password_changed, account_status, people. The admin ones raise 42501 for
-- non-admins.
--
-- One write path (second section): review_fix_value and save_model_answer
-- become thin wrappers over the internal rat._apply_fix_review /
-- rat._apply_model_answer (no EXECUTE for authenticated), which the approve
-- RPCs reuse later. Direct saves (those two plus confirm_fix_values) are open
-- to admins and non-trainee volunteers; a trainee gets 42501 'trainee work is
-- held for approval: use submit_*'. The queues/progress reads and the SELECT
-- policies on data_fix, data_fix_value, model_task and model_run are open to
-- rat.can_review(). data_fix_history and model_answer gain nullable columns
-- (approver, undo links, answer link, old flags) and the history source
-- CHECK gains 'undo'.
--
-- Held work (fourth section): a trainee's reviews and answers go into
-- rat.pending_fix_review / rat.pending_model_answer (RLS on, no policy, no
-- table grants) through submit_fix_review / submit_fix_confirm /
-- submit_model_answer. An admin applies them through the same internal write
-- path with approve_fix_review / approve_model_answer, or turns them down
-- with reject_held; held_list and my_held list them. A trainee's own pending
-- keys drop out of their own queues.
--
-- Apply to live AFTER add_model_check_tables.sql (the user applies it; agents
-- never run DDL on live oci). Requires rat.is_admin() / rat.admin_user and
-- auth.users / auth.uid().
--
-- Idempotent: safe to re-run (IF NOT EXISTS / CREATE OR REPLACE). Validated
-- against a local postgres:17 container by tests/test_volunteer_schema.py,
-- never the live DB.

BEGIN;

CREATE TABLE IF NOT EXISTS rat.volunteer (
  user_id              uuid PRIMARY KEY REFERENCES auth.users(id),
  in_training          boolean     NOT NULL DEFAULT true,
  must_change_password boolean     NOT NULL DEFAULT true,
  added_by             uuid,
  added_at             timestamptz NOT NULL DEFAULT now(),
  removed_by           uuid,
  removed_at           timestamptz
);

ALTER TABLE rat.volunteer ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON rat.volunteer FROM PUBLIC, anon, authenticated;

-- ---------------------------------------------------------------------------
-- Role checks
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION rat.is_volunteer()
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = ''
AS $$
  SELECT EXISTS (SELECT 1 FROM rat.volunteer v
                  WHERE v.user_id = auth.uid() AND v.removed_at IS NULL);
$$;

CREATE OR REPLACE FUNCTION rat.is_trainee()
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = ''
AS $$
  SELECT EXISTS (SELECT 1 FROM rat.volunteer v
                  WHERE v.user_id = auth.uid() AND v.removed_at IS NULL
                    AND v.in_training);
$$;

CREATE OR REPLACE FUNCTION rat.can_review()
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = ''
AS $$
  SELECT rat.is_admin() OR rat.is_volunteer();
$$;

CREATE OR REPLACE FUNCTION rat.my_role()
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = ''
AS $$
DECLARE
  v rat.volunteer;
BEGIN
  IF rat.is_admin() THEN
    RETURN jsonb_build_object('role', 'admin', 'in_training', NULL, 'must_change_password', NULL);
  END IF;
  SELECT * INTO v FROM rat.volunteer x
   WHERE x.user_id = auth.uid() AND x.removed_at IS NULL;
  IF FOUND THEN
    RETURN jsonb_build_object('role', 'volunteer', 'in_training', v.in_training,
                              'must_change_password', v.must_change_password);
  END IF;
  RETURN jsonb_build_object('role', NULL, 'in_training', NULL, 'must_change_password', NULL);
END;
$$;

-- ---------------------------------------------------------------------------
-- Admin management
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION rat.add_volunteer(p_user_id uuid)
RETURNS rat.volunteer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ''
AS $$
DECLARE
  v rat.volunteer;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  IF EXISTS (SELECT 1 FROM rat.admin_user a WHERE a.user_id = p_user_id) THEN
    RAISE EXCEPTION 'admins cannot be volunteers' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM auth.users u WHERE u.id = p_user_id) THEN
    RAISE EXCEPTION 'no such user' USING ERRCODE = 'P0002';
  END IF;
  SELECT * INTO v FROM rat.volunteer x WHERE x.user_id = p_user_id FOR UPDATE;
  IF FOUND THEN
    IF v.removed_at IS NULL THEN
      RAISE EXCEPTION 'already a volunteer' USING ERRCODE = '22023';
    END IF;
    UPDATE rat.volunteer x
       SET removed_by = NULL, removed_at = NULL, in_training = true,
           must_change_password = true, added_by = auth.uid(), added_at = now()
     WHERE x.user_id = p_user_id
     RETURNING * INTO v;
  ELSE
    INSERT INTO rat.volunteer (user_id, added_by)
    VALUES (p_user_id, auth.uid())
    RETURNING * INTO v;
  END IF;
  RETURN v;
END;
$$;

CREATE OR REPLACE FUNCTION rat.set_volunteer(
  p_user_id uuid,
  p_in_training boolean DEFAULT NULL,
  p_active boolean DEFAULT NULL
)
RETURNS rat.volunteer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ''
AS $$
DECLARE
  v rat.volunteer;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  UPDATE rat.volunteer x
     SET in_training = COALESCE(p_in_training, x.in_training),
         removed_by  = CASE WHEN p_active IS NULL THEN x.removed_by
                            WHEN p_active THEN NULL ELSE auth.uid() END,
         removed_at  = CASE WHEN p_active IS NULL THEN x.removed_at
                            WHEN p_active THEN NULL ELSE now() END
   WHERE x.user_id = p_user_id
   RETURNING * INTO v;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no such volunteer' USING ERRCODE = 'P0002';
  END IF;
  RETURN v;
END;
$$;

CREATE OR REPLACE FUNCTION rat.require_password_change(p_user_id uuid)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ''
AS $$
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  UPDATE rat.volunteer x SET must_change_password = true
   WHERE x.user_id = p_user_id AND x.removed_at IS NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no such active volunteer' USING ERRCODE = 'P0002';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION rat.password_changed()
RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = ''
AS $$
  UPDATE rat.volunteer x SET must_change_password = false
   WHERE x.user_id = auth.uid();
$$;

CREATE OR REPLACE FUNCTION rat.account_status(
  p_email text DEFAULT NULL,
  p_user_id uuid DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = ''
AS $$
DECLARE
  v_uid uuid;
  v_removed timestamptz;
  v_kind text;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  IF p_user_id IS NOT NULL THEN
    SELECT u.id INTO v_uid FROM auth.users u WHERE u.id = p_user_id;
  ELSIF p_email IS NOT NULL THEN
    SELECT u.id INTO v_uid FROM auth.users u
     WHERE lower(trim(u.email)) = lower(trim(p_email)) LIMIT 1;
  END IF;
  IF v_uid IS NULL THEN
    RETURN jsonb_build_object('user_id', NULL, 'kind', 'none');
  END IF;
  IF EXISTS (SELECT 1 FROM rat.admin_user a WHERE a.user_id = v_uid) THEN
    v_kind := 'admin';
  ELSIF EXISTS (SELECT 1 FROM rat.volunteer v WHERE v.user_id = v_uid AND v.removed_at IS NULL) THEN
    v_kind := 'volunteer';
  ELSIF EXISTS (SELECT 1 FROM rat.volunteer v WHERE v.user_id = v_uid) THEN
    v_kind := 'removed_volunteer';
  ELSE
    v_kind := 'other';
  END IF;
  RETURN jsonb_build_object('user_id', v_uid, 'kind', v_kind);
END;
$$;

-- held_count: the user's pending rows in rat.pending_fix_review and
-- rat.pending_model_answer (both created in the held-work section below;
-- plpgsql resolves them when it runs).
CREATE OR REPLACE FUNCTION rat.people()
RETURNS TABLE (user_id uuid, email text, role text, in_training boolean,
               added_at timestamptz, removed_at timestamptz, held_count bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = ''
AS $$
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    WITH held AS (
      SELECT f.submitted_by AS uid, count(*) AS n
        FROM rat.pending_fix_review f WHERE f.decision = 'pending' GROUP BY f.submitted_by
      UNION ALL
      SELECT m.submitted_by, count(*)
        FROM rat.pending_model_answer m WHERE m.decision = 'pending' GROUP BY m.submitted_by
    ), held_by AS (
      SELECT h.uid, sum(h.n)::bigint AS n FROM held h GROUP BY h.uid
    )
    SELECT a.user_id, u.email::text, 'admin'::text, NULL::boolean,
           NULL::timestamptz, NULL::timestamptz, coalesce(hb.n, 0)::bigint
      FROM rat.admin_user a
      LEFT JOIN auth.users u ON u.id = a.user_id
      LEFT JOIN held_by hb ON hb.uid = a.user_id
    UNION ALL
    SELECT v.user_id, u.email::text, 'volunteer'::text, v.in_training,
           v.added_at, v.removed_at, coalesce(hb.n, 0)::bigint
      FROM rat.volunteer v
      LEFT JOIN auth.users u ON u.id = v.user_id
      LEFT JOIN held_by hb ON hb.uid = v.user_id;
END;
$$;

REVOKE EXECUTE ON FUNCTION
  rat.is_volunteer(), rat.is_trainee(), rat.can_review(), rat.my_role(),
  rat.add_volunteer(uuid), rat.set_volunteer(uuid, boolean, boolean),
  rat.require_password_change(uuid), rat.password_changed(),
  rat.account_status(text, uuid), rat.people()
  FROM PUBLIC, anon;

GRANT EXECUTE ON FUNCTION
  rat.is_volunteer(), rat.is_trainee(), rat.can_review(), rat.my_role(),
  rat.add_volunteer(uuid), rat.set_volunteer(uuid, boolean, boolean),
  rat.require_password_change(uuid), rat.password_changed(),
  rat.account_status(text, uuid), rat.people()
  TO authenticated;

-- ===========================================================================
-- One write path for reviews and answers; volunteers can review
-- ===========================================================================
--
-- The functions below are redefined from their current text in
-- add_data_fix_tables.sql / add_model_check_tables.sql (which already hold
-- every fix_*.sql change). Signatures and result shapes are unchanged. This
-- file must run after those two: re-running either of them restores the
-- admin-only versions until this file is applied again.
--
-- rat.data_fix_value_guard is NOT touched: a reviewed value still changes
-- only inside a review-context window.

-- New nullable columns, no defaults (additive only). data_fix_history and
-- model_answer stay append-only.
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS approved_by uuid;
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS undoes bigint;
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS undoes_from bigint;
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS model_answer_id bigint;
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS old_needs_recheck boolean;
ALTER TABLE rat.data_fix_history ADD COLUMN IF NOT EXISTS old_description_hash text;
ALTER TABLE rat.model_answer ADD COLUMN IF NOT EXISTS approved_by uuid;
ALTER TABLE rat.model_answer ADD COLUMN IF NOT EXISTS approved_at timestamptz;

-- Drop every CHECK on the history table that mentions source, whatever its
-- name (fix_data_fix_model_check.sql's pattern), then add the one with 'undo'.
DO $$
DECLARE c record;
BEGIN
  FOR c IN
    SELECT conname FROM pg_constraint
     WHERE conrelid = 'rat.data_fix_history'::regclass
       AND contype = 'c'
       AND pg_get_constraintdef(oid) ILIKE '%source%'
  LOOP
    EXECUTE format('ALTER TABLE rat.data_fix_history DROP CONSTRAINT %I', c.conname);
  END LOOP;
END $$;

ALTER TABLE rat.data_fix_history
  ADD CONSTRAINT data_fix_history_source_check
    CHECK (source IN ('extractor', 'review', 'model_check', 'undo'));

-- History trigger. Inside a review window the write path may name the author
-- (rat.acting_for: a trainee whose held work an admin approves) and the
-- approver (rat.approved_by), the answer whose save made a fill
-- (rat.model_answer_id), and for an undo the rows it reverses (rat.undoes,
-- rat.undoes_from; source 'undo' when rat.review_source = 'undo'). An empty
-- or unset setting means NULL; changed_by falls back to auth.uid() as before.
-- On UPDATE the old needs_recheck/description_hash are kept so an undo can
-- restore them.
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
      (image_no, fix_id, old_values, new_values, old_status, new_status, changed_by, source,
       approved_by, model_answer_id, undoes, undoes_from, old_needs_recheck, old_description_hash)
    VALUES
      (NEW.image_no, NEW.fix_id,
       CASE WHEN TG_OP = 'UPDATE' THEN OLD."values" END, NEW."values",
       CASE WHEN TG_OP = 'UPDATE' THEN OLD.status END, NEW.status,
       coalesce(nullif(current_setting('rat.acting_for', true), '')::uuid, auth.uid()),
       CASE WHEN coalesce(current_setting('rat.review_source', true), '') = 'undo' THEN 'undo'
            WHEN coalesce(current_setting('rat.review_context', true), '') = 'on'
            THEN CASE WHEN coalesce(current_setting('rat.review_source', true), '') = 'model_check'
                      THEN 'model_check' ELSE 'review' END
            ELSE 'extractor' END,
       nullif(current_setting('rat.approved_by', true), '')::uuid,
       nullif(current_setting('rat.model_answer_id', true), '')::bigint,
       nullif(current_setting('rat.undoes', true), '')::bigint,
       nullif(current_setting('rat.undoes_from', true), '')::bigint,
       CASE WHEN TG_OP = 'UPDATE' THEN OLD.needs_recheck END,
       CASE WHEN TG_OP = 'UPDATE' THEN OLD.description_hash END);
  END IF;
  RETURN NULL;
END;
$$;

-- The one write path for a Data Fixes review: review_fix_value's former body,
-- with the reviewer passed in (p_by) and the approver recorded. Internal: no
-- EXECUTE for authenticated; called by review_fix_value and (later) the
-- approve RPCs.
CREATE OR REPLACE FUNCTION rat._apply_fix_review(
  p_image_no text,
  p_fix_id text,
  p_values text[],
  p_status text,
  p_expected_updated_at timestamptz,
  p_by uuid,
  p_approved_by uuid
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
  PERFORM pg_catalog.set_config('rat.acting_for', coalesce(p_by::text, ''), true);
  PERFORM pg_catalog.set_config('rat.approved_by', coalesce(p_approved_by::text, ''), true);
  UPDATE rat.data_fix_value v
     SET "values" = CASE WHEN p_status IN ('none_visible', 'no_loco') THEN '{}'::text[]
                         ELSE coalesce(p_values, '{}'::text[]) END,
         status = p_status,
         reviewed_by = p_by,
         reviewed_at = now(),
         needs_recheck = false,
         description_hash = pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
           coalesce((SELECT c.description FROM rat.catalog c
                      WHERE c.image_no = p_image_no), ''), 'UTF8')), 'hex')
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id
  RETURNING v.* INTO v_row;
  -- Close the window again: only this UPDATE runs in review context.
  PERFORM pg_catalog.set_config('rat.review_context', '', true);
  PERFORM pg_catalog.set_config('rat.acting_for', '', true);
  PERFORM pg_catalog.set_config('rat.approved_by', '', true);

  RETURN v_row;
END;
$$;

-- Admin or non-trainee volunteer. A trainee's work is held instead.
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
BEGIN
  IF NOT rat.is_admin() THEN
    IF rat.is_trainee() THEN
      RAISE EXCEPTION 'trainee work is held for approval: use submit_*' USING ERRCODE = '42501';
    ELSIF NOT rat.is_volunteer() THEN
      RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
    END IF;
  END IF;
  RETURN rat._apply_fix_review(p_image_no, p_fix_id, p_values, p_status,
                               p_expected_updated_at, auth.uid(), NULL);
END;
$$;

-- Keeps its own set-based UPDATE (reviewed_by = auth.uid()); only the access
-- check changes.
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
    IF rat.is_trainee() THEN
      RAISE EXCEPTION 'trainee work is held for approval: use submit_*' USING ERRCODE = '42501';
    ELSIF NOT rat.is_volunteer() THEN
      RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
    END IF;
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

-- The checks every Model check answer passes, direct or held: task exists
-- (P0002), mode (22023), answer is one of the task's (22023), a sample answer
-- is blind and any other names a run of the task (22023), photo exists
-- (P0002). Internal: no EXECUTE for authenticated; called by
-- _apply_model_answer and submit_model_answer.
CREATE OR REPLACE FUNCTION rat._validate_model_answer(
  p_task_id text,
  p_image_no text,
  p_answer text,
  p_shown_run_id bigint,
  p_shown_answer text,
  p_mode text
)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task rat.model_task;
BEGIN
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no model task %', p_task_id USING ERRCODE = 'P0002';
  END IF;
  IF p_mode IS NULL OR p_mode NOT IN ('sure', 'unsure', 'sample') THEN
    RAISE EXCEPTION 'invalid mode: %', p_mode USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_task.answers) AS e
                  WHERE e ->> 'id' = p_answer) THEN
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
END;
$$;

-- The one write path for a Model check answer: save_model_answer's former
-- body, with the author (p_by), approver and answer time passed in. The
-- Data Fixes fill records the answer id in history. Internal: no EXECUTE for
-- authenticated.
CREATE OR REPLACE FUNCTION rat._apply_model_answer(
  p_task_id text,
  p_image_no text,
  p_answer text,
  p_shown_run_id bigint,
  p_shown_answer text,
  p_mode text,
  p_by uuid,
  p_approved_by uuid,
  p_answered_at timestamptz
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
  PERFORM rat._validate_model_answer(p_task_id, p_image_no, p_answer, p_shown_run_id,
                                     p_shown_answer, p_mode);
  SELECT t.* INTO v_task FROM rat.model_task t WHERE t.id = p_task_id;
  SELECT e INTO v_answer
    FROM jsonb_array_elements(v_task.answers) AS e
   WHERE e ->> 'id' = p_answer
   LIMIT 1;

  INSERT INTO rat.model_answer
    (task_id, image_no, answer, shown_run_id, shown_answer, mode, answered_by,
     answered_at, approved_by, approved_at)
  VALUES (p_task_id, p_image_no, p_answer, p_shown_run_id, p_shown_answer, p_mode,
          p_by, coalesce(p_answered_at, now()), p_approved_by,
          CASE WHEN p_approved_by IS NOT NULL THEN now() END)
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
        PERFORM pg_catalog.set_config('rat.acting_for', coalesce(p_by::text, ''), true);
        PERFORM pg_catalog.set_config('rat.approved_by', coalesce(p_approved_by::text, ''), true);
        PERFORM pg_catalog.set_config('rat.model_answer_id', v_answer_id::text, true);
        UPDATE rat.data_fix_value v
           SET "values" = '{}'::text[],
               status = 'no_loco',
               reviewed_by = p_by,
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
        PERFORM pg_catalog.set_config('rat.acting_for', '', true);
        PERFORM pg_catalog.set_config('rat.approved_by', '', true);
        PERFORM pg_catalog.set_config('rat.model_answer_id', '', true);
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

-- Admin or non-trainee volunteer. A trainee's work is held instead.
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
BEGIN
  IF NOT rat.is_admin() THEN
    IF rat.is_trainee() THEN
      RAISE EXCEPTION 'trainee work is held for approval: use submit_*' USING ERRCODE = '42501';
    ELSIF NOT rat.is_volunteer() THEN
      RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
    END IF;
  END IF;
  RETURN rat._apply_model_answer(p_task_id, p_image_no, p_answer, p_shown_run_id,
                                 p_shown_answer, p_mode, auth.uid(), NULL, NULL);
END;
$$;

-- Reads open to every reviewer (admin or active volunteer, trainee too).
-- model_check_photo, model_check_confusion, model_check_conflicts and
-- data_fix_orphans stay admin-only.
CREATE OR REPLACE FUNCTION rat.data_fix_progress()
RETURNS TABLE (fix_id text, status text, confidence text, needs_recheck boolean, n bigint)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  IF NOT rat.can_review() THEN
    RAISE EXCEPTION 'not a data fixes reviewer' USING ERRCODE = '42501';
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
  IF NOT rat.can_review() THEN
    RAISE EXCEPTION 'not a data fixes reviewer' USING ERRCODE = '42501';
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
       -- A trainee's own held review keeps the photo out of their queue.
       AND NOT EXISTS (SELECT 1 FROM rat.pending_fix_review h
                        WHERE h.submitted_by = auth.uid() AND h.image_no = v.image_no
                          AND h.fix_id = v.fix_id AND h.decision = 'pending')
     ORDER BY v.image_no
     LIMIT least(greatest(coalesce(p_limit, 25), 1), 500);
END;
$$;

-- -----------------------------------------------------------------------------
-- Withdrawn answers (third section). A withdrawal is a row in its own
-- append-only table, so rat.model_answer itself is never touched. Everything
-- that counts answers reads rat.model_answer_live: answers with no withdrawal.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rat.model_answer_withdrawal (
  answer_id bigint PRIMARY KEY REFERENCES rat.model_answer(id),
  withdrawn_by uuid NOT NULL,
  withdrawn_at timestamptz NOT NULL DEFAULT now(),
  note text
);

DROP TRIGGER IF EXISTS model_answer_withdrawal_no_change ON rat.model_answer_withdrawal;
CREATE TRIGGER model_answer_withdrawal_no_change
  BEFORE UPDATE OR DELETE ON rat.model_answer_withdrawal
  FOR EACH ROW EXECUTE FUNCTION rat.model_answer_append_only();

DROP TRIGGER IF EXISTS model_answer_withdrawal_no_truncate ON rat.model_answer_withdrawal;
CREATE TRIGGER model_answer_withdrawal_no_truncate
  BEFORE TRUNCATE ON rat.model_answer_withdrawal
  FOR EACH STATEMENT EXECUTE FUNCTION rat.model_answer_append_only();

ALTER TABLE rat.model_answer_withdrawal ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE VIEW rat.model_answer_live AS
  SELECT a.* FROM rat.model_answer a
   WHERE NOT EXISTS (SELECT 1 FROM rat.model_answer_withdrawal w WHERE w.answer_id = a.id);

REVOKE ALL ON rat.model_answer_withdrawal, rat.model_answer_live
  FROM PUBLIC, anon, authenticated;

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
  IF NOT rat.can_review() THEN
    RAISE EXCEPTION 'not a data fixes reviewer' USING ERRCODE = '42501';
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
         AND NOT EXISTS (SELECT 1 FROM rat.model_answer_live a
                          WHERE a.task_id = p_task_id AND a.image_no = c.image_no
                            AND a.mode = 'sample')
         AND NOT EXISTS (SELECT 1 FROM rat.pending_model_answer h
                          WHERE h.submitted_by = auth.uid() AND h.task_id = p_task_id
                            AND h.image_no = c.image_no AND h.mode = 'sample'
                            AND h.decision = 'pending')
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
         AND NOT EXISTS (SELECT 1 FROM rat.model_answer_live a
                          WHERE a.task_id = p_task_id AND a.image_no = pr.image_no)
         AND NOT EXISTS (SELECT 1 FROM rat.pending_model_answer h
                          WHERE h.submitted_by = auth.uid() AND h.task_id = p_task_id
                            AND h.image_no = pr.image_no AND h.mode = p_mode
                            AND h.decision = 'pending')
       ORDER BY pr.image_no
       LIMIT v_limit;
  END IF;
END;
$$;

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
  IF NOT rat.can_review() THEN
    RAISE EXCEPTION 'not a data fixes reviewer' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    WITH latest AS (
      SELECT DISTINCT ON (a.task_id, a.image_no) a.task_id AS t_id, a.image_no AS img, a.answer AS ans
        FROM rat.model_answer_live a
       ORDER BY a.task_id, a.image_no, a.answered_at DESC, a.id DESC
    ), latest_blind AS (
      SELECT DISTINCT ON (a.task_id, a.image_no) a.task_id AS t_id, a.image_no AS img, a.answer AS ans
        FROM rat.model_answer_live a
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

-- The admin-only reads, moved onto the live view (nothing else changed).
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
        FROM rat.model_answer_live a
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
        FROM rat.model_answer_live a
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
  v_last rat.model_answer_live%ROWTYPE;
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
  SELECT a.* INTO v_last FROM rat.model_answer_live a
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

-- Direct table reads (dataFixService.ts / modelCheckService.ts): same policy
-- names as before, now open to reviewers. data_fix_history, admin_user,
-- model_prediction and model_answer stay closed to volunteers.
DROP POLICY IF EXISTS data_fix_admin_select ON rat.data_fix;
CREATE POLICY data_fix_admin_select ON rat.data_fix
  FOR SELECT TO authenticated USING (rat.can_review());

DROP POLICY IF EXISTS data_fix_value_admin_select ON rat.data_fix_value;
CREATE POLICY data_fix_value_admin_select ON rat.data_fix_value
  FOR SELECT TO authenticated USING (rat.can_review());

DROP POLICY IF EXISTS model_task_admin_select ON rat.model_task;
CREATE POLICY model_task_admin_select ON rat.model_task
  FOR SELECT TO authenticated USING (rat.can_review());

DROP POLICY IF EXISTS model_run_admin_select ON rat.model_run;
CREATE POLICY model_run_admin_select ON rat.model_run
  FOR SELECT TO authenticated USING (rat.can_review());

-- The internal write path is callable only by its owner (and so by the
-- SECURITY DEFINER RPCs that wrap it).
REVOKE ALL ON FUNCTION
  rat._apply_fix_review(text, text, text[], text, timestamptz, uuid, uuid),
  rat._apply_model_answer(text, text, text, bigint, text, text, uuid, uuid, timestamptz)
  FROM PUBLIC, anon, authenticated;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'picaloco_agent') THEN
    REVOKE ALL ON FUNCTION
      rat._apply_fix_review(text, text, text[], text, timestamptz, uuid, uuid),
      rat._apply_model_answer(text, text, text, bigint, text, text, uuid, uuid, timestamptz)
      FROM picaloco_agent;
  END IF;
END $$;

-- CREATE OR REPLACE keeps grants; restated so this file alone is enough.
REVOKE EXECUTE ON FUNCTION
  rat.review_fix_value(text, text, text[], text, timestamptz),
  rat.confirm_fix_values(text, jsonb),
  rat.save_model_answer(text, text, text, bigint, text, text),
  rat.data_fix_progress(),
  rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer),
  rat.model_check_queue(text, text, bigint, text, text, text, integer),
  rat.model_check_progress(),
  rat.data_fix_value_history()
  FROM PUBLIC, anon;
REVOKE EXECUTE ON FUNCTION rat.data_fix_value_history() FROM authenticated;
GRANT EXECUTE ON FUNCTION
  rat.review_fix_value(text, text, text[], text, timestamptz),
  rat.confirm_fix_values(text, jsonb),
  rat.save_model_answer(text, text, text, bigint, text, text),
  rat.data_fix_progress(),
  rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer),
  rat.model_check_queue(text, text, bigint, text, text, text, integer),
  rat.model_check_progress()
  TO authenticated;

-- ===========================================================================
-- Held work (fourth section): a trainee's reviews and answers wait here until
-- an admin approves (applied through _apply_fix_review / _apply_model_answer,
-- author = the trainee, approver = the admin) or rejects them. A held row is a
-- record: it is never deleted, only decided. Nothing in this section writes
-- rat.model_answer or rat.data_fix_value except through the write path.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS rat.pending_fix_review (
  id bigserial PRIMARY KEY,
  image_no text NOT NULL,
  fix_id text NOT NULL REFERENCES rat.data_fix(id),
  "values" text[] NOT NULL,
  status text NOT NULL CHECK (status IN ('verified', 'corrected', 'none_visible', 'no_loco')),
  base_updated_at timestamptz NOT NULL,
  base_values text[],
  base_status text,
  submitted_by uuid NOT NULL,
  submitted_at timestamptz NOT NULL DEFAULT now(),
  decision text NOT NULL DEFAULT 'pending'
    CHECK (decision IN ('pending', 'approved', 'rejected', 'withdrawn')),
  decided_by uuid,
  decided_at timestamptz,
  note text
);

CREATE UNIQUE INDEX IF NOT EXISTS pending_fix_review_one_pending_idx
  ON rat.pending_fix_review (submitted_by, image_no, fix_id) WHERE decision = 'pending';

CREATE TABLE IF NOT EXISTS rat.pending_model_answer (
  id bigserial PRIMARY KEY,
  task_id text NOT NULL REFERENCES rat.model_task(id),
  image_no text NOT NULL,
  answer text NOT NULL,
  shown_run_id bigint REFERENCES rat.model_run(id),
  shown_answer text,
  mode text NOT NULL CHECK (mode IN ('sure', 'unsure', 'sample')),
  submitted_by uuid NOT NULL,
  submitted_at timestamptz NOT NULL DEFAULT now(),
  decision text NOT NULL DEFAULT 'pending'
    CHECK (decision IN ('pending', 'approved', 'rejected', 'withdrawn')),
  decided_by uuid,
  decided_at timestamptz,
  note text,
  -- A blind sample answer must not record what the model said (as model_answer).
  CHECK (mode <> 'sample' OR (shown_run_id IS NULL AND shown_answer IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS pending_model_answer_one_pending_idx
  ON rat.pending_model_answer (submitted_by, task_id, image_no, mode)
  WHERE decision = 'pending';

ALTER TABLE rat.pending_fix_review ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.pending_model_answer ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON rat.pending_fix_review, rat.pending_model_answer
  FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE rat.pending_fix_review_id_seq, rat.pending_model_answer_id_seq
  FROM PUBLIC, anon, authenticated;

-- Submit (trainee only). Validation matches review_fix_value: status list
-- (22023), row exists (P0002), row unchanged since loaded (PT409). A
-- re-submit of the same key withdraws the caller's pending row first.
CREATE OR REPLACE FUNCTION rat.submit_fix_review(
  p_image_no text,
  p_fix_id text,
  p_values text[],
  p_status text,
  p_expected_updated_at timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_live rat.data_fix_value;
  v_id bigint;
BEGIN
  IF NOT rat.is_trainee() THEN
    RAISE EXCEPTION 'only a volunteer in training submits work for approval'
      USING ERRCODE = '42501';
  END IF;
  IF p_status IS NULL
     OR p_status NOT IN ('verified', 'corrected', 'none_visible', 'no_loco') THEN
    RAISE EXCEPTION 'invalid review status: %', p_status USING ERRCODE = '22023';
  END IF;
  SELECT v.* INTO v_live FROM rat.data_fix_value v
   WHERE v.image_no = p_image_no AND v.fix_id = p_fix_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no data_fix_value for image_no=% fix_id=%', p_image_no, p_fix_id
      USING ERRCODE = 'P0002';
  END IF;
  IF v_live.updated_at IS DISTINCT FROM p_expected_updated_at THEN
    RAISE EXCEPTION 'record changed since it was loaded' USING ERRCODE = 'PT409';
  END IF;

  UPDATE rat.pending_fix_review h
     SET decision = 'withdrawn', decided_by = auth.uid(), decided_at = now()
   WHERE h.submitted_by = auth.uid() AND h.image_no = p_image_no
     AND h.fix_id = p_fix_id AND h.decision = 'pending';
  INSERT INTO rat.pending_fix_review
    (image_no, fix_id, "values", status, base_updated_at, base_values, base_status,
     submitted_by)
  VALUES (p_image_no, p_fix_id,
          CASE WHEN p_status IN ('none_visible', 'no_loco') THEN '{}'::text[]
               ELSE coalesce(p_values, '{}'::text[]) END,
          p_status, v_live.updated_at, v_live."values", v_live.status, auth.uid())
  RETURNING id INTO v_id;
  RETURN v_id;
END;
$$;

-- Mirrors confirm_fix_values: one held 'verified' row (values = the
-- suggestion) per item whose row is still 'extracted' and unchanged. Stale or
-- reviewed items are simply not counted.
CREATE OR REPLACE FUNCTION rat.submit_fix_confirm(p_fix_id text, p_items jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  r record;
  v_count integer := 0;
BEGIN
  IF NOT rat.is_trainee() THEN
    RAISE EXCEPTION 'only a volunteer in training submits work for approval'
      USING ERRCODE = '42501';
  END IF;
  FOR r IN
    SELECT v.image_no, v.updated_at, v."values", v.status, v.suggested_values
      FROM rat.data_fix_value v
      JOIN (SELECT DISTINCT i.image_no, i.updated_at
              FROM pg_catalog.jsonb_to_recordset(coalesce(p_items, '[]'::jsonb))
                   AS i(image_no text, updated_at timestamptz)) AS item
        ON v.image_no = item.image_no AND v.updated_at = item.updated_at
     WHERE v.fix_id = p_fix_id AND v.status = 'extracted'
     ORDER BY v.image_no
  LOOP
    UPDATE rat.pending_fix_review h
       SET decision = 'withdrawn', decided_by = auth.uid(), decided_at = now()
     WHERE h.submitted_by = auth.uid() AND h.image_no = r.image_no
       AND h.fix_id = p_fix_id AND h.decision = 'pending';
    INSERT INTO rat.pending_fix_review
      (image_no, fix_id, "values", status, base_updated_at, base_values, base_status,
       submitted_by)
    VALUES (r.image_no, p_fix_id, r.suggested_values, 'verified', r.updated_at,
            r."values", r.status, auth.uid());
    v_count := v_count + 1;
  END LOOP;
  RETURN v_count;
END;
$$;

-- Validation matches save_model_answer (rat._validate_model_answer).
CREATE OR REPLACE FUNCTION rat.submit_model_answer(
  p_task_id text,
  p_image_no text,
  p_answer text,
  p_shown_run_id bigint,
  p_shown_answer text,
  p_mode text
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_id bigint;
BEGIN
  IF NOT rat.is_trainee() THEN
    RAISE EXCEPTION 'only a volunteer in training submits work for approval'
      USING ERRCODE = '42501';
  END IF;
  PERFORM rat._validate_model_answer(p_task_id, p_image_no, p_answer, p_shown_run_id,
                                     p_shown_answer, p_mode);
  UPDATE rat.pending_model_answer h
     SET decision = 'withdrawn', decided_by = auth.uid(), decided_at = now()
   WHERE h.submitted_by = auth.uid() AND h.task_id = p_task_id
     AND h.image_no = p_image_no AND h.mode = p_mode AND h.decision = 'pending';
  INSERT INTO rat.pending_model_answer
    (task_id, image_no, answer, shown_run_id, shown_answer, mode, submitted_by)
  VALUES (p_task_id, p_image_no, p_answer, p_shown_run_id, p_shown_answer, p_mode,
          auth.uid())
  RETURNING id INTO v_id;
  RETURN v_id;
END;
$$;

-- Admin decisions. Each id is handled on its own, in id order, inside its own
-- subtransaction: an error in one item becomes a 'skipped' (or 'stale') entry
-- and never fails the call or the other items.
CREATE OR REPLACE FUNCTION rat.approve_fix_review(p_ids bigint[], p_force boolean DEFAULT false)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_id bigint;
  r rat.pending_fix_review;
  v_live rat.data_fix_value;
  v_expected timestamptz;
  v_approved integer := 0;
  v_stale jsonb := '[]'::jsonb;
  v_skipped jsonb := '[]'::jsonb;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  FOR v_id IN SELECT DISTINCT x FROM unnest(coalesce(p_ids, '{}'::bigint[])) AS x
               WHERE x IS NOT NULL ORDER BY x
  LOOP
    BEGIN
      SELECT h.* INTO r FROM rat.pending_fix_review h WHERE h.id = v_id FOR UPDATE;
      IF NOT FOUND OR r.decision <> 'pending' THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', 'not pending');
      ELSE
        IF coalesce(p_force, false) THEN
          -- The admin saw the current value: approve over it.
          SELECT v.updated_at INTO v_expected FROM rat.data_fix_value v
           WHERE v.image_no = r.image_no AND v.fix_id = r.fix_id;
        ELSE
          v_expected := r.base_updated_at;
        END IF;
        PERFORM rat._apply_fix_review(r.image_no, r.fix_id, r."values", r.status,
                                      v_expected, r.submitted_by, auth.uid());
        UPDATE rat.pending_fix_review h
           SET decision = 'approved', decided_by = auth.uid(), decided_at = now()
         WHERE h.id = v_id;
        v_approved := v_approved + 1;
      END IF;
    EXCEPTION
      WHEN SQLSTATE 'PT409' THEN
        SELECT v.* INTO v_live FROM rat.data_fix_value v
         WHERE v.image_no = r.image_no AND v.fix_id = r.fix_id;
        v_stale := v_stale || jsonb_build_object('id', v_id,
                                                 'current_values', to_jsonb(v_live."values"),
                                                 'current_status', v_live.status);
      WHEN no_data_found THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', 'no data_fix_value');
      WHEN OTHERS THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', SQLERRM);
    END;
  END LOOP;
  RETURN jsonb_build_object('approved', v_approved, 'stale', v_stale, 'skipped', v_skipped);
END;
$$;

-- Answers are never stale: a later answer simply becomes the latest.
CREATE OR REPLACE FUNCTION rat.approve_model_answer(p_ids bigint[])
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_id bigint;
  r rat.pending_model_answer;
  v_approved integer := 0;
  v_skipped jsonb := '[]'::jsonb;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  FOR v_id IN SELECT DISTINCT x FROM unnest(coalesce(p_ids, '{}'::bigint[])) AS x
               WHERE x IS NOT NULL ORDER BY x
  LOOP
    BEGIN
      SELECT h.* INTO r FROM rat.pending_model_answer h WHERE h.id = v_id FOR UPDATE;
      IF NOT FOUND OR r.decision <> 'pending' THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', 'not pending');
      ELSE
        PERFORM rat._apply_model_answer(r.task_id, r.image_no, r.answer, r.shown_run_id,
                                        r.shown_answer, r.mode, r.submitted_by, auth.uid(),
                                        r.submitted_at);
        UPDATE rat.pending_model_answer h
           SET decision = 'approved', decided_by = auth.uid(), decided_at = now()
         WHERE h.id = v_id;
        v_approved := v_approved + 1;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', SQLERRM);
    END;
  END LOOP;
  RETURN jsonb_build_object('approved', v_approved, 'skipped', v_skipped);
END;
$$;

-- Nothing reaches the real tables. A rejection is final (no undo).
CREATE OR REPLACE FUNCTION rat.reject_held(p_kind text, p_ids bigint[], p_note text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_id bigint;
  v_decision text;
  v_rejected integer := 0;
  v_skipped jsonb := '[]'::jsonb;
BEGIN
  IF NOT rat.is_admin() THEN
    RAISE EXCEPTION 'not a data fixes admin' USING ERRCODE = '42501';
  END IF;
  IF p_kind IS NULL OR p_kind NOT IN ('fix', 'answer') THEN
    RAISE EXCEPTION 'invalid held kind: %', p_kind USING ERRCODE = '22023';
  END IF;
  FOR v_id IN SELECT DISTINCT x FROM unnest(coalesce(p_ids, '{}'::bigint[])) AS x
               WHERE x IS NOT NULL ORDER BY x
  LOOP
    BEGIN
      v_decision := NULL;
      IF p_kind = 'fix' THEN
        SELECT h.decision INTO v_decision FROM rat.pending_fix_review h
         WHERE h.id = v_id FOR UPDATE;
      ELSE
        SELECT h.decision INTO v_decision FROM rat.pending_model_answer h
         WHERE h.id = v_id FOR UPDATE;
      END IF;
      IF v_decision IS DISTINCT FROM 'pending' THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', 'not pending');
      ELSE
        IF p_kind = 'fix' THEN
          UPDATE rat.pending_fix_review h
             SET decision = 'rejected', decided_by = auth.uid(), decided_at = now(),
                 note = p_note
           WHERE h.id = v_id;
        ELSE
          UPDATE rat.pending_model_answer h
             SET decision = 'rejected', decided_by = auth.uid(), decided_at = now(),
                 note = p_note
           WHERE h.id = v_id;
        END IF;
        v_rejected := v_rejected + 1;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        v_skipped := v_skipped || jsonb_build_object('id', v_id, 'reason', SQLERRM);
    END;
  END LOOP;
  RETURN jsonb_build_object('rejected', v_rejected, 'skipped', v_skipped);
END;
$$;

-- Admin: every pending row (or one user's), oldest first. A fix is stale when
-- its live row changed (or vanished) since the trainee loaded it.
CREATE OR REPLACE FUNCTION rat.held_list(p_user uuid DEFAULT NULL)
RETURNS TABLE (
  kind text,
  id bigint,
  submitted_by uuid,
  submitted_at timestamptz,
  image_no text,
  description text,
  subject text,
  before jsonb,
  proposed jsonb,
  current jsonb,
  stale boolean
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
    SELECT hl.k, hl.hid, hl.sb, hl.sa, hl.img, hl.descr, hl.subj, hl.bef, hl.prop,
           hl.cur, hl.st
      FROM (
        SELECT 'fix'::text AS k, f.id AS hid, f.submitted_by AS sb, f.submitted_at AS sa,
               f.image_no AS img, c.description AS descr, d.title AS subj,
               jsonb_build_object('values', to_jsonb(f.base_values), 'status', f.base_status) AS bef,
               jsonb_build_object('values', to_jsonb(f."values"), 'status', f.status) AS prop,
               CASE WHEN v.image_no IS NULL THEN NULL::jsonb
                    ELSE jsonb_build_object('values', to_jsonb(v."values"), 'status', v.status)
               END AS cur,
               (v.updated_at IS DISTINCT FROM f.base_updated_at) AS st
          FROM rat.pending_fix_review f
          LEFT JOIN rat.data_fix_value v ON v.image_no = f.image_no AND v.fix_id = f.fix_id
          LEFT JOIN rat.catalog c ON c.image_no = f.image_no
          LEFT JOIN rat.data_fix d ON d.id = f.fix_id
         WHERE f.decision = 'pending' AND (p_user IS NULL OR f.submitted_by = p_user)
        UNION ALL
        SELECT 'answer'::text, m.id, m.submitted_by, m.submitted_at,
               m.image_no, c.description, t.question,
               jsonb_build_object('shown_answer', m.shown_answer),
               jsonb_build_object('answer', m.answer, 'mode', m.mode),
               NULL::jsonb, false
          FROM rat.pending_model_answer m
          LEFT JOIN rat.catalog c ON c.image_no = m.image_no
          LEFT JOIN rat.model_task t ON t.id = m.task_id
         WHERE m.decision = 'pending' AND (p_user IS NULL OR m.submitted_by = p_user)
      ) hl
     ORDER BY hl.sa, hl.k DESC, hl.hid;
END;
$$;

-- Any signed-in caller: their own pending and rejected rows, newest first.
CREATE OR REPLACE FUNCTION rat.my_held()
RETURNS TABLE (
  kind text,
  id bigint,
  image_no text,
  subject text,
  proposed jsonb,
  decision text,
  decided_at timestamptz,
  note text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  RETURN QUERY
    SELECT hl.k, hl.hid, hl.img, hl.subj, hl.prop, hl.dec, hl.dat, hl.nt
      FROM (
        SELECT 'fix'::text AS k, f.id AS hid, f.image_no AS img, d.title AS subj,
               jsonb_build_object('values', to_jsonb(f."values"), 'status', f.status) AS prop,
               f.decision AS dec, f.decided_at AS dat, f.note AS nt, f.submitted_at AS sa
          FROM rat.pending_fix_review f
          LEFT JOIN rat.data_fix d ON d.id = f.fix_id
         WHERE f.submitted_by = auth.uid() AND f.decision IN ('pending', 'rejected')
        UNION ALL
        SELECT 'answer'::text, m.id, m.image_no, t.question,
               jsonb_build_object('answer', m.answer, 'mode', m.mode),
               m.decision, m.decided_at, m.note, m.submitted_at
          FROM rat.pending_model_answer m
          LEFT JOIN rat.model_task t ON t.id = m.task_id
         WHERE m.submitted_by = auth.uid() AND m.decision IN ('pending', 'rejected')
      ) hl
     ORDER BY hl.sa DESC, hl.k, hl.hid DESC;
END;
$$;

REVOKE ALL ON FUNCTION
  rat._validate_model_answer(text, text, text, bigint, text, text)
  FROM PUBLIC, anon, authenticated;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'picaloco_agent') THEN
    REVOKE ALL ON FUNCTION
      rat._validate_model_answer(text, text, text, bigint, text, text)
      FROM picaloco_agent;
  END IF;
END $$;

REVOKE EXECUTE ON FUNCTION
  rat.submit_fix_review(text, text, text[], text, timestamptz),
  rat.submit_fix_confirm(text, jsonb),
  rat.submit_model_answer(text, text, text, bigint, text, text),
  rat.approve_fix_review(bigint[], boolean),
  rat.approve_model_answer(bigint[]),
  rat.reject_held(text, bigint[], text),
  rat.held_list(uuid),
  rat.my_held()
  FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION
  rat.submit_fix_review(text, text, text[], text, timestamptz),
  rat.submit_fix_confirm(text, jsonb),
  rat.submit_model_answer(text, text, text, bigint, text, text),
  rat.approve_fix_review(bigint[], boolean),
  rat.approve_model_answer(bigint[]),
  rat.reject_held(text, bigint[], text),
  rat.held_list(uuid),
  rat.my_held()
  TO authenticated;

COMMIT;
