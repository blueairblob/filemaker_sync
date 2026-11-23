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

-- held_count is a placeholder (0) until a later task redefines people().
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
    SELECT a.user_id, u.email::text, 'admin'::text, NULL::boolean,
           NULL::timestamptz, NULL::timestamptz, 0::bigint
      FROM rat.admin_user a
      LEFT JOIN auth.users u ON u.id = a.user_id
    UNION ALL
    SELECT v.user_id, u.email::text, 'volunteer'::text, v.in_training,
           v.added_at, v.removed_at, 0::bigint
      FROM rat.volunteer v
      LEFT JOIN auth.users u ON u.id = v.user_id;
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

COMMIT;
