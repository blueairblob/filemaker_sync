"""Schema tests for the volunteer role (add_volunteer_tables.sql).

Run against the throwaway Docker postgres only (Golden Rule #4). The fixture DB
is shared, so each test makes its own users and never asserts table-wide counts.
"""
import json
import uuid

import psycopg2
import psycopg2.errors
import pytest

from tests.conftest import (
    BOOTSTRAP_SQL, VOLUNTEER_SQL, apply_sql_file, as_user, make_admin,
    make_user, make_volunteer, seed_catalog, seed_fix_value, seed_run, seed_task,
)


def _call(pg, uid, sql, args=()):
    pg.commit()
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
    pg.commit()  # persist and drop the SET LOCAL ROLE
    return rows


def _row(pg, uid):
    with pg.cursor() as cur:
        cur.execute("select in_training, must_change_password, removed_by, removed_at, added_by "
                    "from rat.volunteer where user_id = %s", (uid,))
        return cur.fetchone()


def test_my_role_per_kind(pg):
    a, v, t, gone, s = (make_admin(pg), make_volunteer(pg, in_training=False),
                        make_volunteer(pg), make_volunteer(pg, removed=True), make_user(pg))
    pg.commit()
    expect = {a: "admin", v: "volunteer", t: "volunteer", gone: None, s: None}
    for uid, role in expect.items():
        as_user(pg, uid)
        with pg.cursor() as cur:
            cur.execute("select rat.my_role()")
            r = cur.fetchone()[0]
        assert r["role"] == role
        if role == "volunteer":
            assert r["in_training"] is (uid == t)
            assert r["must_change_password"] is True
        pg.rollback()
    as_user(pg, t)
    with pg.cursor() as cur:
        cur.execute("select rat.is_trainee(), rat.can_review()")
        assert cur.fetchone() == (True, True)
    pg.rollback()
    as_user(pg, v)
    with pg.cursor() as cur:
        cur.execute("select rat.is_trainee(), rat.can_review(), rat.is_volunteer()")
        assert cur.fetchone() == (False, True, True)
    pg.rollback()
    as_user(pg, gone)
    with pg.cursor() as cur:
        cur.execute("select rat.is_volunteer(), rat.can_review()")
        assert cur.fetchone() == (False, False)


def test_add_volunteer_rules(pg):
    admin, adm2, target = make_admin(pg), make_admin(pg), make_user(pg)
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute("select rat.add_volunteer(%s)", (adm2,))
        assert ei.value.pgcode == "22023" and "admins cannot be volunteers" in str(ei.value)
    pg.rollback()
    as_user(pg, admin)
    with pg.cursor() as cur:
        cur.execute("select (rat.add_volunteer(%s)).user_id", (target,))
        assert str(cur.fetchone()[0]) == target
        cur.execute("savepoint s")
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute("select rat.add_volunteer(%s)", (target,))
        assert ei.value.pgcode == "22023" and "already a volunteer" in str(ei.value)
        cur.execute("rollback to savepoint s")
        cur.execute("select rat.set_volunteer(%s, false, false)", (target,))
        cur.execute("select (rat.add_volunteer(%s)).user_id", (target,))
    pg.commit()
    in_training, must_change, removed_by, removed_at, _ = _row(pg, target)
    assert (in_training, must_change, removed_by, removed_at) == (True, True, None, None)


def test_add_volunteer_admin_only(pg):
    v, s, target = make_volunteer(pg), make_user(pg), make_user(pg)
    pg.commit()
    calls = [
        ("select rat.add_volunteer(%s)", (target,)),
        ("select rat.set_volunteer(%s, true, null)", (v,)),
        ("select * from rat.people()", ()),
        ("select rat.account_status('x@example.test')", ()),
        ("select rat.require_password_change(%s)", (v,)),
    ]
    for uid in (v, s):
        for sql, args in calls:
            as_user(pg, uid)
            with pg.cursor() as cur:
                with pytest.raises(psycopg2.Error) as ei:
                    cur.execute(sql, args)
                assert ei.value.pgcode == "42501", sql
            pg.rollback()


def test_set_volunteer_partial(pg):
    admin, v = make_admin(pg), make_volunteer(pg)
    _call(pg, admin, "select rat.set_volunteer(%s, null, false)", (v,))
    _, _, removed_by, removed_at, _ = _row(pg, v)
    assert str(removed_by) == admin and removed_at is not None
    # in_training only: removal untouched
    _call(pg, admin, "select rat.set_volunteer(%s, false, null)", (v,))
    in_training, _, removed_by2, removed_at2, _ = _row(pg, v)
    assert in_training is False and removed_at2 == removed_at and str(removed_by2) == admin
    # active true clears
    _call(pg, admin, "select rat.set_volunteer(%s, null, true)", (v,))
    in_training, _, removed_by3, removed_at3, _ = _row(pg, v)
    assert in_training is False and removed_by3 is None and removed_at3 is None
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute("select rat.set_volunteer(%s, true, null)", (str(uuid.uuid4()),))
        assert ei.value.pgcode == "P0002"


def test_require_password_change(pg):
    admin, v, gone = make_admin(pg), make_volunteer(pg), make_volunteer(pg, removed=True)
    with pg.cursor() as cur:
        cur.execute("update rat.volunteer set must_change_password = false where user_id = %s", (v,))
    _call(pg, admin, "select rat.require_password_change(%s)", (v,))
    assert _row(pg, v)[1] is True
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute("select rat.require_password_change(%s)", (gone,))
        assert ei.value.pgcode == "P0002"


def test_account_status_kinds(pg):
    admin = make_admin(pg)
    tag = uuid.uuid4().hex[:8]
    ids = {
        "admin": make_admin(pg), "volunteer": make_volunteer(pg),
        "removed_volunteer": make_volunteer(pg, removed=True), "other": make_user(pg),
    }
    with pg.cursor() as cur:
        for kind, uid in ids.items():
            cur.execute("update auth.users set email = %s where id = %s",
                        (f"{kind}_{tag}@example.test", uid))
    pg.commit()
    for kind, uid in ids.items():
        r = _call(pg, admin, "select rat.account_status(%s)", (f" {kind.upper()}_{tag}@Example.test ",))[0][0]
        assert r == {"user_id": uid, "kind": kind}
        pg.rollback()
    r = _call(pg, admin, "select rat.account_status(%s)", (f"nobody_{tag}@example.test",))[0][0]
    assert r == {"user_id": None, "kind": "none"}
    pg.rollback()
    r = _call(pg, admin, "select rat.account_status(null, %s)", (ids["volunteer"],))[0][0]
    assert r["kind"] == "volunteer"


def test_people_lists_admins_and_volunteers(pg):
    admin, v, gone = make_admin(pg), make_volunteer(pg), make_volunteer(pg, removed=True)
    rows = {str(r[0]): r for r in _call(pg, admin, "select * from rat.people()")}
    assert rows[admin][2] == "admin"
    assert rows[v][2] == "volunteer" and rows[v][3] is True and rows[v][5] is None
    assert rows[gone][2] == "volunteer" and rows[gone][5] is not None
    assert rows[v][6] == 0


def test_password_changed_self_only(pg):
    v1, v2, s = make_volunteer(pg), make_volunteer(pg), make_user(pg)
    pg.commit()
    as_user(pg, v1)
    with pg.cursor() as cur:
        cur.execute("select rat.password_changed()")
    pg.commit()
    assert _row(pg, v1)[1] is False
    assert _row(pg, v2)[1] is True
    as_user(pg, s)  # non-volunteer: no-op, no error
    with pg.cursor() as cur:
        cur.execute("select rat.password_changed()")
    pg.rollback()
    assert _row(pg, s) is None


def test_bootstrap_carries_volunteer_copy():
    boot = BOOTSTRAP_SQL.read_text(encoding="utf-8")
    block = boot.split("-- >>> add_volunteer_tables.sql\n")[1].split("-- <<< add_volunteer_tables.sql")[0]
    assert block == VOLUNTEER_SQL.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Task 2: one write path, history columns, access for direct saves and reads
# --------------------------------------------------------------------------
#
# (Other modules re-apply the volunteer migration last after re-running the
# original SQL, so no per-module re-apply fixture is needed here.)

LINKS = {"no_answer_flag": "loco", "fix_ids": ["loco_number", "loco_name"]}


def _make(pg, who):
    return {
        "admin": lambda: make_admin(pg),
        "volunteer": lambda: make_volunteer(pg, in_training=False),
        "trainee": lambda: make_volunteer(pg),
        "removed": lambda: make_volunteer(pg, removed=True),
        "stranger": lambda: make_user(pg),
    }[who]()


def _as(pg, uid, sql, args=()):
    """Run sql as uid; return (rows, None) or (None, error). Always rolls back
    on error; commits on success."""
    as_user(pg, uid)
    try:
        with pg.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        pg.rollback()
        return None, exc
    pg.commit()
    return rows, None


def _fix(pg, image_no, fix_id="loco_number"):
    with pg.cursor() as cur:
        cur.execute('select "values", status, reviewed_by::text from rat.data_fix_value '
                    "where image_no = %s and fix_id = %s", (image_no, fix_id))
        return cur.fetchone()


@pytest.mark.parametrize("who,ok", [("admin", True), ("volunteer", True), ("trainee", False),
                                    ("removed", False), ("stranger", False)])
def test_direct_save_access(pg, who, ok):
    uid = _make(pg, who)
    p = f"v2d_{who}_"
    ts_review = seed_fix_value(pg, f"{p}r", values=["1"])
    ts_confirm = seed_fix_value(pg, f"{p}c", values=["7"], suggested_values=["8"])
    task = seed_task(pg, f"t_v2d_{who}")
    seed_catalog(pg, f"{p}a", "a scene")
    pg.commit()

    calls = {
        "review_fix_value": ("select rat.review_fix_value(%s, 'loco_number', %s, 'corrected', %s)",
                             (f"{p}r", ["2"], ts_review)),
        "confirm_fix_values": ("select rat.confirm_fix_values('loco_number', %s::jsonb)",
                               (json.dumps([{"image_no": f"{p}c",
                                             "updated_at": ts_confirm.isoformat()}]),)),
        "save_model_answer": ("select rat.save_model_answer(%s, %s, 'loco', null, null, 'sample')",
                              (task, f"{p}a")),
    }
    for rpc, (sql, args) in calls.items():
        rows, err = _as(pg, uid, sql, args)
        if ok:
            assert err is None, (rpc, err)
        else:
            assert err is not None and err.pgcode == "42501", rpc
            if who == "trainee":
                assert "held for approval" in str(err), rpc

    if ok:
        assert _fix(pg, f"{p}r") == (["2"], "corrected", uid)
        assert _fix(pg, f"{p}c") == (["8"], "verified", uid)
        with pg.cursor() as cur:
            cur.execute("select answered_by::text, approved_by from rat.model_answer "
                        "where task_id = %s", (task,))
            assert cur.fetchall() == [(uid, None)]
    else:
        assert _fix(pg, f"{p}r")[:2] == (["1"], "extracted")
        assert _fix(pg, f"{p}c")[:2] == (["7"], "extracted")
        with pg.cursor() as cur:
            cur.execute("select count(*) from rat.model_answer where task_id = %s", (task,))
            assert cur.fetchone()[0] == 0


READ_RPCS = {
    "data_fix_queue": "select * from rat.data_fix_queue('loco_number', p_limit => 1)",
    "data_fix_progress": "select * from rat.data_fix_progress()",
    "model_check_queue": "select * from rat.model_check_queue('t_v2_reads', 'sample', null, p_limit => 1)",
    "model_check_progress": "select * from rat.model_check_progress()",
}


@pytest.mark.parametrize("rpc", list(READ_RPCS))
def test_reads_open_to_volunteers(pg, rpc):
    seed_task(pg, "t_v2_reads")
    users = {who: _make(pg, who) for who in ("volunteer", "trainee", "removed", "stranger")}
    pg.commit()
    for who, uid in users.items():
        _, err = _as(pg, uid, READ_RPCS[rpc])
        if who in ("volunteer", "trainee"):
            assert err is None, (who, err)
        else:
            assert err is not None and err.pgcode == "42501", who


def test_admin_only_reads_stay(pg):
    v, t = make_volunteer(pg, in_training=False), make_volunteer(pg)
    pg.commit()
    for sql in ("select * from rat.model_check_photo('t', 'i')",
                "select * from rat.model_check_confusion(1)",
                "select * from rat.model_check_conflicts('t')",
                "select * from rat.data_fix_orphans()"):
        for uid in (v, t):
            _, err = _as(pg, uid, sql)
            assert err is not None and err.pgcode == "42501", sql


def test_volunteer_rls_tables(pg):
    seed_fix_value(pg, "v2_rls_1", values=["1"])
    task = seed_task(pg, "t_v2_rls")
    run_id = seed_run(pg, task)
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode) "
                    "values (%s, 'v2_rls_1', 'loco', 'sample')", (task,))
    v, t, gone = make_volunteer(pg, in_training=False), make_volunteer(pg), make_volunteer(pg, removed=True)
    pg.commit()
    counts = {
        "select count(*) from rat.data_fix where id = 'loco_number'": 1,
        "select count(*) from rat.data_fix_value where image_no = 'v2_rls_1'": 1,
        f"select count(*) from rat.model_task where id = '{task}'": 1,
        f"select count(*) from rat.model_run where id = {run_id}": 1,
        "select count(*) from rat.data_fix_history where image_no = 'v2_rls_1'": 0,
    }
    for uid, sees in ((v, True), (t, True), (gone, False)):
        for sql, n in counts.items():
            rows, err = _as(pg, uid, sql)
            assert err is None, sql
            assert rows[0][0] == (n if sees else 0), (uid, sql)
        _, err = _as(pg, uid, "select * from rat.model_answer limit 1")
        assert isinstance(err, psycopg2.errors.InsufficientPrivilege)


def test_history_records_old_flags(pg):
    ts = seed_fix_value(pg, "v2_old_1", values=["1"], needs_recheck=True,
                        description_hash="h_before")
    v = make_volunteer(pg, in_training=False)
    pg.commit()
    _, err = _as(pg, v, "select rat.review_fix_value('v2_old_1', 'loco_number', %s, 'corrected', %s)",
                 (["2"], ts))
    assert err is None, err
    with pg.cursor() as cur:
        cur.execute("select old_needs_recheck, old_description_hash, changed_by::text, approved_by, "
                    "source, model_answer_id, undoes, undoes_from "
                    "from rat.data_fix_history where image_no = 'v2_old_1' order by id")
        rows = cur.fetchall()
    # The extractor's insert has no old row; the review records the old flags.
    assert rows[0][:2] == (None, None)
    assert rows[-1] == (True, "h_before", v, None, "review", None, None, None)


def test_fill_history_links_answer(pg):
    task, img = seed_task(pg, "t_v2_fill", links=LINKS), "v2_fill_1"
    seed_catalog(pg, img, "A station scene")
    for fix_id in LINKS["fix_ids"]:
        seed_fix_value(pg, img, fix_id, confidence="none")
    v = make_volunteer(pg, in_training=False)
    pg.commit()
    rows, err = _as(pg, v, "select rat.save_model_answer(%s, %s, 'station', null, null, 'sample')",
                    (task, img))
    assert err is None, err
    res = rows[0][0]
    assert res["filled"] == ["loco_number", "loco_name"]
    with pg.cursor() as cur:
        cur.execute("select fix_id, source, changed_by::text, model_answer_id, approved_by "
                    "from rat.data_fix_history where image_no = %s and old_status is not null "
                    "order by fix_id", (img,))
        assert cur.fetchall() == [("loco_name", "model_check", v, res["answer_id"], None),
                                  ("loco_number", "model_check", v, res["answer_id"], None)]


def test_guard_still_blocks_reviewed_overwrite(pg):
    seed_fix_value(pg, "v2_guard_1", values=["5"], status="verified")
    pg.commit()
    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("""update rat.data_fix_value set "values" = '{6}' """
                        "where image_no = 'v2_guard_1' and fix_id = 'loco_number'")
    assert "reviewed value is protected" in str(exc.value)
    pg.rollback()
    # The migration leaves the guard itself alone.
    assert "rat.data_fix_value_guard()" not in VOLUNTEER_SQL.read_text(encoding="utf-8")


def test_apply_records_author_and_approver(pg):
    """The internal write path, as an approval will call it: the author is
    p_by, the approver is recorded, and every setting is closed again."""
    trainee, admin = make_volunteer(pg), make_admin(pg)
    ts = seed_fix_value(pg, "v2_apply_1", values=["1"])
    task, img = seed_task(pg, "t_v2_apply", links=LINKS), "v2_apply_2"
    seed_catalog(pg, img, "A scene")
    for fix_id in LINKS["fix_ids"]:
        seed_fix_value(pg, img, fix_id, confidence="none")
    pg.commit()
    # Not callable by a signed-in user, not even an admin.
    for sql in ("select rat._apply_fix_review('v2_apply_1', 'loco_number', '{9}', 'corrected', "
                "now(), null, null)",
                "select rat._apply_model_answer('t', 'i', 'a', null, null, 'sample', null, null, null)"):
        _, err = _as(pg, admin, sql)
        assert isinstance(err, psycopg2.errors.InsufficientPrivilege), sql
    # As the owner, the way the approve RPCs (SECURITY DEFINER) will run them.
    with pg.cursor() as cur:
        cur.execute("select (rat._apply_fix_review('v2_apply_1', 'loco_number', %s, 'corrected', "
                    "%s, %s, %s)).reviewed_by::text", (["9"], ts, trainee, admin))
        assert cur.fetchone()[0] == trainee
        cur.execute("select rat._apply_model_answer(%s, %s, 'station', null, null, 'sample', "
                    "%s, %s, '2026-01-02T03:04:05Z')", (task, img, trainee, admin))
        res = cur.fetchone()[0]
        assert res["filled"] == ["loco_number", "loco_name"]
        cur.execute("select " + ", ".join(
            f"current_setting('rat.{s}', true)" for s in
            ("review_context", "review_source", "acting_for", "approved_by", "model_answer_id")))
        assert set(cur.fetchone()) <= {"", None}
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("select changed_by::text, approved_by::text, source from rat.data_fix_history "
                    "where image_no = 'v2_apply_1' and old_status is not null")
        assert cur.fetchall() == [(trainee, admin, "review")]
        cur.execute("select answered_by::text, answered_at = '2026-01-02T03:04:05Z'::timestamptz, "
                    "approved_by::text, approved_at is not null from rat.model_answer "
                    "where id = %s", (res["answer_id"],))
        assert cur.fetchone() == (trainee, True, admin, True)
        cur.execute("select changed_by::text, approved_by::text, model_answer_id "
                    "from rat.data_fix_history where image_no = %s and old_status is not null",
                    (img,))
        assert set(cur.fetchall()) == {(trainee, admin, res["answer_id"])}


def test_internal_and_admin_privileges(pg):
    with pg.cursor() as cur:
        for fn in ("rat._apply_fix_review(text, text, text[], text, timestamptz, uuid, uuid)",
                   "rat._apply_model_answer(text, text, text, bigint, text, text, uuid, uuid, timestamptz)"):
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "has_function_privilege('public', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn, fn))
            assert cur.fetchone() == (False, False, False, True, ['search_path=""']), fn
        cur.execute("select has_function_privilege('anon', 'rat.add_volunteer(uuid)', 'EXECUTE'), "
                    "has_table_privilege('authenticated', 'rat.volunteer', 'SELECT'), "
                    "has_table_privilege('anon', 'rat.volunteer', 'SELECT')")
        assert cur.fetchone() == (False, False, False)
        # The redefined RPCs keep their grants and definer rights.
        for fn in ("rat.review_fix_value(text, text, text[], text, timestamptz)",
                   "rat.confirm_fix_values(text, jsonb)",
                   "rat.save_model_answer(text, text, text, bigint, text, text)",
                   "rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer)",
                   "rat.data_fix_progress()",
                   "rat.model_check_queue(text, text, bigint, text, text, text, integer)",
                   "rat.model_check_progress()"):
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn))
            assert cur.fetchone() == (False, True, True, ['search_path=""']), fn


def test_history_source_check_and_undo_tag(pg):
    with pg.cursor() as cur:
        cur.execute("select conname, pg_get_constraintdef(oid) from pg_constraint "
                    "where conrelid = 'rat.data_fix_history'::regclass and contype = 'c' "
                    "and pg_get_constraintdef(oid) ilike '%source%'")
        ((name, definition),) = cur.fetchall()
        assert name == "data_fix_history_source_check"
        for src in ("extractor", "review", "model_check", "undo"):
            assert f"'{src}'" in definition
    seed_fix_value(pg, "v2_undo_1", values=["1"], status="verified")
    with pg.cursor() as cur:
        for k, v in (("review_context", "on"), ("review_source", "undo"),
                     ("undoes", "41"), ("undoes_from", "40")):
            cur.execute("select set_config(%s, %s, true)", (f"rat.{k}", v))
        cur.execute("""update rat.data_fix_value set "values" = '{}', status = 'extracted' """
                    "where image_no = 'v2_undo_1' and fix_id = 'loco_number'")
        cur.execute("select source, undoes, undoes_from, old_status, old_needs_recheck "
                    "from rat.data_fix_history where image_no = 'v2_undo_1' "
                    "order by id desc limit 1")
        assert cur.fetchone() == ("undo", 41, 40, "verified", False)
    pg.rollback()


def test_new_columns_nullable(pg):
    expect = {
        ("data_fix_history", "approved_by"): "uuid",
        ("data_fix_history", "undoes"): "bigint",
        ("data_fix_history", "undoes_from"): "bigint",
        ("data_fix_history", "model_answer_id"): "bigint",
        ("data_fix_history", "old_needs_recheck"): "boolean",
        ("data_fix_history", "old_description_hash"): "text",
        ("model_answer", "approved_by"): "uuid",
        ("model_answer", "approved_at"): "timestamp with time zone",
    }
    with pg.cursor() as cur:
        for (table, col), typ in expect.items():
            cur.execute("select data_type, is_nullable, column_default "
                        "from information_schema.columns "
                        "where table_schema = 'rat' and table_name = %s and column_name = %s",
                        (table, col))
            assert cur.fetchone() == (typ, "YES", None), (table, col)


# --------------------------------------------------------------------------
# Task 3: withdrawn answers stop counting
# --------------------------------------------------------------------------

def _answer(pg, uid, task, image_no, answer, mode="sample", run=None, shown=None):
    """Save an answer through the RPC as uid; return the new model_answer id."""
    _call(pg, uid, "select rat.save_model_answer(%s, %s, %s, %s, %s, %s)",
          (task, image_no, answer, run, shown, mode))
    with pg.cursor() as cur:
        cur.execute("select max(id) from rat.model_answer where task_id = %s and image_no = %s",
                    (task, image_no))
        return cur.fetchone()[0]


def _withdraw(pg, answer_id, by):
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer_withdrawal (answer_id, withdrawn_by) "
                    "values (%s, %s)", (answer_id, by))
    pg.commit()


def test_withdrawal_append_only(pg):
    task, img = "t3w_ao", "t3w_ao_1"
    seed_task(pg, task)
    seed_catalog(pg, img, "x")
    admin = make_admin(pg)
    pg.commit()
    aid = _answer(pg, admin, task, img, "loco")
    _withdraw(pg, aid, admin)
    for sql in ("update rat.model_answer_withdrawal set note = 'x'",
                "delete from rat.model_answer_withdrawal",
                "truncate rat.model_answer_withdrawal"):
        with pytest.raises(psycopg2.Error) as ei:
            with pg.cursor() as cur:
                cur.execute(sql)
        assert "append-only" in str(ei.value)
        pg.rollback()


def test_withdrawn_answer_stops_counting(pg):
    task, img = "t3w_cnt", "t3w_cnt_1"
    seed_task(pg, task)
    seed_catalog(pg, img, "x")
    run = seed_run(pg, task, "v1", {img: ("loco", 0.95)})
    admin = make_admin(pg)
    pg.commit()
    old = _answer(pg, admin, task, img, "station", "sure", run, "loco")
    new = _answer(pg, admin, task, img, "loco", "sure", run, "loco")
    prog = "select n_checked, n_agree from rat.model_check_progress() where run_id = %s"
    conf = "select model_answer, checked_answer, n from rat.model_check_confusion(%s, false)"
    assert _call(pg, admin, prog, (run,)) == [(1, 1)]
    assert _call(pg, admin, conf, (run,)) == [("loco", "loco", 1)]
    _withdraw(pg, new, admin)
    assert _call(pg, admin, prog, (run,)) == [(1, 0)]
    assert _call(pg, admin, conf, (run,)) == [("loco", "station", 1)]
    photo = _call(pg, admin, "select checked_answer from rat.model_check_photo(%s, %s)", (task, img))
    assert photo == [("station",)]
    q = "select image_no from rat.model_check_queue(%s, 'sure', %s)"
    assert _call(pg, admin, q, (task, run)) == []
    _withdraw(pg, old, admin)
    assert _call(pg, admin, prog, (run,)) == [(0, 0)]
    assert _call(pg, admin, q, (task, run)) == [(img,)]


def test_withdrawn_blind_leaves_sample(pg):
    task, img = "t3w_bl", "t3w_bl_1"
    seed_task(pg, task)
    seed_catalog(pg, img, "x")
    run = seed_run(pg, task, "v1", {img: ("loco", 0.95)})
    admin = make_admin(pg)
    pg.commit()
    aid = _answer(pg, admin, task, img, "loco")
    prog = "select n_blind from rat.model_check_progress() where run_id = %s"
    # md5 prefix sorts just before this photo's own md5: limit 1 is then this photo
    # when it is eligible, regardless of how many other catalog rows the shared DB holds
    import hashlib
    after = hashlib.md5(img.encode()).hexdigest()[:-1]
    sample = "select image_no from rat.model_check_queue(%s, 'sample', null, null, null, %s, 1)"
    assert _call(pg, admin, prog, (run,)) == [(1,)]
    assert (img,) not in _call(pg, admin, sample, (task, after))
    _withdraw(pg, aid, admin)
    assert _call(pg, admin, prog, (run,)) == [(0,)]
    assert (img,) in _call(pg, admin, sample, (task, after))


def test_withdrawal_objects_closed(pg):
    with pg.cursor() as cur:
        cur.execute("select has_table_privilege(r, t, 'select') from "
                    "(values ('anon'), ('authenticated')) a(r), "
                    "(values ('rat.model_answer_live'), ('rat.model_answer_withdrawal')) b(t)")
        assert [x[0] for x in cur.fetchall()] == [False] * 4
