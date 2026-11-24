"""Schema tests for the volunteer role (add_volunteer_tables.sql).

Run against the throwaway Docker postgres only (Golden Rule #4). The fixture DB
is shared, so each test makes its own users and never asserts table-wide counts.
"""
import datetime
import json
import uuid

import psycopg2
import psycopg2.errors
import pytest

from tests.conftest import (
    BOOTSTRAP_SQL, VOLUNTEER_SQL, _connect, apply_sql_file, as_user, make_admin,
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
    admin, adm2, adm3, target = make_admin(pg), make_admin(pg), make_admin(pg), make_user(pg)
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
    pg.commit()
    assert str(_row(pg, target)[4]) == admin
    # Re-adding a removed volunteer: added_by becomes the re-adding admin.
    as_user(pg, adm3)
    with pg.cursor() as cur:
        cur.execute("select (rat.add_volunteer(%s)).user_id", (target,))
    pg.commit()
    in_training, must_change, removed_by, removed_at, added_by = _row(pg, target)
    assert (in_training, must_change, removed_by, removed_at) == (True, True, None, None)
    assert str(added_by) == adm3


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


# --------------------------------------------------------------------------
# Task 4: held work (trainees submit, an admin approves or rejects)
# --------------------------------------------------------------------------

SUBMIT_FIX = "select rat.submit_fix_review(%s, 'loco_number', %s, %s, %s)"
SUBMIT_ANSWER = "select rat.submit_model_answer(%s, %s, %s, %s, %s, %s)"


def _one(pg, uid, sql, args=()):
    """Run sql as uid (must succeed); return the first column of the first row."""
    rows, err = _as(pg, uid, sql, args)
    assert err is None, err
    return rows[0][0]


def _pending(pg, table, pid):
    with pg.cursor() as cur:
        cur.execute(f"select decision, decided_by::text, note from rat.{table} where id = %s",
                    (pid,))
        return cur.fetchone()


def _live(pg, image_no, fix_id="loco_number"):
    with pg.cursor() as cur:
        cur.execute('select "values", status, reviewed_by::text, updated_at '
                    "from rat.data_fix_value where image_no = %s and fix_id = %s",
                    (image_no, fix_id))
        return cur.fetchone()


def _linked_photo(pg, img, description="A station scene"):
    seed_catalog(pg, img, description)
    for fix_id in LINKS["fix_ids"]:
        seed_fix_value(pg, img, fix_id, confidence="none")


def test_submit_trainee_only(pg):
    task = seed_task(pg, "t4_only")
    seed_catalog(pg, "v4o_a", "a scene")
    ts = seed_fix_value(pg, "v4o_r", values=["1"])
    users = {who: _make(pg, who) for who in ("admin", "volunteer", "removed", "stranger", "trainee")}
    pg.commit()
    calls = [
        (SUBMIT_FIX, ("v4o_r", ["2"], "corrected", ts)),
        ("select rat.submit_fix_confirm('loco_number', %s::jsonb)",
         (json.dumps([{"image_no": "v4o_r", "updated_at": ts.isoformat()}]),)),
        (SUBMIT_ANSWER, (task, "v4o_a", "loco", None, None, "sample")),
    ]
    for who, uid in users.items():
        for sql, args in calls:
            _, err = _as(pg, uid, sql, args)
            if who == "trainee":
                assert err is None, (sql, err)
            else:
                assert err is not None and err.pgcode == "42501", (who, sql)


def test_submit_validates_like_direct(pg):
    task = seed_task(pg, "t4_valid")
    seed_catalog(pg, "v4v_a", "a scene")
    run = seed_run(pg, task)
    ts = seed_fix_value(pg, "v4v_r", values=["1"])
    t = make_volunteer(pg)
    pg.commit()
    bad = [
        (SUBMIT_FIX, ("v4v_r", ["2"], "bogus", ts), "22023"),
        (SUBMIT_FIX, ("v4v_none", ["2"], "corrected", ts), "P0002"),
        (SUBMIT_FIX, ("v4v_r", ["2"], "corrected", "2000-01-01T00:00:00Z"), "PT409"),
        (SUBMIT_ANSWER, ("t4_nope", "v4v_a", "loco", None, None, "sample"), "P0002"),
        (SUBMIT_ANSWER, (task, "v4v_a", "loco", None, None, "bogus"), "22023"),
        (SUBMIT_ANSWER, (task, "v4v_a", "nope", None, None, "sample"), "22023"),
        (SUBMIT_ANSWER, (task, "v4v_a", "loco", run, "loco", "sample"), "22023"),
        (SUBMIT_ANSWER, (task, "v4v_a", "loco", None, None, "sure"), "22023"),
        (SUBMIT_ANSWER, (task, "v4v_none", "loco", None, None, "sample"), "P0002"),
    ]
    for sql, args, code in bad:
        _, err = _as(pg, t, sql, args)
        assert err is not None and err.pgcode == code, (args, err)
    # The table itself also refuses a non-blind sample row.
    with pytest.raises(psycopg2.errors.CheckViolation):
        with pg.cursor() as cur:
            cur.execute("insert into rat.pending_model_answer (task_id, image_no, answer, "
                        "shown_run_id, shown_answer, mode, submitted_by) "
                        "values (%s, 'v4v_a', 'loco', %s, 'loco', 'sample', %s)", (task, run, t))
    pg.rollback()


def test_submit_holds_nothing_live(pg):
    task, img = seed_task(pg, "t4_hold", links=LINKS), "v4h_a"
    _linked_photo(pg, img)
    run = seed_run(pg, task, predictions={img: ("station", 0.95)})
    ts = seed_fix_value(pg, "v4h_r", values=["1"])
    tc = seed_fix_value(pg, "v4h_c", values=["7"], suggested_values=["8"])
    t = make_volunteer(pg)
    pg.commit()
    before = {i: _live(pg, i) for i in ("v4h_r", "v4h_c")}
    before_fill = [_live(pg, img, f) for f in LINKS["fix_ids"]]

    _one(pg, t, SUBMIT_FIX, ("v4h_r", ["2"], "corrected", ts))
    n = _one(pg, t, "select rat.submit_fix_confirm('loco_number', %s::jsonb)",
             (json.dumps([{"image_no": "v4h_c", "updated_at": tc.isoformat()},
                          {"image_no": "v4h_r", "updated_at": ts.isoformat()},
                          {"image_no": "v4h_c", "updated_at": "2000-01-01T00:00:00Z"}]),))
    assert n == 2
    _one(pg, t, SUBMIT_ANSWER, (task, img, "station", run, "station", "sure"))

    assert {i: _live(pg, i) for i in before} == before
    assert [_live(pg, img, f) for f in LINKS["fix_ids"]] == before_fill
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.model_answer where task_id = %s", (task,))
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from rat.data_fix_history "
                    "where image_no in ('v4h_r', 'v4h_c', %s) and old_status is not null", (img,))
        assert cur.fetchone()[0] == 0
        cur.execute("select image_no, \"values\", status, base_values, base_status, "
                    "base_updated_at, submitted_by::text, decision "
                    "from rat.pending_fix_review where submitted_by = %s order by image_no, id",
                    (t,))
        # The confirm re-held v4h_r (still 'extracted' live): the earlier review is withdrawn.
        assert cur.fetchall() == [
            ("v4h_c", ["8"], "verified", ["7"], "extracted", tc, t, "pending"),
            ("v4h_r", ["2"], "corrected", ["1"], "extracted", ts, t, "withdrawn"),
            ("v4h_r", ["1"], "verified", ["1"], "extracted", ts, t, "pending"),
        ]
    admin = make_admin(pg)
    pg.commit()
    rows, err = _as(pg, admin, "select n_checked from rat.model_check_progress() where run_id = %s",
                    (run,))
    assert err is None and rows == [(0,)]


def test_resubmit_withdraws_previous(pg):
    task, img = seed_task(pg, "t4_resub"), "v4rs_a"
    seed_catalog(pg, img, "a scene")
    ts = seed_fix_value(pg, "v4rs_r", values=["1"])
    t = make_volunteer(pg)
    pg.commit()
    f1 = _one(pg, t, SUBMIT_FIX, ("v4rs_r", ["2"], "corrected", ts))
    f2 = _one(pg, t, SUBMIT_FIX, ("v4rs_r", ["3"], "corrected", ts))
    a1 = _one(pg, t, SUBMIT_ANSWER, (task, img, "loco", None, None, "sample"))
    a2 = _one(pg, t, SUBMIT_ANSWER, (task, img, "station", None, None, "sample"))
    assert _pending(pg, "pending_fix_review", f1) == ("withdrawn", t, None)
    assert _pending(pg, "pending_fix_review", f2) == ("pending", None, None)
    assert _pending(pg, "pending_model_answer", a1) == ("withdrawn", t, None)
    assert _pending(pg, "pending_model_answer", a2) == ("pending", None, None)
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.pending_fix_review "
                    "where image_no = 'v4rs_r' and decision = 'pending'")
        assert cur.fetchone()[0] == 1
        cur.execute("select count(*) from rat.pending_model_answer "
                    "where image_no = %s and decision = 'pending'", (img,))
        assert cur.fetchone()[0] == 1
        # At most one pending row per key per trainee, enforced by the table.
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute("insert into rat.pending_fix_review (image_no, fix_id, \"values\", status, "
                        "base_updated_at, submitted_by) "
                        "values ('v4rs_r', 'loco_number', '{4}', 'corrected', now(), %s)", (t,))
    pg.rollback()


def test_own_pending_hidden_from_own_queue_only(pg):
    import hashlib
    task, img, s_img = seed_task(pg, "t4_queue"), "v4q_a", "v4q_s"
    seed_catalog(pg, img, "a scene")
    seed_catalog(pg, s_img, "a scene")
    run = seed_run(pg, task, predictions={img: ("loco", 0.95)})
    ts = seed_fix_value(pg, img, values=["1"])
    a, b, admin = make_volunteer(pg), make_volunteer(pg), make_admin(pg)
    pg.commit()
    _one(pg, a, SUBMIT_FIX, (img, ["2"], "corrected", ts))
    _one(pg, a, SUBMIT_ANSWER, (task, img, "loco", run, "loco", "sure"))
    _one(pg, a, SUBMIT_ANSWER, (task, s_img, "loco", None, None, "sample"))

    fixq = "select image_no from rat.data_fix_queue('loco_number', p_text => %s)"
    sure = "select image_no from rat.model_check_queue(%s, 'sure', %s)"
    after = hashlib.md5(s_img.encode()).hexdigest()[:-1]
    sample = "select image_no from rat.model_check_queue(%s, 'sample', null, null, null, %s, 1)"
    for uid, sees in ((a, False), (b, True), (admin, True)):
        expect = [(img,)] if sees else []
        rows, err = _as(pg, uid, fixq, (img,))
        assert err is None and rows == expect, uid
        rows, err = _as(pg, uid, sure, (task, run))
        assert err is None and rows == expect, uid
        rows, err = _as(pg, uid, sample, (task, after))
        assert err is None and ((s_img,) in rows) is sees, uid


def test_approve_fix_records_author_and_approver(pg):
    ts = seed_fix_value(pg, "v4af_r", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_FIX, ("v4af_r", ["2"], "corrected", ts))
    res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([pid],))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    assert _live(pg, "v4af_r")[:3] == (["2"], "corrected", t)
    with pg.cursor() as cur:
        cur.execute("select changed_by::text, approved_by::text, source from rat.data_fix_history "
                    "where image_no = 'v4af_r' order by id desc limit 1")
        assert cur.fetchone() == (t, admin, "review")
        cur.execute("select decision, decided_by::text, decided_at is not null "
                    "from rat.pending_fix_review where id = %s", (pid,))
        assert cur.fetchone() == ("approved", admin, True)


def test_approve_stale_then_force(pg):
    ts = seed_fix_value(pg, "v4st_r", values=["1"])
    t, v, admin = make_volunteer(pg), make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_FIX, ("v4st_r", ["2"], "corrected", ts))
    _one(pg, v, "select rat.review_fix_value('v4st_r', 'loco_number', %s, 'corrected', %s)",
         (["5"], ts))
    res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([pid],))
    assert res == {"approved": 0, "skipped": [],
                   "stale": [{"id": pid, "current_values": ["5"], "current_status": "corrected"}]}
    assert _pending(pg, "pending_fix_review", pid)[0] == "pending"
    assert _live(pg, "v4st_r")[:3] == (["5"], "corrected", v)
    rows = _one(pg, admin, "select json_agg(h) from rat.held_list(%s) h", (t,))
    assert rows[0]["stale"] is True and rows[0]["current"] == {"values": ["5"], "status": "corrected"}
    res = _one(pg, admin, "select rat.approve_fix_review(%s, true)", ([pid],))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    assert _live(pg, "v4st_r")[:3] == (["2"], "corrected", t)


def test_approve_twice_skips(pg):
    ts = seed_fix_value(pg, "v4tw_r", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_FIX, ("v4tw_r", ["2"], "corrected", ts))
    assert _one(pg, admin, "select rat.approve_fix_review(%s)", ([pid],))["approved"] == 1
    hist = "select count(*) from rat.data_fix_history where image_no = 'v4tw_r'"
    with pg.cursor() as cur:
        cur.execute(hist)
        n = cur.fetchone()[0]
    res = _one(pg, admin, "select rat.approve_fix_review(%s, true)", ([pid],))
    assert res == {"approved": 0, "stale": [], "skipped": [{"id": pid, "reason": "not pending"}]}
    with pg.cursor() as cur:
        cur.execute(hist)
        assert cur.fetchone()[0] == n


def test_approve_missing_row_skipped(pg):
    ts1 = seed_fix_value(pg, "v4mr_1", values=["1"])
    ts2 = seed_fix_value(pg, "v4mr_2", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    p1 = _one(pg, t, SUBMIT_FIX, ("v4mr_1", ["2"], "corrected", ts1))
    p2 = _one(pg, t, SUBMIT_FIX, ("v4mr_2", ["3"], "corrected", ts2))
    with pg.cursor() as cur:
        cur.execute("delete from rat.data_fix_value where image_no = 'v4mr_1'")
    pg.commit()
    res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([p2, p1],))
    assert res == {"approved": 1, "stale": [],
                   "skipped": [{"id": p1, "reason": "no data_fix_value"}]}
    assert _live(pg, "v4mr_2")[:3] == (["3"], "corrected", t)
    assert _pending(pg, "pending_fix_review", p1)[0] == "pending"


def test_approve_answer_dates_and_fills(pg):
    task, img = seed_task(pg, "t4_appr", links=LINKS), "v4aa_a"
    _linked_photo(pg, img)
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_ANSWER, (task, img, "station", None, None, "sample"))
    res = _one(pg, admin, "select rat.approve_model_answer(%s)", ([pid, 999999999],))
    assert res == {"approved": 1, "skipped": [{"id": 999999999, "reason": "not pending"}]}
    with pg.cursor() as cur:
        cur.execute("select a.id, a.answered_by::text, a.answered_at = p.submitted_at, "
                    "a.approved_by::text, a.approved_at is not null, a.answer, a.mode "
                    "from rat.model_answer a, rat.pending_model_answer p "
                    "where a.task_id = %s and p.id = %s", (task, pid))
        aid, *rest = cur.fetchone()
        assert rest == [t, True, admin, True, "station", "sample"]
        cur.execute("select fix_id, source, changed_by::text, approved_by::text, model_answer_id "
                    "from rat.data_fix_history where image_no = %s and old_status is not null "
                    "order by fix_id", (img,))
        assert cur.fetchall() == [("loco_name", "model_check", t, admin, aid),
                                  ("loco_number", "model_check", t, admin, aid)]
    assert _pending(pg, "pending_model_answer", pid)[:2] == ("approved", admin)


def test_reject_keeps_note_and_my_held(pg):
    task, img = seed_task(pg, "t4_rej"), "v4rj_a"
    seed_catalog(pg, img, "a scene")
    ts = seed_fix_value(pg, "v4rj_r", values=["1"])
    t, other, admin = make_volunteer(pg), make_volunteer(pg), make_admin(pg)
    pg.commit()
    fid = _one(pg, t, SUBMIT_FIX, ("v4rj_r", ["2"], "corrected", ts))
    aid = _one(pg, t, SUBMIT_ANSWER, (task, img, "loco", None, None, "sample"))
    _, err = _as(pg, admin, "select rat.reject_held('bogus', %s, null)", ([fid],))
    assert err is not None and err.pgcode == "22023"
    res = _one(pg, admin, "select rat.reject_held('fix', %s, 'wrong number')", ([fid],))
    assert res == {"rejected": 1, "skipped": []}
    res = _one(pg, admin, "select rat.reject_held('answer', %s, null)", ([aid],))
    assert res["rejected"] == 1
    res = _one(pg, admin, "select rat.reject_held('fix', %s, 'again')", ([fid],))
    assert res == {"rejected": 0, "skipped": [{"id": fid, "reason": "not pending"}]}
    assert _live(pg, "v4rj_r")[:2] == (["1"], "extracted")
    assert _pending(pg, "pending_fix_review", fid) == ("rejected", admin, "wrong number")

    rows, err = _as(pg, t, "select kind, id, image_no, subject, proposed, decision, "
                           "decided_at is not null, note from rat.my_held()")
    assert err is None, err
    assert ("fix", fid, "v4rj_r", "Loco number", {"values": ["2"], "status": "corrected"},
            "rejected", True, "wrong number") in rows
    assert ("answer", aid, img, f"Question for {task}?", {"answer": "loco", "mode": "sample"},
            "rejected", True, None) in rows
    rows, err = _as(pg, other, "select id from rat.my_held()")
    assert err is None and rows == []


def test_removed_trainee_items_still_approvable(pg):
    ts = seed_fix_value(pg, "v4rm_r", values=["1"])
    ts2 = seed_fix_value(pg, "v4rm_s", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_FIX, ("v4rm_r", ["2"], "corrected", ts))
    _one(pg, admin, "select rat.set_volunteer(%s, null, false)", (t,))
    _, err = _as(pg, t, SUBMIT_FIX, ("v4rm_s", ["2"], "corrected", ts2))
    assert err is not None and err.pgcode == "42501"
    res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([pid],))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    assert _live(pg, "v4rm_r")[:3] == (["2"], "corrected", t)
    with pg.cursor() as cur:
        cur.execute("select changed_by::text, approved_by::text from rat.data_fix_history "
                    "where image_no = 'v4rm_r' order by id desc limit 1")
        assert cur.fetchone() == (t, admin)


def test_people_held_count(pg):
    task = seed_task(pg, "t4_people")
    seed_catalog(pg, "v4p_a", "a scene")
    ts1 = seed_fix_value(pg, "v4p_1", values=["1"])
    ts2 = seed_fix_value(pg, "v4p_2", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    p1 = _one(pg, t, SUBMIT_FIX, ("v4p_1", ["2"], "corrected", ts1))
    _one(pg, t, SUBMIT_FIX, ("v4p_1", ["3"], "corrected", ts1))      # withdraws p1
    _one(pg, t, SUBMIT_FIX, ("v4p_2", ["2"], "corrected", ts2))
    _one(pg, t, SUBMIT_ANSWER, (task, "v4p_a", "loco", None, None, "sample"))
    people = "select held_count from rat.people() where user_id = %s"
    assert _call(pg, admin, people, (t,)) == [(3,)]
    assert _call(pg, admin, people, (admin,)) == [(0,)]
    held = _call(pg, admin, "select kind, image_no, subject, before, proposed, current, stale "
                            "from rat.held_list(%s)", (t,))
    assert held == [
        ("fix", "v4p_1", "Loco number", {"values": ["1"], "status": "extracted"},
         {"values": ["3"], "status": "corrected"}, {"values": ["1"], "status": "extracted"}, False),
        ("fix", "v4p_2", "Loco number", {"values": ["1"], "status": "extracted"},
         {"values": ["2"], "status": "corrected"}, {"values": ["1"], "status": "extracted"}, False),
        ("answer", "v4p_a", f"Question for {task}?", {"shown_answer": None},
         {"answer": "loco", "mode": "sample"}, None, False),
    ]
    assert p1 not in [r[0] for r in _call(pg, admin, "select id from rat.held_list()")]
    _call(pg, admin, "select rat.reject_held('answer', array(select id from rat.held_list(%s) "
                     "where kind = 'answer'), null)", (t,))
    assert _call(pg, admin, people, (t,)) == [(2,)]


def test_held_admin_rpcs_and_tables_closed(pg):
    t, v = make_volunteer(pg), make_volunteer(pg, in_training=False)
    pg.commit()
    for uid in (t, v):
        for sql in ("select rat.approve_fix_review('{1}')",
                    "select rat.approve_model_answer('{1}')",
                    "select rat.reject_held('fix', '{1}', null)",
                    "select * from rat.held_list()"):
            _, err = _as(pg, uid, sql)
            assert err is not None and err.pgcode == "42501", (uid, sql)
    with pg.cursor() as cur:
        for tbl in ("rat.pending_fix_review", "rat.pending_model_answer"):
            cur.execute("select has_table_privilege('anon', %s, 'select'), "
                        "has_table_privilege('authenticated', %s, 'select'), "
                        "has_table_privilege('authenticated', %s, 'insert'), "
                        "c.relrowsecurity from pg_class c where c.oid = %s::regclass",
                        (tbl, tbl, tbl, tbl))
            assert cur.fetchone() == (False, False, False, True), tbl
        for fn in ("rat.submit_fix_review(text, text, text[], text, timestamptz)",
                   "rat.submit_fix_confirm(text, jsonb)",
                   "rat.submit_model_answer(text, text, text, bigint, text, text)",
                   "rat.approve_fix_review(bigint[], boolean, timestamptz)",
                   "rat.approve_model_answer(bigint[])",
                   "rat.reject_held(text, bigint[], text)",
                   "rat.held_list(uuid)",
                   "rat.my_held()"):
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn))
            assert cur.fetchone() == (False, True, True, ['search_path=""']), fn
        # The old two-argument approve_fix_review is gone (no ambiguous overload).
        cur.execute("select to_regprocedure('rat.approve_fix_review(bigint[], boolean)')")
        assert cur.fetchone() == (None,)
        fn = "rat._validate_model_answer(text, text, text, bigint, text, text)"
        cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                    "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                    "has_function_privilege('public', %s, 'EXECUTE')", (fn, fn, fn))
        assert cur.fetchone() == (False, False, False)


# --------------------------------------------------------------------------
# Task 5: undo data fix changes and withdraw model answers
# --------------------------------------------------------------------------

UNDO = "select rat.undo_fix_change(%s::bigint[])"
WITHDRAW = "select rat.withdraw_model_answers(%s::bigint[], %s)"
REVIEW = "select rat.review_fix_value(%s, %s, %s, %s, %s)"


def _review(pg, uid, image_no, values, status="corrected", fix_id="loco_number"):
    """Review through the RPC as uid; return the history id it wrote."""
    _one(pg, uid, REVIEW, (image_no, fix_id, values, status, _live(pg, image_no, fix_id)[3]))
    return _last_hist(pg, image_no, fix_id)[0]


def _last_hist(pg, image_no, fix_id="loco_number"):
    with pg.cursor() as cur:
        cur.execute("select id, source, undoes, undoes_from, changed_by::text, old_status, "
                    "new_status from rat.data_fix_history where image_no = %s and fix_id = %s "
                    "order by id desc limit 1", (image_no, fix_id))
        return cur.fetchone()


def _n_hist(pg, image_no, source=None):
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.data_fix_history where image_no = %s "
                    "and (%s::text is null or source = %s)", (image_no, source, source))
        return cur.fetchone()[0]


def _flags(pg, image_no, fix_id="loco_number"):
    with pg.cursor() as cur:
        cur.execute("select needs_recheck, description_hash, reviewed_by::text, reviewed_at "
                    "from rat.data_fix_value where image_no = %s and fix_id = %s",
                    (image_no, fix_id))
        return cur.fetchone()


def _owner_history(pg, image_no, old_values, new_values, old_status, new_status, by,
                   source="review", at=None, fix_id="loco_number"):
    """Insert a history row directly as the owner (history written before the
    migration, or an extractor row); returns its id."""
    with pg.cursor() as cur:
        cur.execute("insert into rat.data_fix_history (image_no, fix_id, old_values, new_values, "
                    "old_status, new_status, changed_by, changed_at, source) "
                    "values (%s, %s, %s::text[], %s::text[], %s, %s, %s, coalesce(%s, now()), %s) "
                    "returning id",
                    (image_no, fix_id, old_values, new_values, old_status, new_status, by, at,
                     source))
        hid = cur.fetchone()[0]
    pg.commit()
    return hid


def test_undo_single_review(pg):
    seed_fix_value(pg, "v5_one", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    hid = _review(pg, v, "v5_one", ["2"])
    res = _one(pg, admin, UNDO, ([hid],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_one")[:2] == (["1"], "extracted")
    uid, source, undoes, undoes_from, by, old_status, new_status = _last_hist(pg, "v5_one")
    assert (source, undoes, undoes_from, by, old_status, new_status) == (
        "undo", hid, hid, admin, "corrected", "extracted")
    assert uid > hid
    with pg.cursor() as cur:   # every setting is closed again
        cur.execute("select " + ", ".join(
            f"current_setting('rat.{s}', true)" for s in
            ("review_context", "review_source", "undoes", "undoes_from", "acting_for")))
        assert set(cur.fetchone()) <= {"", None}


def test_undo_back_to_extracted_clears_reviewer(pg):
    seed_fix_value(pg, "v5_clr", values=["1"])
    admin = make_admin(pg)
    pg.commit()
    hid = _review(pg, admin, "v5_clr", ["1"], "verified")
    assert _flags(pg, "v5_clr")[2] == admin
    _one(pg, admin, UNDO, ([hid],))
    _, _, reviewed_by, reviewed_at = _flags(pg, "v5_clr")
    assert (reviewed_by, reviewed_at) == (None, None)
    assert _live(pg, "v5_clr")[:2] == (["1"], "extracted")


def test_undo_restores_recheck_flags(pg):
    # extracted + needs_recheck: the old flag comes back (the fallback would say false)
    seed_fix_value(pg, "v5_rf_e", values=["1"], needs_recheck=True, description_hash="h_e")
    # verified, no recheck: the old flag comes back (the fallback would say true)
    seed_fix_value(pg, "v5_rf_v", values=["1"], status="verified", description_hash="h_v")
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    h_e = _review(pg, v, "v5_rf_e", ["2"])
    h_v = _review(pg, v, "v5_rf_v", ["3"])
    assert _flags(pg, "v5_rf_e")[:2] != (True, "h_e")
    res = _one(pg, admin, UNDO, ([h_e, h_v],))
    assert res == {"undone": 2, "changed_since": [], "skipped": []}
    assert _flags(pg, "v5_rf_e")[:2] == (True, "h_e")
    assert _flags(pg, "v5_rf_v")[:2] == (False, "h_v")
    assert _live(pg, "v5_rf_v")[:2] == (["1"], "verified")


def test_undo_old_history_sets_recheck(pg):
    seed_fix_value(pg, "v5_old", values=["2"], status="corrected", description_hash="h_now")
    w, u, admin = make_user(pg), make_user(pg), make_admin(pg)
    pg.commit()
    # Written before this migration: no old_* flags.
    _owner_history(pg, "v5_old", [], ["1"], "extracted", "verified", w,
                   at="2026-01-01T00:00:00Z")
    hid = _owner_history(pg, "v5_old", ["1"], ["2"], "verified", "corrected", u)
    res = _one(pg, admin, UNDO, ([hid],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_old")[:2] == (["1"], "verified")
    needs_recheck, description_hash, reviewed_by, reviewed_at = _flags(pg, "v5_old")
    assert (needs_recheck, description_hash, reviewed_by) == (True, "h_now", w)
    assert reviewed_at.isoformat().startswith("2026-01-01T00:00:00")


def test_batch_undo_unwinds_run_per_key(pg):
    seed_fix_value(pg, "v5_run", values=["1"], needs_recheck=True, description_hash="h_r")
    seed_fix_value(pg, "v5_run2", values=["5"])
    t, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    h1 = _review(pg, t, "v5_run", ["2"])
    h2 = _review(pg, t, "v5_run", ["3"])
    h3 = _review(pg, t, "v5_run2", ["6"])
    n_undo = _n_hist(pg, "v5_run", "undo")
    res = _one(pg, admin, UNDO, ([h2, h3, h1, h1],))
    assert res == {"undone": 2, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_run")[:2] == (["1"], "extracted")
    assert _flags(pg, "v5_run")[:3] == (True, "h_r", None)
    assert _live(pg, "v5_run2")[:2] == (["5"], "extracted")
    assert _n_hist(pg, "v5_run", "undo") == n_undo + 1
    assert _last_hist(pg, "v5_run")[1:4] == ("undo", h2, h1)
    assert _last_hist(pg, "v5_run2")[1:4] == ("undo", h3, h3)


def test_undo_partial_set_is_changed_since(pg):
    seed_fix_value(pg, "v5_part", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    h1 = _review(pg, v, "v5_part", ["2"])
    _review(pg, v, "v5_part", ["3"])
    n = _n_hist(pg, "v5_part")
    res = _one(pg, admin, UNDO, ([h1],))
    assert res == {"undone": 0, "changed_since": [h1], "skipped": []}
    assert _live(pg, "v5_part")[:2] == (["3"], "corrected")
    assert _n_hist(pg, "v5_part") == n


def test_undo_blocked_by_later_extractor_row(pg):
    seed_fix_value(pg, "v5_ext", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("select id from rat.data_fix_history where image_no = 'v5_ext' "
                    "and source = 'extractor'")
        h_insert = cur.fetchone()[0]
    hid = _review(pg, v, "v5_ext", ["2"])
    h_x = _owner_history(pg, "v5_ext", ["2"], ["2"], "corrected", "corrected", None,
                         source="extractor")
    res = _one(pg, admin, UNDO, ([hid],))
    assert res == {"undone": 0, "changed_since": [hid], "skipped": []}
    res = _one(pg, admin, UNDO, ([h_x, hid, h_insert, 999999999],))
    assert res["undone"] == 0 and res["changed_since"] == [hid]
    assert sorted(res["skipped"], key=lambda s: s["id"]) == [
        {"id": h_insert, "reason": "not a human change"},
        {"id": h_x, "reason": "not a human change"},
        {"id": 999999999, "reason": "no such change"},
    ]
    assert _live(pg, "v5_ext")[:2] == (["2"], "corrected")


def test_undo_value_mismatch_and_missing_row(pg):
    seed_fix_value(pg, "v5_mm", values=["2"], status="corrected")
    seed_fix_value(pg, "v5_gone", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    # History says the value became 9, but it is 2 now: belt-and-braces check.
    h_mm = _owner_history(pg, "v5_mm", ["1"], ["9"], "verified", "corrected", v)
    h_gone = _review(pg, v, "v5_gone", ["2"])
    with pg.cursor() as cur:
        cur.execute("delete from rat.data_fix_value where image_no = 'v5_gone'")
    pg.commit()
    res = _one(pg, admin, UNDO, ([h_gone, h_mm],))
    assert res == {"undone": 0, "changed_since": [h_mm],
                   "skipped": [{"id": h_gone, "reason": "no data_fix_value"}]}
    assert _live(pg, "v5_mm")[:2] == (["2"], "corrected")


def _hist_rows(pg, image_no, after_id, fix_id="loco_number"):
    with pg.cursor() as cur:
        cur.execute('select source, undoes, undoes_from, old_values, new_values, old_status, '
                    'new_status, changed_by::text, old_needs_recheck, old_description_hash, '
                    'approved_by, model_answer_id from rat.data_fix_history '
                    'where image_no = %s and fix_id = %s and id > %s order by id',
                    (image_no, fix_id, after_id))
        return cur.fetchall()


def test_noop_undo_run_still_records_undo_row(pg):
    # A -> B -> A, both ids given: the value ends where it is now, yet the undo
    # must still leave exactly one undo row so the run counts as undone.
    seed_fix_value(pg, "v5_noop", values=["1"], status="verified", description_hash="h_n")
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    h1 = _review(pg, v, "v5_noop", ["2"])
    h2 = _review(pg, v, "v5_noop", ["1"], "verified")
    flags = _flags(pg, "v5_noop")
    res = _one(pg, admin, UNDO, ([h1, h2],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_noop")[:2] == (["1"], "verified")
    assert _hist_rows(pg, "v5_noop", h2) == [
        ("undo", h2, h1, ["1"], ["1"], "verified", "verified", admin, flags[0], flags[1],
         None, None)]
    assert _flags(pg, "v5_noop")[:2] == (False, "h_n")
    # The run is now covered by that undo row: undoing h1 again is skipped.
    res = _one(pg, admin, UNDO, ([h1],))
    assert res == {"undone": 0, "changed_since": [],
                   "skipped": [{"id": h1, "reason": "already undone"}]}


def test_undo_change_with_its_undo_records_one_row(pg):
    # R, then its undo U; undoing {R, U} together leaves the value where it is
    # and records one undo row covering both.
    seed_fix_value(pg, "v5_ru", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    r = _review(pg, v, "v5_ru", ["2"])
    _one(pg, admin, UNDO, ([r],))
    u = _last_hist(pg, "v5_ru")[0]
    res = _one(pg, admin, UNDO, ([u, r],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_ru")[:2] == (["1"], "extracted")
    rows = _hist_rows(pg, "v5_ru", u)
    assert len(rows) == 1
    assert rows[0][:8] == ("undo", u, r, ["1"], ["1"], "extracted", "extracted", admin)


def test_undo_of_undo_reapplies(pg):
    seed_fix_value(pg, "v5_uu", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    h1 = _review(pg, v, "v5_uu", ["2"])
    _one(pg, admin, UNDO, ([h1],))
    h2 = _last_hist(pg, "v5_uu")[0]
    res = _one(pg, admin, UNDO, ([h2],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v5_uu")[:3] == (["2"], "corrected", v)
    assert _flags(pg, "v5_uu")[0] is False
    h3, source, undoes, undoes_from = _last_hist(pg, "v5_uu")[:4]
    assert (source, undoes, undoes_from) == ("undo", h2, h2)
    # The first review is now followed by two undos: undoing it alone is blocked.
    res = _one(pg, admin, UNDO, ([h1],))
    assert res == {"undone": 0, "changed_since": [h1], "skipped": []}


def _filled_answer(pg, uid, task, img, run=None):
    rows, err = _as(pg, uid, "select rat.save_model_answer(%s, %s, 'station', null, null, 'sample')",
                    (task, img))
    assert err is None, err
    res = rows[0][0]
    assert res["filled"] == LINKS["fix_ids"]
    return res["answer_id"]


def test_withdraw_answer_undoes_fill(pg):
    task, img = seed_task(pg, "t5_wd", links=LINKS), "v5_wd"
    _linked_photo(pg, img)
    run = seed_run(pg, task, predictions={img: ("station", 0.95)})
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    aid = _filled_answer(pg, v, task, img)
    prog = "select n_blind from rat.model_check_progress() where run_id = %s"
    assert _call(pg, admin, prog, (run,)) == [(1,)]
    with pg.cursor() as cur:
        cur.execute("select id from rat.data_fix_history where model_answer_id = %s order by id",
                    (aid,))
        fills = [r[0] for r in cur.fetchall()]
    res = _one(pg, admin, WITHDRAW, ([aid], "misread the photo"))
    assert res == {"withdrawn": 1, "skipped": [], "fills_left": []}
    for fix_id in LINKS["fix_ids"]:
        assert _live(pg, img, fix_id)[:3] == ([], "extracted", None)
        assert _last_hist(pg, img, fix_id)[1] == "undo"
        assert _last_hist(pg, img, fix_id)[2] in fills
    with pg.cursor() as cur:
        cur.execute("select withdrawn_by::text, note from rat.model_answer_withdrawal "
                    "where answer_id = %s", (aid,))
        assert cur.fetchone() == (admin, "misread the photo")
        cur.execute("select count(*) from rat.model_answer where id = %s", (aid,))
        assert cur.fetchone()[0] == 1   # the answer itself is never touched
    assert _call(pg, admin, prog, (run,)) == [(0,)]


def test_withdraw_legacy_fill_by_match(pg):
    """An answer saved before this migration: its fill has no model_answer_id
    and is found by image_no / model_check / changed_by / changed_at."""
    task, img = seed_task(pg, "t5_leg", links=LINKS), "v5_leg"
    _linked_photo(pg, img)
    u, admin = make_user(pg), make_admin(pg)
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode, answered_by) "
                    "values (%s, %s, 'station', 'sample', %s) returning id", (task, img, u))
        aid = cur.fetchone()[0]
        for k, val in (("review_context", "on"), ("review_source", "model_check"),
                       ("acting_for", u)):
            cur.execute("select set_config(%s, %s, true)", (f"rat.{k}", val))
        cur.execute("""update rat.data_fix_value set "values" = '{}', status = 'no_loco' """
                    "where image_no = %s", (img,))
    pg.commit()
    res = _one(pg, admin, WITHDRAW, ([aid], None))
    assert res == {"withdrawn": 1, "skipped": [], "fills_left": []}
    for fix_id in LINKS["fix_ids"]:
        assert _live(pg, img, fix_id)[:2] == ([], "extracted")


def test_withdraw_fill_changed_since_left(pg):
    task, img = seed_task(pg, "t5_wl", links=LINKS), "v5_wl"
    _linked_photo(pg, img)
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    aid = _filled_answer(pg, v, task, img)
    _review(pg, v, img, ["7"], "corrected", fix_id="loco_number")
    res = _one(pg, admin, WITHDRAW, ([aid], None))
    assert res == {"withdrawn": 1, "skipped": [],
                   "fills_left": [{"image_no": img, "fix_id": "loco_number"}]}
    assert _live(pg, img, "loco_number")[:2] == (["7"], "corrected")
    assert _live(pg, img, "loco_name")[:2] == ([], "extracted")
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.model_answer_withdrawal where answer_id = %s", (aid,))
        assert cur.fetchone()[0] == 1


def test_withdraw_twice_skips(pg):
    task, img = seed_task(pg, "t5_w2", links=LINKS), "v5_w2"
    _linked_photo(pg, img)
    admin = make_admin(pg)
    pg.commit()
    aid = _filled_answer(pg, admin, task, img)
    assert _one(pg, admin, WITHDRAW, ([aid], None))["withdrawn"] == 1
    n = _n_hist(pg, img)
    res = _one(pg, admin, WITHDRAW, ([aid, 999999999], "again"))
    assert res == {"withdrawn": 0, "fills_left": [],
                   "skipped": [{"id": aid, "reason": "already withdrawn"},
                               {"id": 999999999, "reason": "no such answer"}]}
    assert _n_hist(pg, img) == n
    with pg.cursor() as cur:
        cur.execute("select note from rat.model_answer_withdrawal where answer_id = %s", (aid,))
        assert cur.fetchone() == (None,)


def test_undo_withdraw_admin_only(pg):
    seed_fix_value(pg, "v5_ao", values=["1"])
    users = {who: _make(pg, who) for who in ("volunteer", "trainee", "removed", "stranger")}
    pg.commit()
    hid = _review(pg, users["volunteer"], "v5_ao", ["2"])
    for who, uid in users.items():
        for sql, args in ((UNDO, ([hid],)), (WITHDRAW, ([1], None))):
            _, err = _as(pg, uid, sql, args)
            assert err is not None and err.pgcode == "42501", (who, sql)
    assert _live(pg, "v5_ao")[:2] == (["2"], "corrected")
    with pg.cursor() as cur:
        for fn in ("rat.undo_fix_change(bigint[])", "rat.withdraw_model_answers(bigint[], text)"):
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn))
            assert cur.fetchone() == (False, True, True, ['search_path=""']), fn
        for fn in ("rat._undo_fix_keys(bigint[])", "rat._fix_history_undone(bigint, bigint[])"):
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "has_function_privilege('public', %s, 'EXECUTE'), "
                        "has_function_privilege('picaloco_agent', %s, 'EXECUTE')",
                        (fn, fn, fn, fn))
            assert cur.fetchone() == (False, False, False, False), fn


def test_undo_keeps_recheck_flag_set_after_change(pg):
    # verified -> corrected (old_needs_recheck false); the description then
    # changes and the extractor flags the row (no history row). Undo must not
    # hide that changed description again.
    seed_fix_value(pg, "v7_rc", values=["1"], status="verified", description_hash="h_v")
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    hid = _review(pg, v, "v7_rc", ["2"])
    n = _n_hist(pg, "v7_rc")
    with pg.cursor() as cur:   # the extractor, as the owner: a non-guarded column
        cur.execute("update rat.data_fix_value set needs_recheck = true "
                    "where image_no = 'v7_rc' and fix_id = 'loco_number'")
    pg.commit()
    assert _n_hist(pg, "v7_rc") == n
    res = _one(pg, admin, UNDO, ([hid],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "v7_rc")[:2] == (["1"], "verified")
    assert _flags(pg, "v7_rc")[0] is True


def test_undo_already_undone_is_skipped(pg):
    seed_fix_value(pg, "v7_twice", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    hid = _review(pg, v, "v7_twice", ["2"])
    assert _one(pg, admin, UNDO, ([hid],))["undone"] == 1
    n = _n_hist(pg, "v7_twice")
    res = _one(pg, admin, UNDO, ([hid],))
    assert res == {"undone": 0, "changed_since": [],
                   "skipped": [{"id": hid, "reason": "already undone"}]}
    assert _n_hist(pg, "v7_twice") == n
    assert _live(pg, "v7_twice")[:2] == (["1"], "extracted")


def test_withdraw_after_fill_undone_leaves_nothing(pg):
    task, img = seed_task(pg, "t7_wu", links=LINKS), "v7_wu"
    _linked_photo(pg, img)
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    aid = _filled_answer(pg, v, task, img)
    with pg.cursor() as cur:
        cur.execute("select id from rat.data_fix_history where model_answer_id = %s "
                    "and fix_id = 'loco_number'", (aid,))
        fill = cur.fetchone()[0]
    assert _one(pg, admin, UNDO, ([fill],))["undone"] == 1
    res = _one(pg, admin, WITHDRAW, ([aid], None))
    assert res == {"withdrawn": 1, "skipped": [], "fills_left": []}
    for fix_id in LINKS["fix_ids"]:
        assert _live(pg, img, fix_id)[:2] == ([], "extracted")


def test_preservation_checksum_untouched_by_migration(pg):
    from data_fix_preservation_check import CHECKSUM_SQL
    seed_fix_value(pg, "v5_pc", values=["4"], status="verified")
    pg.commit()
    with pg.cursor() as cur:
        cur.execute(CHECKSUM_SQL)
        before = cur.fetchone()
    pg.commit()
    assert before[0] >= 1
    apply_sql_file(VOLUNTEER_SQL)
    with pg.cursor() as cur:
        cur.execute(CHECKSUM_SQL)
        assert cur.fetchone() == before


# --------------------------------------------------------------------------
# Task 6: change list for the admin audit page
# --------------------------------------------------------------------------

CHANGES = ("select * from rat.change_list(p_user => %s::uuid, p_from => %s::timestamptz, "
           "p_to => %s::timestamptz, p_kinds => %s::text[], p_after => %s, p_limit => %s)")
CURSOR_RE = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\|[a-z_]+\|\d+$"


def _changes(pg, admin, user=None, frm=None, to=None, kinds=None, after=None, limit=100,
             tz=None):
    """change_list as admin (optionally in session time zone tz); a list of
    dicts (column name -> value)."""
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        if tz:
            cur.execute("select set_config('timezone', %s, true)", (tz,))
        cur.execute(CHANGES, (user, frm, to, kinds, after, limit))
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    pg.commit()
    for r in rows:
        for k in ("by_user", "approved_by"):
            r[k] = None if r[k] is None else str(r[k])
    return rows


def test_change_list_kinds_and_people(pg):
    import re
    task = seed_task(pg, "t6_kinds", links=LINKS)
    plain = seed_task(pg, "t6_kinds_plain")
    img_f = "v6k_fill"
    _linked_photo(pg, img_f)
    seed_catalog(pg, "v6k_ans", "a scene")
    seed_catalog(pg, "v6k_wd", "a scene")
    seed_catalog(pg, "v6k_held", "a scene")
    seed_fix_value(pg, "v6k_r", values=["1"])
    seed_fix_value(pg, "v6k_u", values=["1"])
    ts1 = seed_fix_value(pg, "v6k_h1", values=["1"])
    ts2 = seed_fix_value(pg, "v6k_h2", values=["1"])
    v, t, admin = make_volunteer(pg, in_training=False), make_volunteer(pg), make_admin(pg)
    pg.commit()
    extractor = _owner_history(pg, "v6k_r", ["1"], ["9"], "extracted", "extracted", v,
                               source="extractor")

    hr = _review(pg, v, "v6k_r", ["2"])                               # fix_review
    hu0 = _review(pg, v, "v6k_u", ["3"])
    _one(pg, admin, UNDO, ([hu0],))                                    # fix_undo
    hu = _last_hist(pg, "v6k_u")[0]
    aid_f = _filled_answer(pg, v, task, img_f)                         # answer + fix_model_check
    aid = _answer(pg, v, plain, "v6k_ans", "loco")                     # answer
    aid_w = _answer(pg, v, plain, "v6k_wd", "station")
    _one(pg, admin, WITHDRAW, ([aid_w], "misread"))                    # answer_withdrawn
    p1 = _one(pg, t, SUBMIT_FIX, ("v6k_h1", ["5"], "corrected", ts1))  # held_submitted x3
    p2 = _one(pg, t, SUBMIT_FIX, ("v6k_h2", ["6"], "corrected", ts2))
    pa = _one(pg, t, SUBMIT_ANSWER, (plain, "v6k_held", "loco", None, None, "sample"))
    _one(pg, admin, "select rat.approve_fix_review(%s)", ([p1],))     # held_approved
    _one(pg, admin, "select rat.reject_held('fix', %s, 'wrong number')", ([p2],))  # held_rejected
    happ = _last_hist(pg, "v6k_h1")[0]

    def key(r):
        return (r["kind"], r["ref_table"], r["ref_id"])

    by_v = _changes(pg, admin, v)
    with pg.cursor() as cur:
        cur.execute("select id from rat.data_fix_history where model_answer_id = %s order by id",
                    (aid_f,))
        fills = [r[0] for r in cur.fetchall()]
    assert len(fills) == 2
    assert sorted(map(key, by_v)) == sorted(
        [("fix_review", "data_fix_history", hr), ("fix_review", "data_fix_history", hu0),
         ("answer", "model_answer", aid_f), ("answer", "model_answer", aid),
         ("answer", "model_answer", aid_w)]
        + [("fix_model_check", "data_fix_history", h) for h in fills])
    assert extractor not in [r["ref_id"] for r in by_v]   # extractor rows never listed
    assert all(r["by_user"] == v for r in by_v)
    for r in by_v:
        assert re.match(CURSOR_RE, r["cursor"]), r["cursor"]
        assert r["cursor"] == (r["at"].astimezone(datetime.timezone.utc)
                               .strftime("%Y-%m-%dT%H:%M:%S.%f")
                               + f"|{r['ref_table']}|{r['ref_id']}")

    rev = next(r for r in by_v if r["ref_id"] == hr)
    assert (rev["image_no"], rev["subject"], rev["before"], rev["after"], rev["approved_by"],
            rev["undone"]) == ("v6k_r", "Loco number", {"values": ["1"], "status": "extracted"},
                               {"values": ["2"], "status": "corrected"}, None, False)
    ans = next(r for r in by_v if r["kind"] == "answer" and r["ref_id"] == aid)
    assert (ans["image_no"], ans["subject"], ans["before"], ans["after"], ans["undone"]) == (
        "v6k_ans", f"Question for {plain}?", {"shown_answer": None},
        {"answer": "loco", "mode": "sample"}, False)

    by_admin = _changes(pg, admin, admin)
    assert sorted(map(key, by_admin)) == sorted([
        ("fix_undo", "data_fix_history", hu),
        ("answer_withdrawn", "model_answer_withdrawal", aid_w),
        ("held_approved", "pending_fix_review", p1),
        ("held_rejected", "pending_fix_review", p2)])
    und = next(r for r in by_admin if r["ref_id"] == hu and r["kind"] == "fix_undo")
    assert (und["before"], und["after"], und["undone"]) == (
        {"values": ["3"], "status": "corrected"}, {"values": ["1"], "status": "extracted"}, False)
    wd = next(r for r in by_admin if r["kind"] == "answer_withdrawn")
    assert (wd["image_no"], wd["subject"], wd["after"], wd["approved_by"], wd["undone"]) == (
        "v6k_wd", f"Question for {plain}?", {"note": "misread"}, None, False)
    rej = next(r for r in by_admin if r["kind"] == "held_rejected")
    assert (rej["image_no"], rej["subject"], rej["before"], rej["after"], rej["approved_by"]) == (
        "v6k_h2", "Loco number", {"values": ["1"], "status": "extracted"},
        {"values": ["6"], "status": "corrected", "note": "wrong number"}, None)
    appr = next(r for r in by_admin if r["kind"] == "held_approved")
    assert (appr["after"], appr["approved_by"], appr["undone"]) == (
        {"values": ["5"], "status": "corrected"}, None, False)

    by_t = _changes(pg, admin, t)
    assert sorted(map(key, by_t)) == sorted([
        ("held_submitted", "pending_fix_review", p1),
        ("held_submitted", "pending_fix_review", p2),
        ("held_submitted", "pending_model_answer", pa),
        ("fix_review", "data_fix_history", happ)])
    sub = next(r for r in by_t if r["ref_table"] == "pending_model_answer")
    assert (sub["image_no"], sub["before"], sub["after"], sub["approved_by"]) == (
        "v6k_held", {"shown_answer": None}, {"answer": "loco", "mode": "sample"}, None)
    approved = next(r for r in by_t if r["kind"] == "fix_review")
    assert approved["approved_by"] == admin

    only = _changes(pg, admin, admin, kinds=["fix_undo", "held_rejected"])
    assert {r["kind"] for r in only} == {"fix_undo", "held_rejected"}
    assert _changes(pg, admin, admin, kinds=[]) == []
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute(CHANGES, (None, None, None, ["extractor"], None, 10))
        assert ei.value.pgcode == "22023"
    pg.rollback()


def test_change_list_paging(pg):
    for i in range(3):
        seed_fix_value(pg, f"v6p_{i}", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    ids = [_review(pg, v, f"v6p_{i}", [str(i + 2)]) for i in range(3)]
    same = datetime.datetime(2020, 5, 5, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)
    ids += [_owner_history(pg, "v6p_x", ["1"], ["2"], "extracted", "corrected", v, at=same)
            for _ in range(2)]   # two events at the same instant: tie broken by ref_id DESC

    full = _changes(pg, admin, v, limit=500)
    assert len(full) == 5 and sorted(r["ref_id"] for r in full) == sorted(ids)
    assert [r["ref_id"] for r in full] == [ids[2], ids[1], ids[0], ids[4], ids[3]]
    pages, after = [], None
    while True:
        page = _changes(pg, admin, v, after=after, limit=2)
        if not page:
            break
        assert len(page) <= 2
        pages.append(page)
        after = page[-1]["cursor"]
    assert [len(p) for p in pages] == [2, 2, 1]
    assert [r["cursor"] for p in pages for r in p] == [r["cursor"] for r in full]
    # The cursor is in UTC whatever the session time zone, so it pages the same.
    pages, after = [], None
    while True:
        page = _changes(pg, admin, v, after=after, limit=2, tz="America/New_York")
        if not page:
            break
        pages += page
        after = page[-1]["cursor"]
    assert [r["cursor"] for r in pages] == [r["cursor"] for r in full]

    assert len(_changes(pg, admin, v, limit=0)) == 1      # clamped to 1..500
    assert len(_changes(pg, admin, v, limit=-5)) == 1
    assert len(_changes(pg, admin, v, limit=None)) == 5
    for bad in ("nope", "2020-05-05T12:00:00.123456|data_fix_history", "x|y|z",
                "2020-13-05T12:00:00.123456|data_fix_history|1"):
        pg.commit()
        as_user(pg, admin)
        with pg.cursor() as cur:
            with pytest.raises(psycopg2.Error) as ei:
                cur.execute(CHANGES, (v, None, None, None, bad, 10))
            assert ei.value.pgcode == "22023", bad
        pg.rollback()


def test_change_list_dates(pg):
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    utc = datetime.timezone.utc
    t1, t2, t3 = (datetime.datetime(2020, 1, 1, tzinfo=utc),
                  datetime.datetime(2020, 6, 1, tzinfo=utc),
                  datetime.datetime(2021, 1, 1, tzinfo=utc))
    h1, h2, h3 = (_owner_history(pg, "v6d", ["1"], ["2"], "extracted", "corrected", v, at=at)
                  for at in (t1, t2, t3))

    def ids(**kw):
        return [r["ref_id"] for r in _changes(pg, admin, v, **kw)]

    assert ids() == [h3, h2, h1]
    assert ids(frm=datetime.datetime(2020, 3, 1, tzinfo=utc)) == [h3, h2]
    assert ids(to=datetime.datetime(2020, 12, 31, tzinfo=utc)) == [h2, h1]
    assert ids(frm=datetime.datetime(2020, 3, 1, tzinfo=utc),
               to=datetime.datetime(2020, 12, 31, tzinfo=utc)) == [h2]
    assert ids(frm=t2, to=t3) == [h2]     # from inclusive, to exclusive


def test_change_list_undone_flags(pg):
    task, plain = seed_task(pg, "t6_und", links=LINKS), seed_task(pg, "t6_und_plain")
    img_f = "v6u_fill"
    _linked_photo(pg, img_f)
    seed_catalog(pg, "v6u_a", "a scene")
    seed_catalog(pg, "v6u_b", "a scene")
    seed_fix_value(pg, "v6u_r", values=["1"])
    seed_fix_value(pg, "v6u_k", values=["1"])
    v, admin = make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()

    def undone(user=None):
        return {(r["kind"], r["ref_id"]): r["undone"]
                for r in _changes(pg, admin, user or v, limit=500)}

    h1 = _review(pg, v, "v6u_r", ["2"])
    hk = _review(pg, v, "v6u_k", ["7"])           # kept: never undone
    _one(pg, admin, UNDO, ([h1],))
    u1 = _last_hist(pg, "v6u_r")[0]
    assert undone()[("fix_review", h1)] is True
    assert undone()[("fix_review", hk)] is False
    assert undone(admin)[("fix_undo", u1)] is False
    _one(pg, admin, UNDO, ([u1],))                # undoing the undo: h1 counts again
    u2 = _last_hist(pg, "v6u_r")[0]
    assert undone()[("fix_review", h1)] is False
    flags = undone(admin)
    assert (flags[("fix_undo", u1)], flags[("fix_undo", u2)]) == (True, False)
    _one(pg, admin, UNDO, ([u2],))                # and once more: h1 undone again
    u3 = _last_hist(pg, "v6u_r")[0]
    flags = undone(admin)
    assert (flags[("fix_undo", u1)], flags[("fix_undo", u2)], flags[("fix_undo", u3)]) == (
        False, True, False)
    assert undone()[("fix_review", h1)] is True

    aid_f = _filled_answer(pg, v, task, img_f)
    aid_a = _answer(pg, v, plain, "v6u_a", "loco")
    aid_b = _answer(pg, v, plain, "v6u_b", "loco")
    _one(pg, admin, WITHDRAW, ([aid_f, aid_a], None))
    flags = undone()
    assert (flags[("answer", aid_f)], flags[("answer", aid_a)], flags[("answer", aid_b)]) == (
        True, True, False)
    fills = [k for k in flags if k[0] == "fix_model_check"]
    assert len(fills) == 2 and all(flags[k] for k in fills)
    assert not any(r["undone"] for r in _changes(pg, admin, admin,
                                                 kinds=["answer_withdrawn"], limit=500))


def test_change_list_admin_only(pg):
    users = {who: _make(pg, who) for who in ("volunteer", "trainee", "removed", "stranger")}
    pg.commit()
    for who, uid in users.items():
        _, err = _as(pg, uid, "select * from rat.change_list()")
        assert err is not None and err.pgcode == "42501", who
    fn = "rat.change_list(uuid, timestamptz, timestamptz, text[], text, integer)"
    with pg.cursor() as cur:
        cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                    "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                    "has_function_privilege('public', %s, 'EXECUTE'), "
                    "p.prosecdef, p.proconfig "
                    "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn, fn))
        assert cur.fetchone() == (False, True, False, True, ['search_path=""'])


# --------------------------------------------------------------------------
# Wave A: ledger minors
# --------------------------------------------------------------------------

def test_account_status_email_lookup_is_deterministic(pg):
    admin = make_admin(pg)
    tag = uuid.uuid4().hex[:8]
    newer, older = str(uuid.uuid4()), str(uuid.uuid4())
    lo, hi = f"00000000-0000-4000-8000-{tag}0000", f"ffffffff-0000-4000-8000-{tag}0000"
    with pg.cursor() as cur:
        # Inserted newest first, so a plain scan meets the wrong one first.
        cur.execute("insert into auth.users (id, email, created_at) values "
                    "(%s, %s, '2026-06-01T00:00:00Z'), (%s, %s, '2026-01-01T00:00:00Z')",
                    (newer, f"Dup_{tag}@example.test", older, f" dup_{tag}@example.test"))
        # Same created_at: the lower id wins.
        cur.execute("insert into auth.users (id, email, created_at) values "
                    "(%s, %s, '2026-01-01T00:00:00Z'), (%s, %s, '2026-01-01T00:00:00Z')",
                    (hi, f"Tie_{tag}@example.test", lo, f"tie_{tag}@example.test"))
    pg.commit()
    r = _call(pg, admin, "select rat.account_status(%s)", (f"dup_{tag}@example.test",))[0][0]
    assert r == {"user_id": older, "kind": "other"}
    r = _call(pg, admin, "select rat.account_status(%s)", (f"tie_{tag}@example.test",))[0][0]
    assert r == {"user_id": lo, "kind": "other"}


def test_set_volunteer_remove_again_keeps_first_removal(pg):
    a1, a2, v = make_admin(pg), make_admin(pg), make_volunteer(pg)
    _call(pg, a1, "select rat.set_volunteer(%s, null, false)", (v,))
    _, _, by1, at1, _ = _row(pg, v)
    assert str(by1) == a1 and at1 is not None
    _call(pg, a2, "select rat.set_volunteer(%s, p_active => false)", (v,))
    _, _, by2, at2, _ = _row(pg, v)
    assert (str(by2), at2) == (a1, at1)


def test_withdraw_race_reports_already_withdrawn(pg):
    """Two withdrawals of one answer at once: the second one's pre-check cannot
    see the first (not yet committed), so its insert hits the primary key. That
    must read as 'already withdrawn', not the raw constraint message."""
    import threading
    import time
    task, img = seed_task(pg, "wa_race", links=LINKS), "wa_race"
    _linked_photo(pg, img)
    admin = make_admin(pg)
    pg.commit()
    aid = _filled_answer(pg, admin, task, img)
    first, caller, watch = _connect(), _connect(), _connect(autocommit=True)
    out = {}

    def withdraw():
        try:
            as_user(caller, admin)
            with caller.cursor() as cur:
                cur.execute(WITHDRAW, ([aid], "second"))
                out["res"] = cur.fetchone()[0]
            caller.commit()
        except Exception as exc:  # reported below
            out["err"] = exc

    try:
        with first.cursor() as cur:
            cur.execute("insert into rat.model_answer_withdrawal (answer_id, withdrawn_by, note) "
                        "values (%s, %s, 'first')", (aid, admin))
        th = threading.Thread(target=withdraw)
        th.start()
        deadline = time.monotonic() + 20
        while True:   # wait until the caller is blocked on the first insert
            with watch.cursor() as cur:
                cur.execute("select wait_event_type from pg_stat_activity where pid = %s",
                            (caller.get_backend_pid(),))
                if cur.fetchone()[0] == "Lock":
                    break
            assert time.monotonic() < deadline, "caller never blocked"
            time.sleep(0.05)
        first.commit()
        th.join(20)
        assert "err" not in out, out.get("err")
        assert out["res"] == {"withdrawn": 0, "fills_left": [],
                              "skipped": [{"id": aid, "reason": "already withdrawn"}]}
    finally:
        for conn in (first, caller, watch):
            conn.close()
    with pg.cursor() as cur:
        cur.execute("select note from rat.model_answer_withdrawal where answer_id = %s", (aid,))
        assert cur.fetchone() == ("first",)


def _held_row(pg, admin, uid, pid):
    rows = _one(pg, admin, "select json_agg(h) from rat.held_list(%s) h", (uid,))
    return next(r for r in rows if r["id"] == pid)


def test_held_list_current_updated_at(pg):
    task, img = seed_task(pg, "wa_hcu"), "wa_hcu_a"
    seed_catalog(pg, img, "a scene")
    ts = seed_fix_value(pg, "wa_hcu_r", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    fid = _one(pg, t, SUBMIT_FIX, ("wa_hcu_r", ["2"], "corrected", ts))
    aid = _one(pg, t, SUBMIT_ANSWER, (task, img, "loco", None, None, "sample"))
    rows, err = _as(pg, admin, "select kind, id, current_updated_at from rat.held_list(%s)", (t,))
    assert err is None, err
    assert rows == [("fix", fid, ts), ("answer", aid, None)]


def _stale_held(pg, image_no):
    """A trainee's held fix that a volunteer then changed: (trainee, volunteer,
    admin, held id)."""
    ts = seed_fix_value(pg, image_no, values=["1"])
    t, v, admin = make_volunteer(pg), make_volunteer(pg, in_training=False), make_admin(pg)
    pg.commit()
    pid = _one(pg, t, SUBMIT_FIX, (image_no, ["2"], "corrected", ts))
    _review(pg, v, image_no, ["5"])
    return t, v, admin, pid


APPROVE_SEEN = ("select rat.approve_fix_review(%s, p_force => true, "
                "p_seen_updated_at => %s::timestamptz)")


def test_approve_anyway_against_seen_value(pg):
    t, v, admin, pid = _stale_held(pg, "wa_seen")
    seen = _held_row(pg, admin, t, pid)
    assert seen["stale"] is True and seen["current"] == {"values": ["5"], "status": "corrected"}
    res = _one(pg, admin, APPROVE_SEEN, ([pid], seen["current_updated_at"]))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    assert _live(pg, "wa_seen")[:3] == (["2"], "corrected", t)


def test_approve_anyway_changed_after_seen_is_stale(pg):
    t, v, admin, pid = _stale_held(pg, "wa_seen2")
    seen = _held_row(pg, admin, t, pid)["current_updated_at"]
    _review(pg, v, "wa_seen2", ["6"])      # changed after the admin looked
    res = _one(pg, admin, APPROVE_SEEN, ([pid], seen))
    assert res == {"approved": 0, "skipped": [],
                   "stale": [{"id": pid, "current_values": ["6"], "current_status": "corrected"}]}
    assert _pending(pg, "pending_fix_review", pid)[0] == "pending"
    assert _live(pg, "wa_seen2")[:3] == (["6"], "corrected", v)
    # Without a seen value, force keeps today's behaviour: approve over the live row.
    res = _one(pg, admin, APPROVE_SEEN, ([pid], None))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    assert _live(pg, "wa_seen2")[:3] == (["2"], "corrected", t)


def test_held_rows_cannot_be_deleted(pg):
    task, img = seed_task(pg, "wa_nodel"), "wa_nodel_a"
    seed_catalog(pg, img, "a scene")
    ts = seed_fix_value(pg, "wa_nodel_r", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    fid = _one(pg, t, SUBMIT_FIX, ("wa_nodel_r", ["2"], "corrected", ts))
    aid = _one(pg, t, SUBMIT_ANSWER, (task, img, "loco", None, None, "sample"))
    for table, pid in (("pending_fix_review", fid), ("pending_model_answer", aid)):
        for sql in (f"delete from rat.{table} where id = {pid}", f"truncate rat.{table}"):
            with pg.cursor() as cur:
                cur.execute("savepoint s")
                try:
                    with pytest.raises(psycopg2.Error) as ei:
                        cur.execute(sql)
                    assert ei.value.pgcode == "23001", sql
                    assert "held work is kept: decide it, never delete it" in str(ei.value)
                finally:
                    cur.execute("rollback to savepoint s")
    pg.commit()
    # Decisions (UPDATEs) still work.
    res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([fid],))
    assert res == {"approved": 1, "stale": [], "skipped": []}
    res = _one(pg, admin, "select rat.reject_held('answer', %s, 'no')", ([aid],))
    assert res == {"rejected": 1, "skipped": []}
    assert _pending(pg, "pending_fix_review", fid)[0] == "approved"
    assert _pending(pg, "pending_model_answer", aid) == ("rejected", admin, "no")


def test_approve_answer_missing_photo_skipped(pg):
    task, gone, kept = seed_task(pg, "wa_ansk"), "wa_ansk_gone", "wa_ansk_kept"
    seed_catalog(pg, gone, "a scene")
    seed_catalog(pg, kept, "a scene")
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    p_gone = _one(pg, t, SUBMIT_ANSWER, (task, gone, "loco", None, None, "sample"))
    p_kept = _one(pg, t, SUBMIT_ANSWER, (task, kept, "loco", None, None, "sample"))
    with pg.cursor() as cur:
        cur.execute("delete from rat.catalog where image_no = %s", (gone,))
    pg.commit()
    res = _one(pg, admin, "select rat.approve_model_answer(%s)", ([p_kept, p_gone],))
    assert res == {"approved": 1,
                   "skipped": [{"id": p_gone, "reason": f"no catalog photo {gone}"}]}
    assert _pending(pg, "pending_model_answer", p_gone)[0] == "pending"
    assert _pending(pg, "pending_model_answer", p_kept)[:2] == ("approved", admin)
    with pg.cursor() as cur:
        cur.execute("select image_no from rat.model_answer where task_id = %s", (task,))
        assert cur.fetchall() == [(kept,)]


def test_approve_fix_unexpected_error_skipped(pg):
    """approve_fix_review's WHEN OTHERS: any other error in one item becomes a
    'skipped' entry with the message. No schema path raises one today, so a
    test-only trigger on data_fix_value raises for one image_no."""
    ts1 = seed_fix_value(pg, "wa_boom", values=["1"])
    ts2 = seed_fix_value(pg, "wa_fine", values=["1"])
    t, admin = make_volunteer(pg), make_admin(pg)
    pg.commit()
    p1 = _one(pg, t, SUBMIT_FIX, ("wa_boom", ["2"], "corrected", ts1))
    p2 = _one(pg, t, SUBMIT_FIX, ("wa_fine", ["3"], "corrected", ts2))
    with pg.cursor() as cur:
        cur.execute("create function public.wa_boom() returns trigger language plpgsql as $$ "
                    "begin if new.image_no = 'wa_boom' then raise exception 'boom'; end if; "
                    "return new; end $$")
        cur.execute("create trigger wa_boom before update on rat.data_fix_value "
                    "for each row execute function public.wa_boom()")
    pg.commit()
    try:
        res = _one(pg, admin, "select rat.approve_fix_review(%s)", ([p1, p2],))
    finally:
        with pg.cursor() as cur:
            cur.execute("drop trigger wa_boom on rat.data_fix_value")
            cur.execute("drop function public.wa_boom()")
        pg.commit()
    assert res == {"approved": 1, "stale": [], "skipped": [{"id": p1, "reason": "boom"}]}
    assert _pending(pg, "pending_fix_review", p1)[0] == "pending"
    assert _live(pg, "wa_boom")[:2] == (["1"], "extracted")
    assert _live(pg, "wa_fine")[:3] == (["3"], "corrected", t)


def test_withdraw_legacy_match_ignores_fill_of_another_answer(pg):
    """The legacy match only takes fills with no model_answer_id: a fill that
    matches by image_no / changed_by / changed_at but belongs to another
    answer is left alone."""
    task, img = seed_task(pg, "wa_leg", links=LINKS), "wa_leg"
    _linked_photo(pg, img)
    u, admin = make_user(pg), make_admin(pg)
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode, answered_by) "
                    "values (%s, %s, 'station', 'sample', %s) returning id", (task, img, u))
        legacy = cur.fetchone()[0]
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode, answered_by) "
                    "values (%s, %s, 'station', 'sample', %s) returning id", (task, img, u))
        other = cur.fetchone()[0]
        for k, val in (("review_context", "on"), ("review_source", "model_check"),
                       ("acting_for", u), ("model_answer_id", str(other))):
            cur.execute("select set_config(%s, %s, true)", (f"rat.{k}", val))
        cur.execute("""update rat.data_fix_value set "values" = '{}', status = 'no_loco' """
                    "where image_no = %s", (img,))
        cur.execute("select count(*) from rat.data_fix_history where image_no = %s "
                    "and model_answer_id = %s", (img, other))
        assert cur.fetchone()[0] == 2
    pg.commit()
    res = _one(pg, admin, WITHDRAW, ([legacy], None))
    assert res == {"withdrawn": 1, "skipped": [], "fills_left": []}
    for fix_id in LINKS["fix_ids"]:
        assert _live(pg, img, fix_id)[:2] == ([], "no_loco")
        assert _last_hist(pg, img, fix_id)[1] == "model_check"


def _hist_at(pg, hid):
    with pg.cursor() as cur:
        cur.execute("select changed_at from rat.data_fix_history where id = %s", (hid,))
        return cur.fetchone()[0]


def test_undo_restored_reviewer_skips_undo_and_undone_rows(pg):
    # alice R1, bob R2, admin undoes R2, carol R3, undo R3: alice's value is
    # back, so alice (not the admin who undid R2, nor bob) is the reviewer.
    seed_fix_value(pg, "wa_rev", values=["1"])
    alice, bob, carol = (make_volunteer(pg, in_training=False) for _ in range(3))
    admin = make_admin(pg)
    pg.commit()
    r1 = _review(pg, alice, "wa_rev", ["2"])
    r2 = _review(pg, bob, "wa_rev", ["3"])
    assert _one(pg, admin, UNDO, ([r2],))["undone"] == 1
    assert _flags(pg, "wa_rev")[2] == alice
    r3 = _review(pg, carol, "wa_rev", ["4"])
    res = _one(pg, admin, UNDO, ([r3],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "wa_rev")[:2] == (["2"], "corrected")
    _, _, reviewed_by, reviewed_at = _flags(pg, "wa_rev")
    assert (reviewed_by, reviewed_at) == (alice, _hist_at(pg, r1))


def test_undo_restored_reviewer_after_reapply(pg):
    # alice R1, bob R2, undo R2 (U1), undo U1 (re-applies R2), carol R3, undo
    # R3: bob's value is back, so bob is the reviewer.
    seed_fix_value(pg, "wa_rev2", values=["1"])
    alice, bob, carol = (make_volunteer(pg, in_training=False) for _ in range(3))
    admin = make_admin(pg)
    pg.commit()
    _review(pg, alice, "wa_rev2", ["2"])
    r2 = _review(pg, bob, "wa_rev2", ["3"])
    _one(pg, admin, UNDO, ([r2],))
    u1 = _last_hist(pg, "wa_rev2")[0]
    _one(pg, admin, UNDO, ([u1],))
    r3 = _review(pg, carol, "wa_rev2", ["4"])
    assert _one(pg, admin, UNDO, ([r3],))["undone"] == 1
    assert _live(pg, "wa_rev2")[:2] == (["3"], "corrected")
    _, _, reviewed_by, reviewed_at = _flags(pg, "wa_rev2")
    assert (reviewed_by, reviewed_at) == (bob, _hist_at(pg, r2))


def test_undo_restored_reviewer_through_undo_chain(pg):
    # alice R1, bob R2, U1 undoes R2 (alice back), U2 undoes U1 (bob back),
    # then undo U2: alice's value is back, so alice is the reviewer. U2 is
    # given, so it must not count as undoing U1 anywhere down the chain.
    seed_fix_value(pg, "wb_tri", values=["1"])
    alice, bob = make_volunteer(pg, in_training=False), make_volunteer(pg, in_training=False)
    admin = make_admin(pg)
    pg.commit()
    r1 = _review(pg, alice, "wb_tri", ["2"])
    r2 = _review(pg, bob, "wb_tri", ["3"])
    _one(pg, admin, UNDO, ([r2],))
    u1 = _last_hist(pg, "wb_tri")[0]
    _one(pg, admin, UNDO, ([u1],))
    u2 = _last_hist(pg, "wb_tri")[0]
    res = _one(pg, admin, UNDO, ([u2],))
    assert res == {"undone": 1, "changed_since": [], "skipped": []}
    assert _live(pg, "wb_tri")[:2] == (["2"], "corrected")
    _, _, reviewed_by, reviewed_at = _flags(pg, "wb_tri")
    assert (reviewed_by, reviewed_at) == (alice, _hist_at(pg, r1))


def test_approve_seen_value_is_for_one_item(pg):
    t, v, admin, pid = _stale_held(pg, "wb_one")
    ts2 = seed_fix_value(pg, "wb_one2", values=["1"])
    pg.commit()
    pid2 = _one(pg, t, SUBMIT_FIX, ("wb_one2", ["2"], "corrected", ts2))
    seen = _held_row(pg, admin, t, pid)["current_updated_at"]
    _, err = _as(pg, admin, APPROVE_SEEN, ([pid, pid2], seen))
    assert err is not None and err.pgcode == "22023"
    assert "a seen value is for one item at a time" in str(err)
    assert _pending(pg, "pending_fix_review", pid)[0] == "pending"
    assert _pending(pg, "pending_fix_review", pid2)[0] == "pending"
    # Several ids without a seen value still work.
    res = _one(pg, admin, "select rat.approve_fix_review(%s, true)", ([pid, pid2],))
    assert res == {"approved": 2, "stale": [], "skipped": []}
