"""Schema tests for the volunteer role (add_volunteer_tables.sql).

Run against the throwaway Docker postgres only (Golden Rule #4). The fixture DB
is shared, so each test makes its own users and never asserts table-wide counts.
"""
import uuid

import psycopg2
import pytest

from tests.conftest import (
    BOOTSTRAP_SQL, VOLUNTEER_SQL, as_user, make_admin, make_user, make_volunteer,
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
