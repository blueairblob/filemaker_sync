"""Schema tests for the Model check tables (add_model_check_tables.sql).

Run against the throwaway Docker postgres only (Golden Rule #4). The fixture DB
is shared, so each test uses its own task id (R2).
"""
import hashlib
import threading
import time

import psycopg2
import pytest

from loco_extract import description_hash
from tests.conftest import (
    BOOTSTRAP_SQL, DATA_FIX_SQL, MODEL_CHECK_SQL, VOLUNTEER_SQL, _connect, apply_sql_file,
    as_anon, as_user, make_admin, make_user, seed_catalog, seed_fix_value,
    seed_run, seed_task,
)


def _insert_answer(cur, task_id, image_no, mode="sure", shown_answer="loco",
                   shown_run_id=None):
    cur.execute(
        """insert into rat.model_answer
             (task_id, image_no, answer, shown_run_id, shown_answer, mode)
           values (%s, %s, 'loco', %s, %s, %s) returning id""",
        (task_id, image_no, shown_run_id, shown_answer, mode),
    )
    return cur.fetchone()[0]


def test_answers_append_only(pg):
    seed_task(pg, "t_append")
    with pg.cursor() as cur:
        aid = _insert_answer(cur, "t_append", "ma_img_1", mode="sample", shown_answer=None)
        for stmt in ("update rat.model_answer set answer = 'x' where id = %s",
                     "delete from rat.model_answer where id = %s"):
            cur.execute("savepoint s")
            with pytest.raises(psycopg2.Error) as ei:
                cur.execute(stmt, (aid,))
            assert "append-only" in str(ei.value)
            cur.execute("rollback to savepoint s")
        with pytest.raises(psycopg2.Error) as ei:
            cur.execute("truncate rat.model_answer")
        # Once model_answer_withdrawal references it, Postgres refuses the truncate
        # on the foreign key before the trigger runs; either way it is blocked.
        assert ("append-only" in str(ei.value)
                or "referenced in a foreign key" in str(ei.value))


def test_sample_answer_must_be_blind(pg):
    seed_task(pg, "t_blind")
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.errors.CheckViolation):
            _insert_answer(cur, "t_blind", "mb_img_1", mode="sample", shown_answer="loco")


def test_anon_and_non_admin_closed(pg):
    seed_task(pg, "t_closed")
    seed_run(pg, "t_closed", predictions={"mc_img_1": ("loco", 0.95)})
    uid = make_user(pg)
    pg.commit()  # the anon checks below roll back; the seeded rows must survive
    with pg.cursor() as cur:
        for t in ("model_task", "model_run"):
            cur.execute(f"select count(*) from rat.{t}")
            assert cur.fetchone()[0] >= 1  # owner sees the seeded rows
    pg.rollback()
    tables = ("model_task", "model_run", "model_prediction", "model_answer")
    for t in tables:
        as_anon(pg)
        with pg.cursor() as cur:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(f"select * from rat.{t}")
        pg.rollback()
    as_user(pg, uid)
    with pg.cursor() as cur:
        for t in ("model_task", "model_run"):
            cur.execute(f"select count(*) from rat.{t}")
            assert cur.fetchone()[0] == 0  # rows exist (checked above); RLS hides them
        for t in ("model_prediction", "model_answer"):
            cur.execute("savepoint s")
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(f"select * from rat.{t}")
            cur.execute("rollback to savepoint s")


def test_admin_reads_tasks_and_runs(pg):
    seed_task(pg, "t_admin")
    run_id = seed_run(pg, "t_admin", predictions={"md_img_1": ("loco", 0.95)})
    admin = make_admin(pg)
    as_user(pg, admin)
    with pg.cursor() as cur:
        cur.execute("select id from rat.model_task where id = 't_admin'")
        assert cur.fetchone() is not None
        cur.execute("select id from rat.model_run where id = %s", (run_id,))
        assert cur.fetchone() is not None


def test_model_check_ddl_idempotent(pg):
    pg.commit()
    apply_sql_file(MODEL_CHECK_SQL)
    apply_sql_file(MODEL_CHECK_SQL)
    apply_sql_file(DATA_FIX_SQL)
    apply_sql_file(VOLUNTEER_SQL)  # re-apply last, as live does


def test_bootstrap_contains_model_check():
    text = MODEL_CHECK_SQL.read_text(encoding="utf-8")
    body = text[text.index("BEGIN;"):]
    assert body in BOOTSTRAP_SQL.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# RPCs: queue, save (with the Data Fixes link), progress, confusion, conflicts
# --------------------------------------------------------------------------

LINKS = {"no_answer_flag": "loco", "fix_ids": ["loco_number", "loco_name"]}
FIX_IDS = ("loco_number", "loco_name")


def _save(cur, task_id, image_no, answer, mode="sure", run_id=None, shown=None):
    cur.execute("select rat.save_model_answer(%s, %s, %s, %s, %s, %s)",
                (task_id, image_no, answer, run_id, shown, mode))
    return cur.fetchone()[0]


def _fix(conn, image_no, fix_id):
    with conn.cursor() as cur:
        cur.execute(
            """select "values", status, confidence, reviewed_by::text, needs_recheck,
                      description_hash
                 from rat.data_fix_value where image_no = %s and fix_id = %s""",
            (image_no, fix_id),
        )
        return cur.fetchone()


def _seed_photo(conn, image_no, description="A scene", number=None, name=None):
    """Catalog row plus both Data Fixes rows. number/name: None = extracted
    with confidence 'none'; else a dict of seed_fix_value kwargs."""
    seed_catalog(conn, image_no, description)
    for fix_id, kw in (("loco_number", number), ("loco_name", name)):
        seed_fix_value(conn, image_no, fix_id, **(kw or {"confidence": "none"}))


def _queue(cur, task_id, mode, run_id=None, **kw):
    args = ", ".join(f"p_{k} => %({k})s" for k in kw)
    sql = ("select image_no, sort_key, model_answer, model_p, checked_answer, "
           "checked_at, fix_state from rat.model_check_queue(%(task)s, %(mode)s, "
           "%(run)s" + (", " + args if args else "") + ")")
    cur.execute(sql, {"task": task_id, "mode": mode, "run": run_id, **kw})
    return cur.fetchall()


def _sample_all(cur, task_id, prefix):
    """Page through the whole sample queue (the shared DB holds other tests'
    photos); return every row, and this test's own rows."""
    rows, after = [], None
    while True:
        page = _queue(cur, task_id, "sample", after=after, limit=500)
        if not page:
            break
        rows += page
        after = page[-1][1]
    return rows, [r for r in rows if r[0].startswith(prefix)]


def _pgcode(conn, uid, fn):
    """Run fn(cur) as uid; return the error's pgcode (None if no error)."""
    as_user(conn, uid)
    try:
        with conn.cursor() as cur:
            fn(cur)
    except psycopg2.Error as exc:
        conn.rollback()
        return exc.pgcode
    conn.rollback()
    return None


def _entry(fix_id, status, confidence, values, suggested=None):
    return {"fix_id": fix_id, "status": status, "confidence": confidence,
            "values": list(values),
            "suggested_values": list(values if suggested is None else suggested)}


def test_save_fills_when_description_named_nothing(pg):
    task, img = "t3_fill", "m3f_1"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("station", 0.95)})
    _seed_photo(pg, img, "A station scene")
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        res = _save(cur, task, img, "station", "sure", run, "station")
        # Both settings are closed again inside the same transaction.
        cur.execute("select current_setting('rat.review_context', true), "
                    "current_setting('rat.review_source', true)")
        assert cur.fetchone() == ("", "")
    pg.commit()

    assert res["filled"] == ["loco_number", "loco_name"]
    assert res["conflicts"] == []
    for fix_id in FIX_IDS:
        values, status, _, reviewed_by, needs_recheck, dhash = _fix(pg, img, fix_id)
        assert (values, status, reviewed_by, needs_recheck) == ([], "no_loco", admin, False)
        assert dhash == description_hash("A station scene")
    with pg.cursor() as cur:
        cur.execute("select fix_id, source, changed_by::text, new_status "
                    "from rat.data_fix_history where image_no = %s and old_status is not null "
                    "order by fix_id", (img,))
        assert cur.fetchall() == [("loco_name", "model_check", admin, "no_loco"),
                                  ("loco_number", "model_check", admin, "no_loco")]
        cur.execute("select id, answer, answered_by::text, mode, shown_run_id, shown_answer "
                    "from rat.model_answer where task_id = %s and image_no = %s", (task, img))
        assert cur.fetchall() == [(res["answer_id"], "station", admin, "sure", run, "station")]


def test_save_conflict_when_description_named_a_number(pg):
    task, img = "t3_named", "m3n_1"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("station", 0.95)})
    _seed_photo(pg, img, "Station with 6853",
                number={"values": ["6853"], "confidence": "high"})
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        res = _save(cur, task, img, "station", "sure", run, "station")
    pg.commit()

    assert res["filled"] == []
    assert res["conflicts"] == [_entry("loco_number", "extracted", "high", ["6853"])]
    assert _fix(pg, img, "loco_number")[:4] == (["6853"], "extracted", "high", None)
    assert _fix(pg, img, "loco_name")[:4] == ([], "extracted", "none", None)


def test_save_never_changes_reviewed_value(pg):
    task, img, img2 = "t3_reviewed", "m3r_1", "m3r_2"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("station", 0.95), img2: ("station", 0.95)})
    _seed_photo(pg, img, number={"values": ["6853"], "status": "verified"})
    # Reviewed with no value (none_visible): still never changed, and the
    # all-or-nothing rule keeps loco_name unfilled too.
    _seed_photo(pg, img2, number={"values": [], "status": "none_visible",
                                  "confidence": "none"})
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        res = _save(cur, task, img, "station", "sure", run, "station")
        res2 = _save(cur, task, img2, "station", "sure", run, "station")
    pg.commit()

    assert res["filled"] == []
    assert res["conflicts"] == [_entry("loco_number", "verified", "high", ["6853"])]
    assert _fix(pg, img, "loco_number")[:4] == (["6853"], "verified", "high", None)
    assert _fix(pg, img, "loco_name")[:2] == ([], "extracted")
    assert res2 == {"answer_id": res2["answer_id"], "filled": [], "conflicts": []}
    assert _fix(pg, img2, "loco_number")[:2] == ([], "none_visible")
    assert _fix(pg, img2, "loco_name")[:2] == ([], "extracted")


def test_save_does_not_fill_row_reviewed_meanwhile(pg):
    task, img, img2 = "t3_meanwhile", "m3m_1", "m3m_2"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("station", 0.95), img2: ("station", 0.95)})
    _seed_photo(pg, img)
    _seed_photo(pg, img2)
    admin = make_admin(pg)
    pg.commit()

    # Queue load shows the field still extracted ...
    as_user(pg, admin)
    with pg.cursor() as cur:
        rows = {r[0]: r for r in _queue(cur, task, "sure", run)}
        assert rows[img][6]["loco_number"] == {
            "status": "extracted", "confidence": "none", "values": [], "suggested_values": []}
        # ... then someone reviews it before the save.
        cur.execute("select updated_at from rat.data_fix_value "
                    "where image_no = %s and fix_id = 'loco_number'", (img,))
        cur.execute("select rat.review_fix_value(%s, 'loco_number', %s, 'corrected', %s)",
                    (img, ["1"], cur.fetchone()[0]))
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        res = _save(cur, task, img, "station", "sure", run, "station")
    pg.commit()
    assert res["filled"] == []
    assert res["conflicts"] == [_entry("loco_number", "corrected", "none", ["1"], [])]
    assert _fix(pg, img, "loco_number")[:2] == (["1"], "corrected")
    assert _fix(pg, img, "loco_name")[:2] == ([], "extracted")

    # Truly concurrent: the review holds the row lock while the save starts;
    # the save waits on it and re-checks what was committed.
    reviewer, saver, watcher = _connect(), _connect(), _connect(autocommit=True)
    result = {}
    try:
        as_user(reviewer, admin)
        with reviewer.cursor() as cur:
            cur.execute("select updated_at from rat.data_fix_value "
                        "where image_no = %s and fix_id = 'loco_number'", (img2,))
            cur.execute("select rat.review_fix_value(%s, 'loco_number', %s, 'corrected', %s)",
                        (img2, ["2"], cur.fetchone()[0]))

        def save():
            try:
                as_user(saver, admin)
                with saver.cursor() as cur:
                    result["res"] = _save(cur, task, img2, "station", "sure", run, "station")
                saver.commit()
            except Exception as exc:  # surfaced by the asserts below
                result["exc"] = exc

        thread = threading.Thread(target=save)
        thread.start()
        deadline = time.monotonic() + 15
        with watcher.cursor() as cur:
            while True:
                cur.execute("select wait_event_type from pg_stat_activity where pid = %s",
                            (saver.get_backend_pid(),))
                if cur.fetchone()[0] == "Lock":
                    break
                assert time.monotonic() < deadline, "save never waited on the row lock"
                time.sleep(0.05)
        reviewer.commit()
        thread.join(timeout=30)
        assert not thread.is_alive()
    finally:
        for conn in (reviewer, saver, watcher):
            conn.close()
    assert "exc" not in result, result.get("exc")
    assert result["res"]["filled"] == []
    assert _fix(pg, img2, "loco_number")[:2] == (["2"], "corrected")
    assert _fix(pg, img2, "loco_name")[:2] == ([], "extracted")


def test_loco_answer_flags_saved_no_loco(pg):
    task, img = "t3_flag", "m3l_1"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("loco", 0.95)})
    _seed_photo(pg, img, number={"values": [], "status": "no_loco", "confidence": "none"})
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        res = _save(cur, task, img, "loco", "sure", run, "loco")
    pg.commit()
    assert res["filled"] == []
    assert res["conflicts"] == [_entry("loco_number", "no_loco", "none", [])]
    assert _fix(pg, img, "loco_number")[:4] == ([], "no_loco", "none", None)
    assert _fix(pg, img, "loco_name")[:4] == ([], "extracted", "none", None)


def test_save_validation(pg):
    task, img = "t3_valid", "m3v_1"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={img: ("loco", 0.95)})
    seed_task(pg, "t3_valid_other")
    other_run = seed_run(pg, "t3_valid_other")
    _seed_photo(pg, img)
    admin, user = make_admin(pg), make_user(pg)
    pg.commit()

    def call(*args):
        return lambda cur: _save(cur, *args)

    assert _pgcode(pg, user, call(task, img, "loco", "sure", run, "loco")) == "42501"
    assert _pgcode(pg, admin, call("t3_nope", img, "loco", "sure", run, "loco")) == "P0002"
    assert _pgcode(pg, admin, call(task, img, "loco", "fast", run, "loco")) == "22023"
    assert _pgcode(pg, admin, call(task, img, "tram", "sure", run, "loco")) == "22023"
    assert _pgcode(pg, admin, call(task, img, "loco", "sample", None, "loco")) == "22023"
    assert _pgcode(pg, admin, call(task, img, "loco", "sample", run, None)) == "22023"
    assert _pgcode(pg, admin, call(task, img, "loco", "sure", None, "loco")) == "22023"
    assert _pgcode(pg, admin, call(task, img, "loco", "unsure", other_run, "loco")) == "22023"
    assert _pgcode(pg, admin, call(task, "m3v_missing", "loco", "sure", run, "loco")) == "P0002"
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.model_answer where task_id = %s", (task,))
        assert cur.fetchone()[0] == 0
    assert _fix(pg, img, "loco_number")[:2] == ([], "extracted")


def test_queue_modes(pg):
    task, a, b, c = "t3_queue", "m3q_a", "m3q_b", "m3q_c"
    seed_task(pg, task, links=LINKS, threshold=0.9)
    run = seed_run(pg, task, predictions={a: ("loco", 0.95), b: ("station", 0.5),
                                          c: ("loco", 0.92)})
    seed_task(pg, "t3_queue_other")
    other_run = seed_run(pg, "t3_queue_other")
    for img in (a, b, c):
        seed_catalog(pg, img, f"photo {img}")
    seed_fix_value(pg, c, "loco_number", values=["42"], confidence="high")
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        sure = _queue(cur, task, "sure", run)
        assert [r[0] for r in sure] == [a, c]
        row_a = sure[0]
        assert row_a[1] == a                       # sort_key = image_no
        assert (row_a[2], round(row_a[3], 2)) == ("loco", 0.95)
        assert (row_a[4], row_a[5]) == (None, None)
        assert row_a[6] == {}                      # linked task, no Data Fixes rows
        assert sure[1][6] == {"loco_number": {"status": "extracted", "confidence": "high",
                                              "values": ["42"], "suggested_values": ["42"]}}
        assert [r[0] for r in _queue(cur, task, "unsure", run)] == [b]
        assert [r[0] for r in _queue(cur, task, "sure", run, answer="loco")] == [a, c]
        assert _queue(cur, task, "sure", run, answer="station") == []
        assert [r[0] for r in _queue(cur, task, "sure", run, after=a)] == [c]
        assert [r[0] for r in _queue(cur, task, "sure", run, limit=1)] == [a]
        assert _queue(cur, task, "sure", run, collection="t3 no such collection") == []
        _save(cur, task, a, "loco", "sure", run, "loco")
        assert [r[0] for r in _queue(cur, task, "sure", run)] == [c]
    pg.commit()

    def q(*args, **kw):
        return lambda cur: _queue(cur, *args, **kw)

    assert _pgcode(pg, admin, q(task, "sure", None)) == "22023"
    assert _pgcode(pg, admin, q(task, "unsure", other_run)) == "22023"
    assert _pgcode(pg, admin, q(task, "fast", run)) == "22023"
    assert _pgcode(pg, admin, q("t3_nope", "sure", run)) == "P0002"
    assert _pgcode(pg, make_user(pg), q(task, "sure", run)) == "42501"


def test_queue_sample_is_blind(pg):
    task, prefix = "t3_sample", "m3s_"
    imgs = [f"{prefix}{i}" for i in range(1, 6)]
    seed_task(pg, task)                            # no Data Fixes link
    run = seed_run(pg, task, predictions={i: ("loco", 0.95) for i in imgs})
    for img in imgs:
        seed_catalog(pg, img, f"photo {img}")
    admin = make_admin(pg)
    pg.commit()

    def md5(s):
        return hashlib.md5(s.encode()).hexdigest()

    as_user(pg, admin)
    with pg.cursor() as cur:
        rows, own = _sample_all(cur, task, prefix)
        assert [r[0] for r in own] == sorted(imgs, key=md5)
        assert all(r[1] == md5(r[0]) for r in own)
        # Blind everywhere: no prediction, no pre-selected answer.
        assert all(r[2] is None and r[3] is None for r in rows)
        assert all(r[4] is None and r[5] is None for r in rows)
        assert all(r[6] is None for r in own)      # task has no link
        _save(cur, task, imgs[0], "loco", "sample")
        _save(cur, task, imgs[1], "loco", "sure", run, "loco")
        _, own = _sample_all(cur, task, prefix)
        assert [r[0] for r in own] == sorted(imgs[1:], key=md5)
    pg.commit()

    def q(**kw):
        return lambda cur: _queue(cur, task, "sample", **kw)

    assert _pgcode(pg, admin, q(answer="loco")) == "22023"
    assert _pgcode(pg, admin, q(collection="anything")) == "22023"


def _progress(conn, run_id):
    with conn.cursor() as cur:
        cur.execute("select task_id, model, version, n_predictions, n_checked, n_agree, "
                    "n_blind, n_blind_correct from rat.model_check_progress() "
                    "where run_id = %s", (run_id,))
        return cur.fetchall()


def test_progress_counts_blind_only_for_accuracy(pg):
    task, a, b, c = "t3_prog", "m3p_a", "m3p_b", "m3p_c"
    seed_task(pg, task)
    run1 = seed_run(pg, task, "v1", {a: ("loco", 0.95), b: ("station", 0.5),
                                     c: ("loco", 0.95)})
    run2 = seed_run(pg, task, "v2", {a: ("loco", 0.95), b: ("loco", 0.6),
                                     c: ("loco", 0.95)})
    for img in (a, b, c):
        seed_catalog(pg, img, f"photo {img}")
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        _save(cur, task, a, "loco", "sample")
        # Same transaction, so equal answered_at: the later row (higher id) wins.
        _save(cur, task, b, "station", "sample")
        _save(cur, task, b, "loco", "sample")
        _save(cur, task, c, "loco", "sure", run1, "loco")
    pg.commit()

    as_user(pg, admin)
    assert _progress(pg, run1) == [(task, "test-model", "v1", 3, 3, 2, 2, 1)]
    assert _progress(pg, run2) == [(task, "test-model", "v2", 3, 3, 3, 2, 2)]
    pg.rollback()
    assert _pgcode(pg, make_user(pg),
                   lambda cur: cur.execute("select * from rat.model_check_progress()")) == "42501"


def test_confusion_and_conflicts(pg):
    task = "t3_conf"
    a, b, n, l = "m3c_a", "m3c_b", "m3c_n", "m3c_l"
    seed_task(pg, task, links=LINKS)
    run = seed_run(pg, task, predictions={a: ("loco", 0.95), b: ("station", 0.5),
                                          n: ("station", 0.95)})
    seed_catalog(pg, a, "photo a")
    seed_catalog(pg, b, "photo b")
    _seed_photo(pg, n, "Station with 6853", number={"values": ["6853"], "confidence": "high"})
    _seed_photo(pg, l, number={"values": [], "status": "no_loco", "confidence": "none"})
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        _save(cur, task, a, "loco", "sample")
        _save(cur, task, b, "loco", "sample")
        _save(cur, task, n, "station", "sure", run, "station")
        _save(cur, task, l, "loco", "sample")
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        cur.execute("select * from rat.model_check_confusion(%s)", (run,))
        assert cur.fetchall() == [("loco", "loco", 1), ("station", "loco", 1)]
        cur.execute("select * from rat.model_check_confusion(%s, false)", (run,))
        assert cur.fetchall() == [("loco", "loco", 1), ("station", "loco", 1),
                                  ("station", "station", 1)]
        cur.execute("select * from rat.model_check_conflicts(%s)", (task,))
        assert cur.fetchall() == [
            (l, "loco", "loco_number", "no_loco", "none", [], []),
            (n, "station", "loco_number", "extracted", "high", ["6853"], ["6853"]),
        ]
        cur.execute("select updated_at from rat.data_fix_value "
                    "where image_no = %s and fix_id = 'loco_number'", (n,))
        cur.execute("select rat.review_fix_value(%s, 'loco_number', %s, 'corrected', %s)",
                    (n, ["6853"], cur.fetchone()[0]))
        cur.execute("select image_no from rat.model_check_conflicts(%s)", (task,))
        assert cur.fetchall() == [(l,)]
    pg.rollback()

    for fn in (lambda cur: cur.execute("select * from rat.model_check_confusion(%s)", (run,)),
               lambda cur: cur.execute("select * from rat.model_check_conflicts(%s)", (task,))):
        assert _pgcode(pg, make_user(pg), fn) == "42501"


MODEL_CHECK_RPCS = {
    "rat.model_check_queue(text, text, bigint, text, text, text, integer)":
        "select * from rat.model_check_queue('t', 'sure')",
    "rat.save_model_answer(text, text, text, bigint, text, text)":
        "select rat.save_model_answer('t', 'i', 'a', null, null, 'sample')",
    "rat.model_check_progress()": "select * from rat.model_check_progress()",
    "rat.model_check_confusion(bigint, boolean)": "select * from rat.model_check_confusion(1)",
    "rat.model_check_conflicts(text)": "select * from rat.model_check_conflicts('t')",
    "rat.model_check_photo(text, text, bigint)": "select * from rat.model_check_photo('t', 'i')",
}


def test_rpc_grants(pg):
    with pg.cursor() as cur:
        for fn in MODEL_CHECK_RPCS:
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn))
            assert cur.fetchone() == (False, True, True, ['search_path=""']), fn
        fn = "rat.model_answer_append_only()"
        cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                    "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                    "has_function_privilege('public', %s, 'EXECUTE')", (fn, fn, fn))
        assert cur.fetchone() == (False, False, False)
    for sql in MODEL_CHECK_RPCS.values():
        as_anon(pg)
        with pytest.raises(psycopg2.errors.InsufficientPrivilege) as exc:
            with pg.cursor() as cur:
                cur.execute(sql)
        assert "permission denied for function" in str(exc.value)
        pg.rollback()


# --------------------------------------------------------------------------
# One photo, to answer again (a conflict's "Model check" link)
# --------------------------------------------------------------------------

PHOTO_PATCH_SQL = MODEL_CHECK_SQL.parent / "fix_model_check_photo.sql"


def _photo(cur, task_id, image_no, run_id=None):
    cur.execute(
        """select image_no, model_answer, model_p, checked_answer, checked_at is not null,
                  checked_mode, run_id, description, fix_state
             from rat.model_check_photo(%s, %s, %s)""",
        (task_id, image_no, run_id))
    return cur.fetchall()


def test_photo_returns_latest_answer_and_prediction(pg):
    task, p = "t9_photo", "m9p_"
    seed_task(pg, task, links=LINKS)
    run1 = seed_run(pg, task, "v1", {f"{p}1": ("loco", 0.95), f"{p}2": ("loco", 0.5)})
    for i in (1, 2, 3):
        _seed_photo(pg, f"{p}{i}", f"photo {i}")
    admin = make_admin(pg)
    pg.commit()

    as_user(pg, admin)
    with pg.cursor() as cur:
        _save(cur, task, f"{p}1", "loco", "sure", run1, "loco")
        _save(cur, task, f"{p}1", "station", "sure", run1, "loco")   # latest wins
        _save(cur, task, f"{p}2", "station", "sample")

        (row,) = _photo(cur, task, f"{p}1")
        assert row[:4] == (f"{p}1", "loco", pytest.approx(0.95), "station")
        assert row[4:7] == (True, "sure", run1)
        assert row[7] == "photo 1"
        assert set(row[8]) == set(FIX_IDS)

        # Last answered blind: the model's guess stays hidden.
        (row,) = _photo(cur, task, f"{p}2", run1)
        assert row[1:7] == (None, None, "station", True, "sample", run1)

        # Never answered and no prediction.
        (row,) = _photo(cur, task, f"{p}3")
        assert row[1:7] == (None, None, None, False, None, run1)

        assert _photo(cur, task, "m9p_not_in_catalog") == []
    pg.commit()

    # With no run named, the newest run's prediction is used.
    run2 = seed_run(pg, task, "v2", {f"{p}1": ("other", 0.6)})
    pg.commit()
    as_user(pg, admin)
    with pg.cursor() as cur:
        assert _photo(cur, task, f"{p}1")[0][1:3] == ("other", pytest.approx(0.6))
        assert _photo(cur, task, f"{p}1")[0][6] == run2
        assert _photo(cur, task, f"{p}1", run1)[0][1] == "loco"
    pg.rollback()

    other = seed_task(pg, "t9_other")
    other_run = seed_run(pg, other, "v1", {})
    user = make_user(pg)
    pg.commit()
    assert _pgcode(pg, admin, lambda cur: _photo(cur, task, f"{p}1", other_run)) == "22023"
    assert _pgcode(pg, admin, lambda cur: _photo(cur, "t9_missing", f"{p}1")) == "P0002"
    assert _pgcode(pg, user, lambda cur: _photo(cur, task, f"{p}1")) == "42501"


def test_photo_patch_matches_and_is_idempotent(pg):
    patch = PHOTO_PATCH_SQL.read_text(encoding="utf-8")
    fn = patch[patch.index("CREATE OR REPLACE FUNCTION rat.model_check_photo"):]
    fn = fn[:fn.index("$$;") + 3]
    assert fn in MODEL_CHECK_SQL.read_text(encoding="utf-8")
    pg.commit()
    apply_sql_file(PHOTO_PATCH_SQL)
    apply_sql_file(PHOTO_PATCH_SQL)
    apply_sql_file(VOLUNTEER_SQL)
