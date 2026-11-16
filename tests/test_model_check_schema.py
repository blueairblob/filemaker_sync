"""Schema tests for the Model check tables (add_model_check_tables.sql).

Run against the throwaway Docker postgres only (Golden Rule #4). The fixture DB
is shared, so each test uses its own task id (R2).
"""
import psycopg2
import pytest

from tests.conftest import (
    BOOTSTRAP_SQL, DATA_FIX_SQL, MODEL_CHECK_SQL, apply_sql_file,
    as_anon, as_user, make_admin, make_user, seed_run, seed_task,
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
        assert "append-only" in str(ei.value)


def test_sample_answer_must_be_blind(pg):
    seed_task(pg, "t_blind")
    with pg.cursor() as cur:
        with pytest.raises(psycopg2.errors.CheckViolation):
            _insert_answer(cur, "t_blind", "mb_img_1", mode="sample", shown_answer="loco")


def test_anon_and_non_admin_closed(pg):
    seed_task(pg, "t_closed")
    seed_run(pg, "t_closed", predictions={"mc_img_1": ("loco", 0.95)})
    uid = make_user(pg)
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
            assert cur.fetchone()[0] == 0
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


def test_bootstrap_contains_model_check():
    text = MODEL_CHECK_SQL.read_text(encoding="utf-8")
    body = text[text.index("BEGIN;"):]
    assert body in BOOTSTRAP_SQL.read_text(encoding="utf-8")
