"""Tests for scripts/load_model_run.py. Pure tests need no DB; the rest use the
shared Docker `pg` fixture (each test its own task id and image_no prefix)."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import pytest

import load_model_run as lmr
from tests.conftest import REPO_ROOT, seed_catalog

REAL_TASK = REPO_ROOT / "scripts" / "model_tasks" / "rat_subject_type.json"


def make_task(task_id: str, ids=("tram", "station")) -> dict:
    keys = "1234567890"
    return {
        "task": task_id, "question": "What?", "client": "rat", "note": "n",
        "sure_threshold": 0.8, "sort_order": 3, "links": {"no_answer_flag": "loco"},
        "answers": [{"key": keys[i % 10], "id": a, "label": a.title(), "loco": i == 0,
                     "prompts": ["p"]} for i, a in enumerate(ids)],
    }


def write_task(tmp_path: Path, task: dict) -> Path:
    f = tmp_path / f"{task['task']}.json"
    f.write_text(json.dumps(task))
    return f


def write_scores(tmp_path: Path, rows, ids=("tram", "station")) -> Path:
    f = tmp_path / "scores.csv"
    lines = ["image_no,answer,p," + ",".join("p_" + i for i in ids)]
    for image_no, answer, p, probs in rows:
        lines.append(f"{image_no},{answer},{p}," + ",".join(str(x) for x in probs))
    f.write_text("\n".join(lines) + "\n")
    return f


def make_args(task_file, scores, version="v1", apply=True) -> argparse.Namespace:
    return argparse.Namespace(task=str(task_file), scores=str(scores), model="m",
                              version=version, notes=None, apply=apply)


# ---------------------------------------------------------------- pure

def test_task_row_drops_prompts():
    row = lmr.task_row(make_task("t"))
    assert set(row) == {"id", "question", "answers", "sure_threshold", "links",
                        "sort_order", "client", "note"}
    assert row["id"] == "t"
    assert all("prompts" not in a for a in row["answers"])
    assert row["answers"][0] == {"key": "1", "id": "tram", "label": "Tram", "loco": True}


def test_task_file_errors():
    assert lmr.task_file_errors(make_task("t")) == []
    dup = make_task("t")
    dup["answers"][1]["key"] = "1"
    assert lmr.task_file_errors(dup)
    bad = make_task("t")
    bad["answers"][1]["key"] = "10"
    assert lmr.task_file_errors(bad)
    dupid = make_task("t", ids=("a", "a"))
    assert lmr.task_file_errors(dupid)
    many = make_task("t", ids=tuple(f"a{i}" for i in range(11)))
    many["answers"] = [{**a, "key": "1"} for a in many["answers"]]
    assert lmr.task_file_errors(many)
    assert lmr.task_file_errors({**make_task("t"), "answers": []})


def test_answer_list_errors():
    stored = [{"key": "1", "id": "a"}, {"key": "2", "id": "b"}]
    assert len(lmr.answer_list_errors(stored, [{"key": "1", "id": "a"}])) == 1
    assert len(lmr.answer_list_errors(stored, [{"key": "1", "id": "a"}, {"key": "3", "id": "b"}])) == 1
    new = [{"key": "1", "id": "a", "label": "Changed"}, {"key": "2", "id": "b"},
           {"key": "3", "id": "c"}]
    assert lmr.answer_list_errors(stored, new) == []


def test_read_scores_rejects_wrong_header(tmp_path):
    f = write_scores(tmp_path, [], ids=("tram", "other"))
    with pytest.raises(ValueError) as e:
        list(lmr.read_scores(f, ["tram", "station"]))
    assert "p_station" in str(e.value) and "p_other" in str(e.value)


def test_read_scores_parses(tmp_path):
    f = write_scores(tmp_path, [("a1", "tram", 0.7, [0.7, 0.3])])
    assert list(lmr.read_scores(f, ["tram", "station"])) == [("a1", "tram", 0.7, [0.7, 0.3])]


def test_real_task_file_is_valid():
    assert lmr.task_file_errors(json.loads(REAL_TASK.read_text())) == []


# ---------------------------------------------------------------- pg

def counts(conn, task_id):
    with conn.cursor() as cur:
        cur.execute("select (select count(*) from rat.model_task where id=%s),"
                    " (select count(*) from rat.model_run where task_id=%s),"
                    " (select count(*) from rat.model_prediction p join rat.model_run r"
                    "  on r.id=p.run_id where r.task_id=%s)", (task_id,) * 3)
        return cur.fetchone()


def test_apply_loads_run(pg, tmp_path, capsys):
    tid = "lmr_apply"
    seed_catalog(pg, "lmra_1", "d")
    seed_catalog(pg, "lmra_2", "d")
    pg.commit()
    task = make_task(tid)
    scores = write_scores(tmp_path, [("lmra_1", "tram", 0.9, [0.9, 0.1]),
                                     ("lmra_2", "station", 0.6, [0.4, 0.6]),
                                     ("lmra_missing", "tram", 0.5, [0.5, 0.5])])
    assert lmr.run(pg, make_args(write_task(tmp_path, task), scores)) == 0
    assert "1 skipped" in capsys.readouterr().out
    with pg.cursor() as cur:
        cur.execute("select n_predictions, answers, id from rat.model_run where task_id=%s", (tid,))
        n, answers, run_id = cur.fetchone()
        assert n == 2
        assert answers == lmr.task_row(task)["answers"]
        cur.execute("select image_no, answer, p, probs from rat.model_prediction"
                    " where run_id=%s order by image_no", (run_id,))
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["lmra_1", "lmra_2"]
    assert rows[1][1] == "station" and abs(rows[1][3][0] - 0.4) < 1e-6
    assert counts(pg, tid) == (1, 1, 2)


def test_dry_run_writes_nothing(pg, tmp_path, capsys):
    tid = "lmr_dry"
    seed_catalog(pg, "lmrd_1", "d")
    pg.commit()
    scores = write_scores(tmp_path, [("lmrd_1", "tram", 0.9, [0.9, 0.1]),
                                     ("lmrd_x", "tram", 0.9, [0.9, 0.1])])
    args = make_args(write_task(tmp_path, make_task(tid)), scores, apply=False)
    assert lmr.run(pg, args) == 0
    out = capsys.readouterr().out
    assert "would load" in out and "1 skipped" in out
    assert counts(pg, tid) == (0, 0, 0)


def test_apply_refuses_duplicate_version(pg, tmp_path, capsys):
    tid = "lmr_dup"
    seed_catalog(pg, "lmrp_1", "d")
    pg.commit()
    scores = write_scores(tmp_path, [("lmrp_1", "tram", 0.9, [0.9, 0.1])])
    args = make_args(write_task(tmp_path, make_task(tid)), scores, version="v7")
    assert lmr.run(pg, args) == 0
    assert lmr.run(pg, args) != 0
    assert "v7" in capsys.readouterr().err
    assert counts(pg, tid) == (1, 1, 1)


def test_apply_refuses_incompatible_answers(pg, tmp_path, capsys):
    tid = "lmr_incompat"
    seed_catalog(pg, "lmri_1", "d")
    pg.commit()
    scores = write_scores(tmp_path, [("lmri_1", "tram", 0.9, [0.9, 0.1])])
    assert lmr.run(pg, make_args(write_task(tmp_path, make_task(tid)), scores)) == 0
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode)"
                    " values (%s, 'lmri_1', 'tram', 'sample')", (tid,))
    pg.commit()
    reduced = make_task(tid, ids=("tram",))
    scores2 = write_scores(tmp_path, [("lmri_1", "tram", 0.9, [0.9])], ids=("tram",))
    assert lmr.run(pg, make_args(write_task(tmp_path, reduced), scores2, version="v2")) != 0
    assert "station" in capsys.readouterr().err
    assert counts(pg, tid) == (1, 1, 1)


def test_has_answers_ignores_withdrawn(pg, tmp_path):
    """A task whose only answers are withdrawn counts as having none, so a
    changed answer list is accepted."""
    tid = "lmr_withdrawn"
    seed_catalog(pg, "lmrw_1", "d")
    pg.commit()
    scores = write_scores(tmp_path, [("lmrw_1", "tram", 0.9, [0.9, 0.1])])
    assert lmr.run(pg, make_args(write_task(tmp_path, make_task(tid)), scores)) == 0
    with pg.cursor() as cur:
        cur.execute("insert into rat.model_answer (task_id, image_no, answer, mode)"
                    " values (%s, 'lmrw_1', 'tram', 'sample') returning id", (tid,))
        aid = cur.fetchone()[0]
        cur.execute("insert into rat.model_answer_withdrawal (answer_id, withdrawn_by)"
                    " values (%s, gen_random_uuid())", (aid,))
    pg.commit()
    reduced = make_task(tid, ids=("tram",))
    scores2 = write_scores(tmp_path, [("lmrw_1", "tram", 0.9, [0.9])], ids=("tram",))
    assert lmr.run(pg, make_args(write_task(tmp_path, reduced), scores2, version="v2")) == 0


def test_apply_never_touches_data_fix(pg, tmp_path):
    seed_catalog(pg, "lmrf_1", "d")
    pg.commit()

    def snap():
        with pg.cursor() as cur:
            cur.execute("select (select count(*) from rat.data_fix_history),"
                        " (select count(*) from rat.data_fix_value)")
            return cur.fetchone()
    before = snap()
    scores = write_scores(tmp_path, [("lmrf_1", "tram", 0.9, [0.9, 0.1])])
    assert lmr.run(pg, make_args(write_task(tmp_path, make_task("lmr_fix")), scores)) == 0
    assert snap() == before
