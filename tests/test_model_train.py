import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

import load_model_run
import model_train

TASK = {
    "task": "t",
    "question": "q",
    "answers": [
        {"id": "steam_loco", "key": "1", "loco": True},
        {"id": "diesel_loco", "key": "2", "loco": True},
        {"id": "ship", "key": "3", "loco": False},
    ],
}
IDS = [a["id"] for a in TASK["answers"]]


def write_zeroshot(path, nos, zs):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_no", "answer", "p"] + ["p_" + i for i in IDS])
        for no, z in zip(nos, zs):
            w.writerow([no, IDS[int(np.argmax(z))], f"{max(z):.4f}"] + [f"{v:.4f}" for v in z])


def make_inputs(tmp, n=30, extra_zs=("zzz extra",), emb_extra=()):
    rng = np.random.default_rng(0)
    nos = [f"p{i:02d}" for i in range(n)] + list(emb_extra)
    emb = rng.normal(size=(len(nos), 6)).astype(np.float16)
    prefix = tmp / "emb"
    np.save(str(prefix) + ".npy", emb)
    (tmp / "emb.image_nos.txt").write_text("\n".join(nos) + "\n", encoding="utf-8")
    zs = rng.dirichlet(np.ones(3), size=len(nos))
    zs_nos = list(nos) + list(extra_zs)
    zs_all = np.vstack([zs, rng.dirichlet(np.ones(3), size=len(extra_zs))]) if extra_zs else zs
    order = list(range(len(zs_nos)))[::-1]
    write_zeroshot(tmp / "zs.csv", [zs_nos[i] for i in order], zs_all[order])
    return prefix, nos, zs


def make_args(tmp, prefix, **kw):
    task = tmp / "task.json"
    task.write_text(json.dumps(TASK), encoding="utf-8")
    d = dict(task=str(task), embeddings=str(prefix), zeroshot=str(tmp / "zs.csv"),
             version="v1", out_dir=str(tmp / "out"), folds=3, min_examples=2, seed=0,
             config="config.toml", target_profile=None)
    d.update(kw)
    return argparse.Namespace(**d)


def answers_for(nos, extra=()):
    t0 = datetime(2026, 1, 1)
    rows = []
    for i, no in enumerate(nos):
        rows.append((no, IDS[i % 3], "sample" if i < 12 else "queue", t0 + timedelta(minutes=i), i))
    rows += list(extra)
    return rows


def test_inputs_aligned_by_image_no(tmp_path):
    prefix, nos, zs = make_inputs(tmp_path)
    got_nos, img, z = model_train.load_inputs(prefix, tmp_path / "zs.csv", IDS)
    assert got_nos == nos
    assert img.shape == (len(nos), 6)
    assert np.allclose(z, zs, atol=1e-4)


def test_write_scores_roundtrip(tmp_path):
    probs = np.array([[0.1, 0.7, 0.2], [0.5, 0.25, 0.25]])
    out = tmp_path / "s.csv"
    model_train.write_scores(out, ["a b", "c"], probs, IDS)
    rows = list(load_model_run.read_scores(out, IDS))
    assert [r[0] for r in rows] == ["a b", "c"]
    assert [r[1] for r in rows] == ["diesel_loco", "steam_loco"]
    assert rows[0][2] == pytest.approx(0.7)
    assert rows[1][3] == pytest.approx([0.5, 0.25, 0.25])


def test_run_writes_all_outputs(tmp_path):
    prefix, nos, _ = make_inputs(tmp_path)
    args = make_args(tmp_path, prefix)
    desc = {no: f"steam loco {no}" for no in nos[:20]}
    rc = model_train.run(args, lambda t: answers_for(nos), lambda: desc)
    assert rc == 0
    out = Path(args.out_dir)
    for name in ["v1-photo.csv", "v1-photo-desc.csv", "v1-report.md",
                 "v1-photo.joblib", "v1-photo-desc.joblib"]:
        assert (out / name).exists(), name
    for name in ["v1-photo.csv", "v1-photo-desc.csv"]:
        assert len(list(load_model_run.read_scores(out / name, IDS))) == len(nos)
    report = (out / "v1-report.md").read_text()
    assert "photo only" in report and "photo + description" in report


def test_missing_embedding_reported(tmp_path):
    prefix, nos, _ = make_inputs(tmp_path)
    args = make_args(tmp_path, prefix)
    t = datetime(2026, 2, 1)
    extra = [("no embedding photo", "ship", "sample", t, 900),
             (nos[0], "not_an_answer", "sample", t + timedelta(days=1), 901)]
    rc = model_train.run(args, lambda t_: answers_for(nos, extra), lambda: {n: "steam loco " + n for n in nos})
    assert rc == 0
    report = (Path(args.out_dir) / "v1-report.md").read_text()
    assert "no embedding photo" in report
    assert nos[0] in report and "not_an_answer" in report


def test_bad_inputs_exit_2(tmp_path):
    prefix, nos, _ = make_inputs(tmp_path)
    ok = lambda t: answers_for(nos)
    assert model_train.run(make_args(tmp_path, tmp_path / "nope"), ok, lambda: {}) == 2
    bad = tmp_path / "bad.csv"
    bad.write_text("image_no,answer,p,p_x\n", encoding="utf-8")
    assert model_train.run(make_args(tmp_path, prefix, zeroshot=str(bad)), ok, lambda: {}) == 2
    assert model_train.run(make_args(tmp_path, prefix), lambda t: [], lambda: {}) == 2


class FakeCursor:
    def __init__(self, log):
        self.log = log
        self.sql = ""

    def execute(self, sql, params=None):
        self.log.append(sql)
        self.sql = sql

    def fetchall(self):
        if "model_answer" in self.sql:
            return answers_for(NOS)
        return [(n, "d " + n) for n in NOS]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass


NOS = [f"p{i:02d}" for i in range(30)]


class FakeConn:
    def __init__(self):
        self.log = []
        self.readonly = None

    def set_session(self, readonly=None, **kw):
        self.readonly = readonly

    def cursor(self):
        return FakeCursor(self.log)

    def close(self):
        pass


def test_never_writes_db(tmp_path, monkeypatch):
    import reject_log
    prefix, nos, _ = make_inputs(tmp_path)
    conn = FakeConn()
    monkeypatch.setattr(reject_log, "open_connection", lambda cfg, prof: conn)
    monkeypatch.setattr(reject_log, "load_config", lambda p: {})
    a = make_args(tmp_path, prefix)
    rc = model_train.main(["--task", a.task, "--embeddings", str(prefix),
                           "--zeroshot", a.zeroshot, "--version", "v1",
                           "--out-dir", a.out_dir, "--folds", "3", "--min-examples", "2"])
    assert rc == 0
    assert conn.readonly is True
    assert conn.log and all(s.lstrip().lower().startswith("select") for s in conn.log)


def test_outputs_independent_of_answer_row_order(tmp_path):
    prefix, nos, _ = make_inputs(tmp_path)
    desc = {no: f"steam loco {no}" for no in nos[:20]}
    rows = answers_for(nos)
    outs = []
    for name, order in (("a", rows), ("b", rows[::-1])):
        args = make_args(tmp_path, prefix, out_dir=str(tmp_path / name))
        assert model_train.run(args, lambda t, o=order: list(o), lambda: desc) == 0
        outs.append(Path(args.out_dir))
    for f in ["v1-photo.csv", "v1-photo-desc.csv", "v1-report.md"]:
        assert (outs[0] / f).read_text() == (outs[1] / f).read_text(), f
