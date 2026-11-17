import numpy as np

from model_report import blind_metrics, render_report

ANSWERS = [
    {"key": "1", "id": "steam_loco", "label": "Steam loco", "loco": True},
    {"key": "2", "id": "diesel_loco", "label": "Diesel loco", "loco": True},
    {"key": "3", "id": "station", "label": "Station", "loco": False},
]


def test_blind_metrics():
    probs = np.array([
        [0.95, 0.03, 0.02],   # a: steam -> steam, sure, right
        [0.05, 0.90, 0.05],   # b: diesel -> diesel, sure, right
        [0.10, 0.20, 0.70],   # c: station -> station, not sure, right
        [0.05, 0.05, 0.90],   # d: steam -> station, sure, wrong, loco miss
        [0.90, 0.05, 0.05],   # e: not blind, ignored
    ])
    nos = ["a", "b", "c", "d", "e"]
    blind = {"a": "steam_loco", "b": "diesel_loco", "c": "station", "d": "steam_loco"}
    m = blind_metrics(probs, nos, blind, ANSWERS)
    assert m["n"] == 4
    assert m["correct"] == 3
    assert m["accuracy"] == 0.75
    assert m["loco_correct"] == 3
    assert m["loco_accuracy"] == 0.75
    assert m["recall"]["steam_loco"] == (1, 2)
    assert m["recall"]["diesel_loco"] == (1, 1)
    assert m["recall"]["station"] == (1, 1)
    assert m["n_sure"] == 3
    assert m["sure_correct"] == 2
    assert m["correct_set"] == {"a", "b", "c"}


def _m(acc, correct, n=100):
    return {"n": n, "correct": int(acc * n), "accuracy": acc, "loco_correct": 0,
            "loco_accuracy": 0.0, "recall": {"steam_loco": (1, 2), "station": (0, 0)},
            "n_sure": 10, "sure_correct": 9, "correct_set": set(correct)}


def _rows():
    return [
        ("always steam", _m(0.3, ["a"])),
        ("zero-shot", _m(0.79, ["a", "b", "c"])),
        ("yardstick", _m(0.8, ["a", "b", "c"])),
        ("photo only", _m(0.83, ["a", "b", "d", "e"])),
        ("photo + description", _m(0.78, ["a", "d"])),
    ]


def test_render_report_pass_fail():
    out = render_report("v1", _rows(), "zero-shot", set(), [])
    lines = out.splitlines()
    photo = [l for l in lines if l.startswith("| photo only")][0]
    desc = [l for l in lines if l.startswith("| photo + description")][0]
    assert "PASS" in photo and "FAIL" not in photo
    assert "FAIL" in desc
    assert "right where zero-shot was wrong: 2 · wrong where zero-shot was right: 1" in out
    assert "right where zero-shot was wrong: 1 · wrong where zero-shot was right: 2" in out


def test_render_report_marks_unseen():
    out = render_report("v1", _rows(), "zero-shot", {"station"}, [])
    assert any("station" in l and "(zero-shot fallback)" in l for l in out.splitlines())
    assert not any("steam_loco" in l and "(zero-shot fallback)" in l for l in out.splitlines())


def test_render_report_notes():
    out = render_report("v1", _rows(), "zero-shot", set(), ["near-duplicate x", "skipped y"])
    assert "near-duplicate x" in out and "skipped y" in out
