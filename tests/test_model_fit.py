"""Tests for scripts/model_fit.py (pure model code, no database).

Synthetic set: 3 separable answers in a 16-dim embedding (30 labelled photos each)
plus 2 "ship" photos; 10 answer ids, the real task's order."""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

import model_fit as mf

ANSWERS = ["steam_loco", "diesel_loco", "electric_loco", "railcar_unit", "tram",
           "bus_lorry", "wagons_coaches", "ship", "station_building_track", "other"]
CLUSTERS = ["steam_loco", "diesel_loco", "tram"]
WORDS = {"steam_loco": "kettle", "diesel_loco": "growler", "tram": "streetcar",
         "ship": "ferry", "electric_loco": "pantograph"}
FILLER = ["photo", "of", "a", "at", "the", "station", "near", "yard", "class", "47279",
          "4-6-0", "shed", "line", "works"]


def _unit(a: np.ndarray) -> np.ndarray:
    return a / np.linalg.norm(a, axis=1, keepdims=True)


def _text(rng, word: str) -> str:
    return " ".join([word] + list(rng.choice(FILLER, size=4)))


def make_set(seed: int = 0, n_per: int = 30, extra: dict[str, int] | None = None):
    """img (n,16) float16 unit rows, z (n,10) uniform, texts, y."""
    rng = np.random.default_rng(seed)
    groups = {a: n_per for a in CLUSTERS}
    groups["ship"] = 2
    groups.update(extra or {})
    centers = _unit(rng.normal(size=(len(groups), 16)))
    img, y, texts = [], [], []
    for c, (a, n) in zip(centers, groups.items()):
        img.append(_unit(c + 0.1 * rng.normal(size=(n, 16))))
        y += [a] * n
        texts += [_text(rng, WORDS[a]) for _ in range(n)]
    img = np.vstack(img).astype(np.float16)
    z = np.full((len(y), 10), 0.1)
    return img, z, texts, y


def accuracy(p: np.ndarray, y: list[str]) -> float:
    pred = [ANSWERS[i] for i in p.argmax(axis=1)]
    return float(np.mean([a == b for a, b in zip(pred, y)]))


def photo_model():
    return mf.SubjectModel(ANSWERS, use_text=False)


def text_model():
    return mf.SubjectModel(ANSWERS, use_text=True)


def test_latest_labels():
    t = datetime(2026, 11, 1, 12, 0)
    rows = [
        ("a1", "tram", "unsure", t, 1),
        ("a1", "steam_loco", "unsure", t, 5),       # same time, higher id wins
        ("a1", "diesel_loco", "unsure", t, 3),
        ("b1", "ship", "sample", datetime(2026, 11, 1), 2),
        ("b1", "other", "unsure", datetime(2026, 11, 2), 4),
        ("c1", "tram", "unsure", datetime(2026, 11, 3), 6),
    ]
    labels, blind = mf.latest_labels(rows)
    assert labels == {"a1": "steam_loco", "b1": "other", "c1": "tram"}
    assert blind == {"b1": "ship"}


def test_combine_unseen():
    seen = ["steam_loco", "tram"]
    z = np.zeros((2, 10))
    z[0, ANSWERS.index("ship")] = 0.6
    z[0, ANSWERS.index("steam_loco")] = 0.2
    z[0, ANSWERS.index("tram")] = 0.2
    z[1] = 0.1
    c = np.array([[0.75, 0.25], [0.5, 0.5]])
    out = mf.combine_unseen(c, seen, z, ANSWERS)
    assert out.shape == (2, 10)
    s, t = ANSWERS.index("steam_loco"), ANSWERS.index("tram")
    assert out[0, ANSWERS.index("ship")] == pytest.approx(0.6)
    assert out[0, s] + out[0, t] == pytest.approx(0.4)
    assert out[0, s] == pytest.approx(0.4 * 0.75)
    assert out[0, t] == pytest.approx(0.4 * 0.25)
    # second row: 8 unseen answers keep 0.1 each, the two seen share 0.2
    assert out[1, s] == pytest.approx(0.1)
    np.testing.assert_allclose(out[1, [i for i in range(10) if i not in (s, t)]], 0.1)
    np.testing.assert_allclose(out.sum(axis=1), 1.0)


def test_fit_predicts_separable():
    img, z, texts, y = make_set()
    p = mf.out_of_fold(photo_model, img, z, None, y)
    assert accuracy(p, y) >= 0.9


def test_oof_never_sees_own_label():
    img, z, texts, y = make_set()
    i = 0                                   # a steam photo, far from the tram cluster
    assert y[i] == "steam_loco"
    y[i] = "tram"
    p = mf.out_of_fold(photo_model, img, z, None, y)
    assert ANSWERS[p[i].argmax()] != "tram"
    # a model that saw the label does memorise it, so the test can tell the two apart
    leak = photo_model().fit(img, z, None, y).predict_proba(img[[i]], z[[i]], None)
    assert ANSWERS[leak[0].argmax()] == "tram"


def test_tfidf_fitted_inside_fold():
    img, z, texts, y = make_set()
    i = 0
    y[i] = "tram"                           # its photo is steam; only its text marks it
    texts[i] = "zqxjv " + texts[i]          # a word in exactly one photo's text
    made = []

    def make():
        m = text_model()
        made.append(m)
        return m

    p = mf.out_of_fold(make, img, z, texts, y)
    assert ANSWERS[p[i].argmax()] != "tram"
    # the vocabulary is fitted per training part: the one part without photo i lacks the word
    has_word = ["zqxjv" in m.vectorizer_.vocabulary_ for m in made]
    assert len(made) == 5
    assert has_word.count(False) == 1
    # a model fitted with photo i's text and label does pick the label up from the word
    leak = text_model().fit(img, z, texts, y).predict_proba(img[[i]], z[[i]], texts[i:i + 1])
    assert ANSWERS[leak[0].argmax()] == "tram"


def test_text_helps_when_photo_ambiguous():
    img, z, texts, y = make_set()
    img[:] = img[0]                         # every photo looks the same
    with_text = mf.out_of_fold(text_model, img, z, texts, y)
    photo_only = mf.out_of_fold(photo_model, img, z, None, y)
    assert accuracy(with_text, y) >= 0.9
    assert accuracy(photo_only, y) <= 0.6


def test_blank_text_uses_photo_part():
    img, z, texts, y = make_set()
    tm = text_model().fit(img, z, texts, y)
    pm = photo_model().fit(img, z, None, y)
    q_texts = ["", None, "   ", texts[0]]
    p_text = tm.predict_proba(img[:4], z[:4], q_texts)
    p_photo = pm.predict_proba(img[:4], z[:4], None)
    # exactly the text model's own photo-only part on those rows ...
    np.testing.assert_array_equal(p_text[:3], tm.photo_model_.predict_proba(img[:3], z[:3], None))
    # ... which is the same model as a separately fitted photo-only one (BLAS batch
    # size can move the last bit, hence allclose)
    np.testing.assert_allclose(p_text[:3], p_photo[:3], rtol=1e-12, atol=1e-15)
    assert not np.array_equal(p_text[3], p_photo[3])


def test_rare_answer_inside_fold():
    img, z, texts, y = make_set(extra={"electric_loco": 6})
    e = ANSWERS.index("electric_loco")
    z = np.full((len(y), 10), 0.63 / 9)
    z[:, e] = 0.37
    made = []

    def make():
        m = photo_model()
        made.append(m)
        return m

    p = mf.out_of_fold(make, img, z, None, y)
    folds = list(mf.outer_folds(y, 5, 0))
    yarr = np.array(y)
    fell_back = 0
    for m, (tr, te) in zip(made, folds):
        n_train = int((yarr[tr] == "electric_loco").sum())
        if n_train < 5:
            fell_back += 1
            assert "electric_loco" not in m.seen_
            np.testing.assert_allclose(p[te, e], 0.37)
        else:
            assert "electric_loco" in m.seen_
    assert fell_back >= 1
    assert fell_back < 5


@pytest.mark.parametrize("make", [photo_model, text_model,
                                  lambda: mf.Yardstick(ANSWERS)],
                         ids=["photo", "text", "yardstick"])
def test_probs_valid(make):
    img, z, texts, y = make_set(extra={"electric_loco": 6})
    rng = np.random.default_rng(1)
    z = rng.dirichlet(np.ones(10), size=len(y))
    z[0] = 0.0
    z[0, ANSWERS.index("ship")] = 1.0       # an extreme zero-shot row
    texts[3] = None
    texts[4] = ""
    oof = mf.out_of_fold(make, img, z, texts, y)
    final = make().fit(img, z, texts, y).predict_proba(img, z, texts)
    for p in (oof, final):
        assert p.shape == (len(y), 10)
        assert not np.isnan(p).any()
        assert (p >= 0).all() and (p <= 1).all()
        np.testing.assert_allclose(p.sum(axis=1), 1.0, atol=1e-6)


@pytest.mark.parametrize("make", [text_model, lambda: mf.Yardstick(ANSWERS)],
                         ids=["text", "yardstick"])
def test_deterministic(make):
    img, z, texts, y = make_set()
    a = mf.out_of_fold(make, img, z, texts, y, seed=3)
    b = mf.out_of_fold(make, img, z, texts, y, seed=3)
    np.testing.assert_array_equal(a, b)


def test_predict_all_uses_oof_for_labelled():
    img, z, texts, y = make_set()
    rng = np.random.default_rng(7)
    n_unl = 20
    unl = _unit(rng.normal(size=(n_unl, 16))).astype(np.float16)
    N = len(y) + n_unl
    order = rng.permutation(N)              # labelled photos scattered among the rest
    labelled_idx = np.sort(order[:len(y)])
    unlabelled_idx = np.sort(order[len(y):])
    img_all = np.zeros((N, 16), dtype=np.float16)
    img_all[labelled_idx] = img
    img_all[unlabelled_idx] = unl
    z_all = np.full((N, 10), 0.1)
    texts_all = [""] * N
    for j, i in enumerate(labelled_idx):
        texts_all[i] = texts[j]
    made = []

    def make():
        m = text_model()
        made.append(m)
        return m

    out = mf.predict_all(make, img_all, z_all, texts_all, labelled_idx, y)
    assert out.shape == (N, 10)
    oof = mf.out_of_fold(text_model, img, z, texts, y)
    np.testing.assert_array_equal(out[labelled_idx], oof)
    final = text_model().fit(img, z, texts, y)
    expect = final.predict_proba(img_all[unlabelled_idx], z_all[unlabelled_idx],
                                 [texts_all[i] for i in unlabelled_idx])
    np.testing.assert_array_equal(out[unlabelled_idx], expect)
    # the last model made is the final all-labels model (Task 3 reads its seen_)
    assert len(made) == 6
    assert made[-1].seen_ == final.seen_


def _photo_X(img, z):
    return np.hstack([np.asarray(img, dtype=np.float64), np.log(np.asarray(z) + 1e-6)])


@pytest.mark.parametrize("make", [photo_model, text_model], ids=["photo", "text"])
def test_scaler_fitted_inside_training_part(make, monkeypatch):
    """Photo features are standardised by a scaler fitted only on the rows each
    classifier trains on (outer training part, and each inner split of the search)."""
    img, z, texts, y = make_set()
    z = np.random.default_rng(2).dirichlet(np.ones(10), size=len(y))
    X = _photo_X(img, z)
    fits = []                               # (fold, rows each scaler was fitted on)
    fold = [-1]

    class Recording(mf.StandardScaler):
        def fit(self, A, *args, **kwargs):
            fits.append((fold[0], np.asarray(A).copy()))
            return super().fit(A, *args, **kwargs)

    monkeypatch.setattr(mf, "StandardScaler", Recording)
    made = []

    def rec():
        fold[0] += 1
        m = make()
        made.append(m)
        return m

    mf.out_of_fold(rec, img, z, texts, y)
    folds = list(mf.outer_folds(y, 5, 0))
    assert fits and {k for k, _ in fits} == set(range(5))
    for k, A in fits:
        train_rows = {r.tobytes() for r in X[folds[k][0]]}
        assert all(r.tobytes() in train_rows for r in A), f"fold {k} scaler saw held-out rows"
    yarr = np.array(y)
    for m, (tr, _) in zip(made, folds):
        parts = [m] + ([m.photo_model_] if m.use_text else [])
        for part in parts:
            trained = tr[np.isin(yarr[tr], part.seen_)]   # rows the classifier fits on
            np.testing.assert_allclose(part.scaler_.mean_, X[trained].mean(axis=0))


def test_fit_converges_with_scale_imbalance():
    """Unit-norm embedding components (~0.06) beside zero-shot logs (down to -13.8):
    unscaled, lbfgs needs hundreds of iterations; standardised, a handful."""
    rng = np.random.default_rng(0)
    centers = _unit(rng.normal(size=(6, 256)))
    img = np.vstack([_unit(c + 0.25 * rng.normal(size=(40, 256))) for c in centers])
    y = [ANSWERS[i] for i in np.repeat([0, 1, 2, 4, 6, 9], 40)]
    z = rng.dirichlet(np.full(10, 0.3), size=len(y))
    m = photo_model().fit(img.astype(np.float16), z, None, y)
    assert m.clf_.n_iter_.max() < 50
