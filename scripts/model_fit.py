"""Subject-type classifier over saved SigLIP embeddings — pure model code, no I/O.

Honesty is the point: a labelled photo's output always comes from a model that was
not trained on its label (out-of-fold), and every setting (regularisation, TF-IDF
size, the yardstick's blend weight) and the TF-IDF vocabulary are chosen inside the
training part only. The final all-labels model scores only unlabelled photos.

Answers with fewer than `min_examples` labels in a training part are "unseen": they
keep the zero-shot probability, and the classifier's probabilities over the seen
answers fill the remainder (see `combine_unseen`).
"""
from __future__ import annotations

from collections import Counter
from typing import Callable, Iterator

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler

C_GRID = (0.1, 1.0, 10.0, 100.0)
TFIDF_SIZES = (2000, None)          # max_features; None keeps the whole vocabulary
INNER_FOLDS = 3
WEIGHT_GRID = tuple(i / 10 for i in range(11))
ZS_FLOOR = 1e-6
CHUNK = 8192                        # rows per predict batch (bounds memory on ~141k photos)
TOKEN_PATTERN = r"(?u)\b\w[\w-]*\b"  # keeps "4-6-0" and "47279" as tokens
_RARE = "\x00rare"                  # stratum for answers too small to stratify alone


def latest_labels(rows) -> tuple[dict[str, str], dict[str, str]]:
    """rows: (image_no, answer, mode, answered_at, id). Returns (labels, blind):
    each photo's latest answer (any mode), and its latest `sample` answer if it has one.
    Latest = greatest (answered_at, id)."""
    labels: dict[str, tuple] = {}
    blind: dict[str, tuple] = {}
    for image_no, answer, mode, answered_at, row_id in rows:
        key = (answered_at, row_id)
        if image_no not in labels or key > labels[image_no][0]:
            labels[image_no] = (key, answer)
        if mode == "sample" and (image_no not in blind or key > blind[image_no][0]):
            blind[image_no] = (key, answer)
    return ({k: v[1] for k, v in labels.items()}, {k: v[1] for k, v in blind.items()})


def _normalise(p: np.ndarray) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=np.float64), 0.0, None)
    s = p.sum(axis=1, keepdims=True)
    uniform = np.full_like(p, 1.0 / p.shape[1]) if p.shape[1] else p
    return np.where(s > 0, p / np.where(s > 0, s, 1.0), uniform)


def combine_unseen(c: np.ndarray, seen: list[str], z: np.ndarray,
                   answer_ids: list[str]) -> np.ndarray:
    """c: (n, len(seen)) classifier probabilities over `seen`, in that order; z: (n, 10)
    zero-shot probabilities in `answer_ids` order. p(unseen a) = z(a);
    p(seen a) = (1 - sum z(unseen)) * c(a). Rows sum to 1."""
    z = _normalise(z)
    n = z.shape[0]
    seen_idx = [answer_ids.index(a) for a in seen]
    unseen = np.ones(len(answer_ids), dtype=bool)
    unseen[seen_idx] = False
    out = np.zeros((n, len(answer_ids)))
    out[:, unseen] = z[:, unseen]
    if seen_idx:
        rest = 1.0 - out.sum(axis=1, keepdims=True)
        out[:, seen_idx] = rest * _normalise(np.asarray(c).reshape(n, len(seen_idx)))
    return _normalise(out)


def _photo_features(img: np.ndarray, z: np.ndarray) -> np.ndarray:
    return np.hstack([np.asarray(img, dtype=np.float64),
                      np.log(np.asarray(z, dtype=np.float64) + ZS_FLOOR)])


def _clean_texts(texts, n: int) -> list[str]:
    if texts is None:
        return [""] * n
    return [t if isinstance(t, str) else "" for t in texts]


def _blank(texts: list[str]) -> np.ndarray:
    return np.array([not t.strip() for t in texts], dtype=bool)


def _vectorizer(size) -> TfidfVectorizer:
    return TfidfVectorizer(token_pattern=TOKEN_PATTERN, ngram_range=(1, 2),
                           sublinear_tf=True, max_features=size)


def _design(X_photo: np.ndarray, scaler: StandardScaler, vec: TfidfVectorizer | None,
            texts: list[str] | None):
    """Standardised photo columns (scaler fitted on the classifier's own training rows:
    unit-norm embedding components are ~0.03 while zero-shot logs reach -13.8, which
    slows lbfgs by ~50x and skews the L2 penalty), plus the TF-IDF block if any."""
    X = scaler.transform(X_photo)
    if vec is None:
        return X
    return sparse.hstack([sparse.csr_matrix(X), vec.transform(texts)]).tocsr()


def _logreg(C: float, seed: int) -> LogisticRegression:
    return LogisticRegression(C=C, max_iter=10000, random_state=seed)


def _inner_splits(y_codes: np.ndarray) -> int:
    return min(INNER_FOLDS, int(np.bincount(y_codes).min()))


def outer_folds(y: list[str], folds: int = 5, seed: int = 0) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """Stratified (train_idx, test_idx) pairs. Answers with fewer than max(5, folds)
    labels stratify together as one bucket; a bucket still too small to spread over
    the folds joins the largest answer's stratum, so splitting never fails or warns."""
    y = list(y)
    if len(y) < folds:
        raise ValueError(f"{len(y)} labelled photos is fewer than {folds} folds")
    counts = Counter(y)
    small = max(5, folds)
    strata = [a if counts[a] >= small else _RARE for a in y]
    n_rare = strata.count(_RARE)
    if 0 < n_rare < folds:
        biggest = counts.most_common(1)[0][0]
        strata = [biggest if s == _RARE else s for s in strata]
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=seed)
    yield from skf.split(np.zeros((len(y), 1)), strata)


class SubjectModel:
    """Logistic regression on photo features (embedding + zero-shot log-probabilities),
    optionally plus TF-IDF of the description. With `use_text`, rows whose text is
    blank are predicted by the model's own photo-only part (`photo_model_`).

    After fit: `seen_` = the answers the classifier trained on (>= min_examples labels),
    in answer_ids order; every other answer falls back to zero-shot."""

    def __init__(self, answer_ids: list[str], use_text: bool, min_examples: int = 5,
                 seed: int = 0):
        self.answer_ids = list(answer_ids)
        self.use_text = use_text
        self.min_examples = min_examples
        self.seed = seed

    def fit(self, img, z, texts, y) -> "SubjectModel":
        y = list(y)
        unknown = sorted(set(y) - set(self.answer_ids))
        if unknown:
            raise ValueError(f"answers not in the task: {unknown}")
        counts = Counter(y)
        self.seen_ = [a for a in self.answer_ids if counts[a] >= self.min_examples]
        keep = np.array([a in self.seen_ for a in y], dtype=bool)
        X_photo = _photo_features(np.asarray(img)[keep], np.asarray(z)[keep])
        codes = np.array([self.seen_.index(a) for a, k in zip(y, keep) if k], dtype=int)
        kept_texts = None
        if self.use_text:
            all_texts = _clean_texts(texts, len(y))
            kept_texts = [t for t, k in zip(all_texts, keep) if k]
            self.photo_model_ = SubjectModel(self.answer_ids, use_text=False,
                                             min_examples=self.min_examples,
                                             seed=self.seed).fit(img, z, None, y)
        self.C_, self.tfidf_size_ = None, None
        self.scaler_, self.vectorizer_, self.clf_ = None, None, None
        if len(self.seen_) < 2:
            return self                       # 0 or 1 seen answers: nothing to learn
        sizes = TFIDF_SIZES if self.use_text else (None,)
        self.C_, self.tfidf_size_ = self._search(X_photo, kept_texts, codes, sizes)
        self.scaler_ = StandardScaler().fit(X_photo)
        if self.use_text:
            self.vectorizer_ = _vectorizer(self.tfidf_size_).fit(kept_texts)
        self.clf_ = _logreg(self.C_, self.seed).fit(
            _design(X_photo, self.scaler_, self.vectorizer_, kept_texts), codes)
        return self

    def _search(self, X_photo, texts, codes, sizes):
        """Pick (C, TF-IDF size) by mean log loss over inner folds of the training part.
        The scaler and the vectorizer are refitted on each inner training part."""
        n_splits = _inner_splits(codes)
        if n_splits < 2:
            return C_GRID[len(C_GRID) // 2], sizes[0]
        skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=self.seed)
        splits = list(skf.split(X_photo, codes))
        labels = np.arange(len(self.seen_))
        best, best_loss = None, np.inf
        for size in sizes:
            designs = []
            for tr, te in splits:
                scaler = StandardScaler().fit(X_photo[tr])
                vec, t_tr, t_te = None, None, None
                if self.use_text:
                    t_tr, t_te = [texts[i] for i in tr], [texts[i] for i in te]
                    vec = _vectorizer(size).fit(t_tr)
                designs.append((_design(X_photo[tr], scaler, vec, t_tr),
                                _design(X_photo[te], scaler, vec, t_te)))
            for C in C_GRID:
                loss = 0.0
                for (tr, te), (Xtr, Xte) in zip(splits, designs):
                    clf = _logreg(C, self.seed).fit(Xtr, codes[tr])
                    loss += log_loss(codes[te], clf.predict_proba(Xte), labels=labels)
                if loss < best_loss - 1e-12:
                    best, best_loss = (C, size), loss
        return best

    def _classify(self, X_photo: np.ndarray, texts: list[str] | None) -> np.ndarray:
        n = X_photo.shape[0]
        if self.clf_ is None:
            return np.ones((n, len(self.seen_)))
        return self.clf_.predict_proba(_design(X_photo, self.scaler_, self.vectorizer_, texts))

    def predict_proba(self, img, z, texts) -> np.ndarray:
        n = len(img)
        out = np.empty((n, len(self.answer_ids)))
        all_texts = _clean_texts(texts, n) if self.use_text else None
        for s in range(0, n, CHUNK):
            sl = slice(s, s + CHUNK)
            zc = np.asarray(z[sl])
            if not self.use_text:
                c = self._classify(_photo_features(img[sl], zc), None)
                out[sl] = combine_unseen(c, self.seen_, zc, self.answer_ids)
                continue
            tc = all_texts[sl]
            blank = _blank(tc)
            part = np.empty((len(tc), len(self.answer_ids)))
            if blank.any():
                part[blank] = self.photo_model_.predict_proba(
                    np.asarray(img[sl])[blank], zc[blank], None)
            if (~blank).any():
                full = [t for t, b in zip(tc, blank) if not b]
                c = self._classify(_photo_features(np.asarray(img[sl])[~blank], zc[~blank]), full)
                part[~blank] = combine_unseen(c, self.seen_, zc[~blank], self.answer_ids)
            out[sl] = part
        return out


class Yardstick:
    """Zero-shot nudged by the nearest labelled photos: p = (1 - w) * z + w * share,
    where share = answer shares among the k most similar training photos (cosine on
    embeddings). With weight=None, w is chosen on the training part (leave-one-out,
    each photo's own label excluded from its neighbours) by mean log loss.
    Ignores texts."""

    def __init__(self, answer_ids: list[str], k: int = 10, weight: float | None = None,
                 seed: int = 0):
        self.answer_ids = list(answer_ids)
        self.k = k
        self.weight = weight
        self.seed = seed

    def fit(self, img, z, texts, y) -> "Yardstick":
        unknown = sorted(set(y) - set(self.answer_ids))
        if unknown:
            raise ValueError(f"answers not in the task: {unknown}")
        self.emb_ = _unit_rows(np.asarray(img, dtype=np.float64))
        self.onehot_ = np.zeros((len(y), len(self.answer_ids)))
        self.onehot_[np.arange(len(y)), [self.answer_ids.index(a) for a in y]] = 1.0
        if self.weight is not None:
            self.weight_ = float(self.weight)
        elif len(y) < 2:
            self.weight_ = 0.0
        else:
            share = self._share(self.emb_, exclude_self=True)
            zz = _normalise(z)
            codes = self.onehot_.argmax(axis=1)
            best, best_loss = 0.0, np.inf
            for w in WEIGHT_GRID:
                p = (1 - w) * zz + w * share
                loss = -np.mean(np.log(np.clip(p[np.arange(len(y)), codes], 1e-12, None)))
                if loss < best_loss - 1e-12:
                    best, best_loss = w, loss
            self.weight_ = best
        return self

    def _share(self, q: np.ndarray, exclude_self: bool = False) -> np.ndarray:
        sim = q @ self.emb_.T
        if exclude_self:
            np.fill_diagonal(sim, -np.inf)
        k = min(self.k, self.emb_.shape[0] - (1 if exclude_self else 0))
        if k <= 0:
            return np.full((q.shape[0], len(self.answer_ids)), 1.0 / len(self.answer_ids))
        # stable sort: ties go to the earlier training photo, so results are deterministic
        nn = np.argsort(-sim, axis=1, kind="stable")[:, :k]
        return self.onehot_[nn].mean(axis=1)

    def predict_proba(self, img, z, texts=None) -> np.ndarray:
        n = len(img)
        out = np.empty((n, len(self.answer_ids)))
        for s in range(0, n, CHUNK):
            sl = slice(s, s + CHUNK)
            share = self._share(_unit_rows(np.asarray(img[sl], dtype=np.float64)))
            out[sl] = _normalise((1 - self.weight_) * _normalise(z[sl]) + self.weight_ * share)
        return out


def _unit_rows(a: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(a, axis=1, keepdims=True)
    return a / np.where(norm > 0, norm, 1.0)


def _take(texts, idx):
    return None if texts is None else [texts[i] for i in idx]


def out_of_fold(make: Callable[[], SubjectModel] | Callable[[], Yardstick], img, z, texts,
                y, folds: int = 5, seed: int = 0) -> np.ndarray:
    """(n, 10): each labelled photo's prediction from a model (made fresh by `make`)
    fitted on the other folds only."""
    y = list(y)
    img, z = np.asarray(img), np.asarray(z)
    out = None
    for tr, te in outer_folds(y, folds, seed):
        model = make().fit(img[tr], z[tr], _take(texts, tr), [y[i] for i in tr])
        p = model.predict_proba(img[te], z[te], _take(texts, te))
        if out is None:
            out = np.empty((len(y), p.shape[1]))
        out[te] = p
    return out


def predict_all(make, img_all, z_all, texts_all, labelled_idx: np.ndarray, y: list[str],
                folds: int = 5, seed: int = 0) -> np.ndarray:
    """(N, 10): out-of-fold rows for `labelled_idx` (y[j] labels photo labelled_idx[j]),
    final all-labels model rows for every other photo. The final model is the last one
    `make` creates, so a caller can keep a reference to it (e.g. to save it or read
    `seen_`)."""
    labelled_idx = np.asarray(labelled_idx, dtype=int)
    N = len(img_all)
    img_l = np.asarray(img_all[labelled_idx])
    z_l = np.asarray(z_all[labelled_idx])
    texts_l = _take(texts_all, labelled_idx)
    oof = out_of_fold(make, img_l, z_l, texts_l, y, folds=folds, seed=seed)
    final = make().fit(img_l, z_l, texts_l, list(y))
    out = np.empty((N, oof.shape[1]))
    out[labelled_idx] = oof
    rest = np.setdiff1d(np.arange(N), labelled_idx)
    for s in range(0, len(rest), CHUNK):
        idx = rest[s:s + CHUNK]
        out[idx] = final.predict_proba(np.asarray(img_all[idx]), np.asarray(z_all[idx]),
                                       _take(texts_all, idx))
    return out
