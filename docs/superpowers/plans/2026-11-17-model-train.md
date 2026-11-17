# Trained Subject-Type Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A script that trains photo-only and photo+description subject-type classifiers on the
volunteers' answers, tests them honestly on the blind sample, and writes loader-ready score files.

**Architecture:** Pure ML in `scripts/model_fit.py` (features, fitting, out-of-fold prediction,
unseen-answer rule, yardstick), metrics and the Markdown report in `scripts/model_report.py`, and
I/O + CLI in `scripts/model_train.py` (files, read-only DB queries, outputs). Tests use small
synthetic data and never touch a database.

**Tech Stack:** Python 3.12 (WSL `venv`), numpy 1.26, scikit-learn 1.9 (new), joblib, psycopg2.

**Spec:** `docs/superpowers/specs/2026-11-17-model-train-design.md`

## Global Constraints

- The training script never writes to any database; DB access is `set_session(readonly=True)`.
- No photo or description leaves the machine; no network calls besides the `oci` read.
- Output CSV header `image_no,answer,p,p_<id>...` in the task file's answer order; all 10 answers
  in every row; must be accepted by `load_model_run.read_scores`.
- Label = each photo's latest answer for the task (order `answered_at DESC, id DESC`), any mode.
- Blind sample = photos with a `mode = 'sample'` answer; graded on their latest sample answer.
- Unsure answers train, never graded. All labelled photos are in the folds.
- 5-fold stratified cross-validation; every labelled photo's output row is its out-of-fold
  prediction; the final all-labels model scores only unlabelled photos.
- Settings (regularisation, TF-IDF size) chosen only inside each training part.
- Answers with fewer than `min_examples = 5` labels **in the training part** are "unseen":
  `p(unseen a) = z(a)`, `p(seen a) = (1 − Σ z(unseen)) · c(a)`.
- Photo features = embedding (1,152) + zero-shot log-probabilities (10). Text model adds TF-IDF of
  words and word pairs, keeping numbers and wheel notations like `4-6-0` as tokens. Blank/NULL
  description → photo-only prediction for that photo.
- Acceptance: a variant passes only if blind-sample accuracy > zero-shot's on the same photos.
  The script writes every file and exits 0 regardless; bad inputs exit non-zero (2) like the loader.
- Default seed 0; same inputs + seed → identical outputs.

## Review Focus

1. A labelled photo whose prediction came from a model that saw its label (leak through the final
   model, the TF-IDF vocabulary, or the settings search) — the page's accuracy would be inflated.
   Pinned by `test_oof_never_sees_own_label` and `test_tfidf_fitted_inside_fold` (Task 1).
2. Embedding rows and zero-shot rows in different orders or with different photo sets — every
   score shifts to the wrong photo. Pinned by `test_inputs_aligned_by_image_no` (Task 3).
3. An answered photo with no embedding, or an answer id not in the task — must be reported and
   skipped, not crash or silently relabel. Pinned by `test_missing_embedding_reported` (Task 3).
4. An answer present in the whole set but with fewer than 2 labels in some fold (StratifiedKFold
   warns; a classifier can't train on 1 class) — must fall back via the unseen rule, not crash.
   Pinned by `test_rare_answer_inside_fold` (Task 1).
5. Rows that don't sum to 1 or contain NaN after the unseen rule — the loader stores them as-is.
   Pinned by `test_probs_valid` (Task 1).

---

### Task 1: Pure model code — `scripts/model_fit.py`

**Files:**
- Create: `scripts/model_fit.py`, `requirements-model.txt` (`scikit-learn>=1.9`)
- Test: `tests/test_model_fit.py`

**Interfaces:**
- Produces:
  - `latest_labels(rows: list[tuple[str, str, str, datetime, int]]) -> tuple[dict[str, str], dict[str, str]]`
    — rows are `(image_no, answer, mode, answered_at, id)`; returns `(labels, blind)`: latest
    answer per photo, and latest *sample* answer per photo that has one.
  - `combine_unseen(c: np.ndarray, seen: list[str], z: np.ndarray, answer_ids: list[str]) -> np.ndarray`
    — `c` is `(n, len(seen))`, `z` is `(n, 10)` zero-shot probs; returns `(n, 10)`.
  - `class SubjectModel(answer_ids: list[str], use_text: bool, min_examples: int = 5, seed: int = 0)`
    with `fit(img: np.ndarray, z: np.ndarray, texts: list[str] | None, y: list[str]) -> SubjectModel`
    and `predict_proba(img, z, texts) -> np.ndarray (n, 10)`. With `use_text`, rows whose text is
    blank use the photo part only (the text model holds its own photo-only model for those).
  - `out_of_fold(make: Callable[[], SubjectModel] | Callable[[], Yardstick], img, z, texts, y, folds: int = 5, seed: int = 0) -> np.ndarray (n, 10)`
  - `class Yardstick(answer_ids, k: int = 10, weight: float | None = None, seed: int = 0)` — zero-shot
    blended with the label share of the k most similar labelled photos (cosine on embeddings); same
    `fit`/`predict_proba` signatures (ignores texts); `weight` chosen inside the training part when None.
  - `predict_all(make, img_all, z_all, texts_all, labelled_idx: np.ndarray, y: list[str], folds=5, seed=0) -> np.ndarray (N, 10)`
    — out-of-fold rows for `labelled_idx`, final-model rows for the rest.

- [ ] **Step 1: Write the failing tests** in `tests/test_model_fit.py`, on a synthetic set (e.g. 3
  separable answers in a 16-dim embedding, 30 labelled photos each, plus 2 "ship" photos; 10
  answer ids; `z` uniform unless stated):
  - `test_latest_labels`: two answers for one photo (`answered_at` tie broken by higher `id`) →
    latest wins; a photo answered `sample` then `unsure` → label is the unsure answer, blind keeps
    the sample answer; a photo with only `unsure` answers is not in `blind`.
  - `test_combine_unseen`: `z` row `[.., ship .6, ..]`, ship unseen → output ship `.6`, seen
    answers scaled to sum `.4`; rows sum to 1.
  - `test_fit_predicts_separable`: photo-only model on the synthetic set → OOF accuracy ≥ 0.9.
  - `test_oof_never_sees_own_label`: flip one labelled photo's label to an answer whose cluster is
    far away; its OOF prediction is NOT the flipped label (a leaking model would memorise it).
  - `test_tfidf_fitted_inside_fold`: a word that appears in exactly one photo's text and marks its
    unique answer → that photo's OOF prediction does not get it from the word (vocabulary is fitted
    per training part).
  - `test_text_helps_when_photo_ambiguous`: identical embeddings, answer determined by a word in
    the text → `use_text=True` OOF accuracy ≥ 0.9, photo-only ≤ 0.6.
  - `test_blank_text_uses_photo_part`: with `use_text=True`, a row with `""` or `None` text gets
    exactly the photo-only part's prediction.
  - `test_rare_answer_inside_fold`: an answer with 6 labels total (≥5 overall, <5 in some fold)
    trains without error and falls back to `z` in folds where it has <5.
  - `test_probs_valid`: every output row (OOF and final, both variants, yardstick) has no NaN, all
    values in [0, 1], sums to 1 ± 1e-6, shape `(n, 10)`.
  - `test_deterministic`: same inputs + seed → identical arrays.
  - `test_predict_all_uses_oof_for_labelled`: `predict_all` rows for `labelled_idx` equal
    `out_of_fold` output; unlabelled rows equal a model fitted on all labels.

- [ ] **Step 2: Run to verify they fail**
  Run: `venv/bin/pip install -r requirements-model.txt && venv/bin/python -m pytest tests/test_model_fit.py -q`
  Expected: FAIL (`ModuleNotFoundError: model_fit`).

- [ ] **Step 3: Implement `scripts/model_fit.py`** with the Interfaces above. Use
  `LogisticRegression` (multinomial, `max_iter` high enough to converge, `C` from a small grid via
  an inner `StratifiedKFold` on the training part only), `TfidfVectorizer` with
  `token_pattern=r"(?u)\b\w[\w-]*\b"` (keeps `4-6-0`, `47279`), `ngram_range=(1, 2)`,
  `sublinear_tf=True`, and stack photo features with `scipy.sparse.hstack`. Zero-shot input =
  `log(z + 1e-6)`. Outer folds: `StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)`;
  labels with fewer than 5 members across all labels stratify as one bucket so splitting never fails.

- [ ] **Step 4: Run to verify they pass**
  Run: `venv/bin/python -m pytest tests/test_model_fit.py -q`
  Expected: all pass, no warnings promoted to errors.

- [ ] **Step 5: Commit**
  `git add scripts/model_fit.py tests/test_model_fit.py requirements-model.txt && git commit -m "feat: subject-type classifier with honest out-of-fold prediction"`

### Task 2: Metrics and report — `scripts/model_report.py`

**Files:**
- Create: `scripts/model_report.py`
- Test: `tests/test_model_report.py`

**Interfaces:**
- Consumes: probability arrays `(n, 10)` from Task 1.
- Produces:
  - `blind_metrics(probs: np.ndarray, image_nos: list[str], blind: dict[str, str], answers: list[dict], sure: float = 0.9) -> dict`
    with keys `n`, `correct`, `accuracy`, `loco_correct`, `loco_accuracy`, `recall` (dict
    answer_id → `(right, total)`), `n_sure`, `sure_correct`, `correct_set` (set of image_nos).
    `answers` are task answer dicts (`id`, `label`, `loco`); loco yes/no uses the `loco` flag.
  - `render_report(version: str, rows: list[tuple[str, dict]], zero_shot_key: str, unseen: set[str], notes: list[str]) -> str`
    — Markdown. `rows` is `[(name, metrics)]` in order: always steam, zero-shot, yardstick, photo
    only, photo + description.

- [ ] **Step 1: Write the failing tests**:
  - `test_blind_metrics`: 4 blind photos (3 right, 1 wrong; one loco-vs-not miss) plus 1 labelled
    non-blind photo that must be ignored → `n == 4`, `accuracy == 0.75`, loco figures, recall per
    answer, `n_sure`/`sure_correct` for rows with max p ≥ 0.9.
  - `test_render_report_pass_fail`: zero-shot 0.79, photo 0.83, photo+desc 0.78 → report contains
    `PASS` on the photo line and `FAIL` on photo+desc; contains the paired counts line
    "right where zero-shot was wrong: N · wrong where zero-shot was right: M" for each variant.
  - `test_render_report_marks_unseen`: answers in `unseen` are marked "(zero-shot fallback)" in the
    per-answer table.
  - `test_render_report_notes`: every string in `notes` appears (used for the near-duplicate note
    and skipped photos).

- [ ] **Step 2: Run to verify they fail**
  Run: `venv/bin/python -m pytest tests/test_model_report.py -q` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `scripts/model_report.py`.** "Always steam" metrics are built by the
  caller as one-hot probabilities on `steam_loco`; the report needs no special case.

- [ ] **Step 4: Run to verify they pass** → all pass.

- [ ] **Step 5: Commit**
  `git add scripts/model_report.py tests/test_model_report.py && git commit -m "feat: blind-sample metrics and report for trained models"`

### Task 3: CLI and I/O — `scripts/model_train.py`

**Files:**
- Create: `scripts/model_train.py`
- Modify: `CLAUDE.md` ("Where things live": one row for `model_train.py` and `requirements-model.txt`)
- Test: `tests/test_model_train.py`

**Interfaces:**
- Consumes: Task 1 and Task 2 functions; `load_model_run.task_file_errors`, `load_model_run.read_scores`.
- Produces:
  - `load_inputs(emb_prefix: Path, zeroshot_csv: Path, answer_ids: list[str]) -> tuple[list[str], np.ndarray, np.ndarray]`
    — `(image_nos, img, z)` aligned by image_no (intersection; order = embedding file order).
  - `write_scores(path: Path, image_nos: list[str], probs: np.ndarray, answer_ids: list[str]) -> None`
    — p formatted `.4f`; `answer`/`p` = argmax.
  - `run(args, fetch_answers: Callable[[str], list[tuple]], fetch_descriptions: Callable[[], dict[str, str | None]]) -> int`
    — does everything except opening the DB; tests pass fakes.
  - `main(argv=None) -> int` — parses args, opens `reject_log.open_connection(...)` with
    `set_session(readonly=True)`, builds the two fetchers, calls `run`.
- CLI flags: `--task`, `--embeddings` (prefix), `--zeroshot`, `--version`, `--out-dir`
  (default `exports/ai_models`), `--folds` (5), `--min-examples` (5), `--seed` (0), `--config`
  (`config.toml`), `--target-profile`.
- Outputs: `<out>/<version>-photo.csv`, `-photo-desc.csv`, `-report.md`, `-photo.joblib`,
  `-photo-desc.joblib` (final all-labels models).

- [ ] **Step 1: Write the failing tests** (fake fetchers, tmp files; a tiny `.npy` +
  `.image_nos.txt` and zero-shot CSV written by the test):
  - `test_inputs_aligned_by_image_no`: zero-shot CSV rows in reverse order and one extra photo →
    `z` rows match each embedding row's photo; extra photo dropped.
  - `test_write_scores_roundtrip`: output read back by `load_model_run.read_scores` gives the same
    image_nos, argmax answers and probabilities (to 4 dp).
  - `test_run_writes_all_outputs`: `run(...)` returns 0 and writes the five files; each CSV has one
    row per embedded photo; report exists and mentions both variants.
  - `test_missing_embedding_reported`: an answered photo with no embedding and an answer id not in
    the task → both skipped, both named in the report's notes, exit 0.
  - `test_bad_inputs_exit_2`: missing embeddings file → 2; zero-shot header mismatch → 2; no labels
    at all → 2.
  - `test_never_writes_db`: `main` with a fake connection whose `cursor().execute` records SQL →
    only `SELECT` statements, and `set_session(readonly=True)` was called (monkeypatch
    `reject_log.open_connection`).

- [ ] **Step 2: Run to verify they fail** → FAIL (`ModuleNotFoundError: model_train`).

- [ ] **Step 3: Implement `scripts/model_train.py`.** Queries:
  `select image_no, answer, mode, answered_at, id from rat.model_answer where task_id = %s` and
  `select image_no, description from rat.catalog`. Report notes always include the near-duplicate
  caveat from the spec and the list of skipped photos. Module docstring gives the spec's example
  command and the two loader commands.

- [ ] **Step 4: Run to verify they pass**, then the full suite:
  Run: `venv/bin/python -m pytest tests -q` → all pass (132 existing + new).

- [ ] **Step 5: Commit**
  `git add scripts/model_train.py tests/test_model_train.py CLAUDE.md && git commit -m "feat: model_train CLI writes trained scores and an honest report"`

### Task 4: Real run (read-only) and hand-off

- [ ] **Step 1: Run on the real data**
  Run: `venv/bin/python scripts/model_train.py --task scripts/model_tasks/rat_subject_type.json --embeddings exports/ai_pilot/emb_siglip_so400m --zeroshot exports/ai_pilot/subject_scores.csv --version 2026-11-18-trained --target-profile oci`
  Expected: exit 0; `exports/ai_models/2026-11-18-trained-report.md` shows zero-shot at
  324/411 (79%) — if not, the blind grading disagrees with the page: stop and fix.

- [ ] **Step 2: Dry-run the loader on each passing variant** (dry run only; `--apply` is the user's):
  `venv/bin/python scripts/load_model_run.py --task scripts/model_tasks/rat_subject_type.json --scores exports/ai_models/2026-11-18-trained-photo.csv --model "logreg(siglip-so400m)" --version 2026-11-18-trained-photo --target-profile oci`
  and the same for `-photo-desc.csv` with `--model "logreg(siglip-so400m+tfidf)"`.
  Expected: "would load 141111 … dry run: nothing written".

- [ ] **Step 3: Hand the report and the `--apply` commands to the user** (photo-only first).
