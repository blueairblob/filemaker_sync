# Trained subject-type model — design

Status: draft for the user's approval (2026-11-17).

## Why

Model check (`/admin/model-check`) holds one zero-shot SigLIP run (`2026-11-16-zeroshot`) for the
task `subject_type` ("What's the main subject?", 10 answers). On the blind random sample it scores
**79% (324 of 411)** — barely above always answering "steam loco" (**78%**, 322 of 411). Volunteers
have now checked 547 photos (411 blind sample + 136 unsure). A model trained on those answers should
do clearly better.

What it is for (the user's words, 2026-11-17): an **assist to populate the subject type** — a
suggestion a person confirms. Later, ideally, pre-populate fields *before* a volunteer fills in the
form for a new photo; much later, scene search ("train on a bridge in winter"). Baby steps: this
design covers only the first.

## Goals

- A trained model whose suggestions are right more often than the zero-shot run, **measured
  honestly** on the blind sample.
- Two variants (the user's choice, C): **photo only** (works for a new photo before any
  description exists) and **photo + description** (for already catalogued photos). Both scored and
  reported side by side, so it is visible what the description adds.
- Output loads into Model check through the existing loader as ordinary runs, so volunteers see the
  new suggestions with no page changes, and the Unsure queue is driven by the better model.
- Cheap to repeat: re-run whenever more answers exist; each run a new version.

## Non-goals

- Pre-populating the FileMaker or web entry form; scene search; other fields.
- Retraining SigLIP itself; scoring photos not already embedded (the old 141k dump only).
- New answers or changes to the answer list.
- Any page or database change.

## Anti-goals (failure even if it "works")

- **Inflated accuracy**: a model graded on answers it was trained on. Every figure, including the
  accuracy the Model check page computes for the new run, must come from photos the model did not
  train on.
- A model that **only** works with a description (useless for new photos) — the photo-only variant
  must stand on its own.
- Any photo or description leaving this machine (RAT has not agreed to external AI services).
- Writing to the live database from the training script.

## Constraints

- Inputs already on disk: `exports/ai_pilot/emb_siglip_so400m.npy` (float16, L2-normalised,
  1,152 per photo) + `.image_nos.txt` (row order); `exports/ai_pilot/subject_scores.csv` (zero-shot
  probabilities per answer). 141,111 photos.
- Labels and descriptions read **read-only** from `oci` (`set_session(readonly=True)`):
  latest answer per photo for the task (any mode) from `rat.model_answer`; `rat.catalog.description`.
- Output CSV must match what `scripts/load_model_run.py` reads: header
  `image_no,answer,p,p_<id>...` in the task file's answer order, one row per photo.
- Runs in the WSL `venv` on CPU. Adds scikit-learn (and joblib, which comes with it).
- Golden Rule 7 untouched: the script never writes; loading is the existing loader, dry run first.

## Approaches considered

1. **Small classifier on the saved embeddings (chosen by the user).** Logistic regression on the
   photo embedding; a word-based classifier on the description (words like "4-6-0", "Class 47",
   "tram", "viaduct" carry the signal, and it reads the whole text — SigLIP's text side only reads
   ~64 tokens); the zero-shot probabilities as an extra input; seconds to train, minutes to score.
2. **Zero-shot nudged by nearest checked photos** (a few-shot adapter). Keeps all 10 answers alive
   without a fallback rule, but weaker with descriptions and gains less as answers grow. Built only
   as a yardstick in the same honest test.
3. **Retraining SigLIP on the GPU.** Needs thousands of answers; would overfit 547; dropped.

## The design

### Script

`scripts/model_train.py` (dry by default: writes files under `exports/`, never the database).

```
venv/bin/python scripts/model_train.py --task scripts/model_tasks/rat_subject_type.json \
    --embeddings exports/ai_pilot/emb_siglip_so400m --zeroshot exports/ai_pilot/subject_scores.csv \
    --version 2026-11-18-trained --out-dir exports/ai_models --target-profile oci
```

Writes:
- `<out-dir>/<version>-photo.csv` and `<version>-photo-desc.csv` — loader-format scores for every
  embedded photo.
- `<out-dir>/<version>-report.md` — the honest test (below), human-readable.
- `<out-dir>/<version>-photo.joblib`, `-photo-desc.joblib` — the fitted models (for the later
  new-photo step).

### Training data

- One label per photo: its **latest** answer for the task, any mode (a corrected answer replaces
  the earlier one).
- Only photos that have an embedding (all answered photos should; report any that don't).
- **The blind sample** = photos whose answers have `mode = 'sample'` in `rat.model_answer`. This is
  exactly what the page grades: each run's prediction against the photo's latest *sample* answer.
  Answering a sample photo again (the conflict link) saves it blind again, so for these photos the
  latest answer and the latest sample answer agree; the report grades on the latest sample answer,
  matching the page and the 79%.
- **Unsure answers train but are never graded** (builder's call, 2026-11-17). They were picked
  because the zero-shot model was unsure, so they are a biased set — but they are real labels and
  are exactly the hard cases. All labelled photos take part in the folds; only blind-sample photos
  are scored in the report.
- The zero-shot scores were produced before any answer existed (fixed prompts, no tuning), so using
  them as an input is not leakage.
- Near-duplicate photos (series shots of one train) can sit in different folds and flatter the
  score slightly. Accepted for this step and noted in the report (builder's call); `image_no` is
  unique in `rat.catalog`, so exact duplicates are not an issue.

### Features

- **Photo only:** the 1,152-number embedding, plus the zero-shot log-probabilities (10 numbers).
- **Photo + description:** the above, plus word features of the description (TF-IDF of words and
  word pairs; numbers and wheel notations like `4-6-0` kept as tokens). Photos with an empty
  description use the photo-only model.

### Answers with too few examples

The classifier is trained only on answers with at least **5** labelled photos (today: steam, diesel,
station/bridge, other, wagons/coaches, tram, electric). For the rest (railcar, bus/lorry, ship), the
final probabilities keep the zero-shot value, and the classifier's probabilities are scaled to fill
the remainder: `p(unseen a) = z(a)`, `p(seen a) = (1 − Σ z(unseen)) · c(a)`. So a photo the
zero-shot run strongly calls a ship still comes out as ship. The threshold 5 is a setting, applied
to each training part separately (an answer can drop below 5 inside a fold). Every output row
carries `p_<id>` for all 10 answers, unseen ones included.

### Honest test (the part that matters)

- **5-fold stratified cross-validation** over all labelled photos: each photo's prediction comes
  from a model trained on the other four-fifths. Settings (regularisation strength, TF-IDF size) are
  chosen inside each training part only.
- **The output CSV uses these out-of-fold predictions for every labelled photo**; the final model
  (trained on all labels) scores only the unlabelled photos. So the accuracy Model check computes
  for the new run is honest, not graded on its own training answers.
- The report compares, on the **blind sample photos only**:
  always-steam · zero-shot · yardstick (approach 2) · photo only · photo + description —
  overall accuracy, loco yes/no accuracy, per-answer recall, and how often "sure" (p ≥ 0.9)
  suggestions are right and how many photos are sure. Per-answer figures mark the answers that
  fell back to zero-shot (too few examples), so they are not read as trained results.

### Acceptance

A variant is worth loading only if its blind-sample accuracy beats the zero-shot run's 79%. The
report says plainly which variants pass. With 411 blind answers the margin is about ±4%, so the
report also shows how many blind photos each variant gets right that zero-shot gets wrong, and vice
versa. The script always writes every file and exits 0; passing or failing is a line in the report,
and the user decides what to load (builder's call). A failing photo-only variant does not stop the
photo + description one, or the reverse. Bad inputs (missing files, header mismatch, no labels) exit
non-zero like the loader.

### Loading

The user loads each passing variant with the existing loader (dry run, then `--apply`), photo-only
first so photo + description is the newest run (the page's default):

```
--model "logreg(siglip-so400m)"          --version 2026-11-18-trained-photo
--model "logreg(siglip-so400m+tfidf)"    --version 2026-11-18-trained-photo-desc
```

### Tests

pytest on small synthetic data, no database: out-of-fold guarantee (a labelled photo's prediction
never comes from a model that saw its label); unseen-answer rule; empty description falls back to
photo only; output CSV is accepted by `load_model_run.read_scores`; label = latest answer.

## Left to the builder

Regularisation grid, TF-IDF settings, the yardstick's blend weight, how the label/description
query is written, file layout inside the script, and the report's exact wording.
