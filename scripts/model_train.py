"""Train the subject-type classifier on volunteer answers and write scores plus an honest report.

Read-only against the database (SELECTs in a read-only session); everything else is local files.
Labelled photos get out-of-fold predictions only; see scripts/model_fit.py.

    venv/bin/python scripts/model_train.py --task scripts/model_tasks/rat_subject_type.json \\
        --embeddings exports/ai_pilot/emb_siglip_so400m --zeroshot exports/ai_pilot/subject_scores.csv \\
        --version v2 --target-profile oci

Then load either variant (dry run first; add --apply to write):

    venv/bin/python scripts/load_model_run.py --task scripts/model_tasks/rat_subject_type.json \\
        --scores exports/ai_models/v2-photo.csv --model siglip-trained-photo --version v2-photo --target-profile oci
    venv/bin/python scripts/load_model_run.py --task scripts/model_tasks/rat_subject_type.json \\
        --scores exports/ai_models/v2-photo-desc.csv --model siglip-trained-photo-desc --version v2-photo-desc --target-profile oci
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Callable

import numpy as np

import model_fit
import model_report
from load_model_run import read_scores, task_file_errors

NEAR_DUPLICATE_NOTE = (
    "Near-duplicate photos (the same subject photographed or scanned more than once) can land "
    "in different folds, so these accuracies may be somewhat optimistic.")


def _read_image_nos(path: Path) -> list[str]:
    # split on "\n" only: image_no values may contain other whitespace and are never trimmed
    names = path.read_text(encoding="utf-8").split("\n")
    if names and names[-1] == "":
        names.pop()
    return names


def load_inputs(emb_prefix: Path, zeroshot_csv: Path, answer_ids: list[str]):
    """(image_nos, img, z) for photos present in both files, in embedding-file order."""
    emb_prefix = Path(emb_prefix)
    emb = np.load(str(emb_prefix) + ".npy", mmap_mode="r")
    names = _read_image_nos(Path(str(emb_prefix) + ".image_nos.txt"))
    if len(names) != len(emb):
        raise ValueError(f"{len(names)} image_nos for {len(emb)} embedding rows")
    zs = {no: probs for no, _a, _p, probs in read_scores(Path(zeroshot_csv), answer_ids)}
    keep = [i for i, no in enumerate(names) if no in zs]
    image_nos = [names[i] for i in keep]
    img = np.asarray(emb[keep], dtype=np.float32)
    z = np.array([zs[no] for no in image_nos], dtype=np.float64).reshape(len(image_nos), len(answer_ids))
    return image_nos, img, z


def write_scores(path: Path, image_nos, probs, answer_ids) -> None:
    probs = np.asarray(probs)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_no", "answer", "p"] + ["p_" + i for i in answer_ids])
        for no, row in zip(image_nos, probs):
            k = int(np.argmax(row))
            w.writerow([no, answer_ids[k], f"{row[k]:.4f}"] + [f"{v:.4f}" for v in row])


def run(args, fetch_answers: Callable, fetch_descriptions: Callable) -> int:
    import joblib

    task = json.loads(Path(args.task).read_text(encoding="utf-8"))
    errors = task_file_errors(task)
    if errors:
        print("task file invalid: " + "; ".join(errors), file=sys.stderr)
        return 2
    answers = task["answers"]
    ids = [a["id"] for a in answers]
    try:
        image_nos, img, z = load_inputs(Path(args.embeddings), Path(args.zeroshot), ids)
    except (OSError, ValueError) as e:
        print(f"bad inputs: {e}", file=sys.stderr)
        return 2
    rows = fetch_answers(task["task"])
    if not rows:
        print(f"no answers found for task {task['task']!r}", file=sys.stderr)
        return 2
    labels, blind_all = model_fit.latest_labels(rows)

    index = {no: i for i, no in enumerate(image_nos)}
    notes = [NEAR_DUPLICATE_NOTE]
    skipped_nolabel, skipped_bad = [], []
    lab_idx, y = [], []
    for no, ans in labels.items():
        if no not in index:
            skipped_nolabel.append(no)
        elif ans not in ids:
            skipped_bad.append((no, ans))
        else:
            lab_idx.append(index[no])
            y.append(ans)
    if not y:
        print("no usable labels after matching answers to embedded photos", file=sys.stderr)
        return 2
    blind, dropped_blind = {}, []
    for no, ans in blind_all.items():
        if no in index and ans in ids:
            blind[no] = ans
        else:
            dropped_blind.append(no)
    if skipped_nolabel:
        notes.append(f"Skipped {len(skipped_nolabel)} answered photo(s) with no embedding or "
                     f"zero-shot row: " + ", ".join(repr(n) for n in sorted(skipped_nolabel)))
    if skipped_bad:
        notes.append(f"Skipped {len(skipped_bad)} photo(s) whose latest answer is not in the task: "
                     + ", ".join(f"{n!r} ({a!r})" for n, a in sorted(skipped_bad)))
    if dropped_blind:
        notes.append(f"Dropped {len(dropped_blind)} blind-sample photo(s) from grading (no "
                     f"embedding or answer not in the task): "
                     + ", ".join(repr(n) for n in sorted(dropped_blind)))

    lab_idx = np.array(lab_idx, dtype=int)
    desc = fetch_descriptions()
    texts = [desc.get(no) for no in image_nos]
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    version = args.version

    def predict(make):
        made = []

        def rec():
            m = make()
            made.append(m)
            return m
        probs = model_fit.predict_all(rec, img, z, texts, lab_idx, y,
                                      folds=args.folds, seed=args.seed)
        return probs, made[-1]

    steam = np.zeros((len(image_nos), len(ids)))
    if "steam_loco" in ids:
        steam[:, ids.index("steam_loco")] = 1.0
    yard, _ = predict(lambda: model_fit.Yardstick(ids))
    photo, m_photo = predict(lambda: model_fit.SubjectModel(
        ids, use_text=False, min_examples=args.min_examples, seed=args.seed))
    both, m_both = predict(lambda: model_fit.SubjectModel(
        ids, use_text=True, min_examples=args.min_examples, seed=args.seed))

    named = [("always steam", steam), ("zero-shot", z), ("yardstick", yard),
             ("photo only", photo), ("photo + description", both)]
    report_rows = [(n, model_report.blind_metrics(p, image_nos, blind, answers))
                   for n, p in named]
    unseen = set(ids) - set(m_both.seen_)
    unseen_photo = set(ids) - set(m_photo.seen_)
    if unseen_photo != unseen:
        notes.append("Zero-shot fallback answers differ by variant; photo only falls back on: "
                     + ", ".join(sorted(unseen_photo)))
    report = model_report.render_report(version, report_rows, "zero-shot", unseen | unseen_photo, notes)

    write_scores(out / f"{version}-photo.csv", image_nos, photo, ids)
    write_scores(out / f"{version}-photo-desc.csv", image_nos, both, ids)
    (out / f"{version}-report.md").write_text(report, encoding="utf-8")
    joblib.dump(m_photo, out / f"{version}-photo.joblib")
    joblib.dump(m_both, out / f"{version}-photo-desc.joblib")
    print(f"wrote {version} outputs to {out}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--task", required=True)
    p.add_argument("--embeddings", required=True, help="prefix: <prefix>.npy + <prefix>.image_nos.txt")
    p.add_argument("--zeroshot", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--out-dir", default="exports/ai_models")
    p.add_argument("--folds", type=int, default=5)
    p.add_argument("--min-examples", type=int, default=5)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--config", default="config.toml")
    p.add_argument("--target-profile", default=None)
    args = p.parse_args(argv)
    import reject_log
    conn = reject_log.open_connection(reject_log.load_config(args.config), args.target_profile)
    try:
        conn.set_session(readonly=True)

        def fetch_answers(task_id):
            cur = conn.cursor()
            try:
                cur.execute("select image_no, answer, mode, answered_at, id "
                            "from rat.model_answer where task_id = %s", (task_id,))
                return cur.fetchall()
            finally:
                cur.close()

        def fetch_descriptions():
            cur = conn.cursor()
            try:
                cur.execute("select image_no, description from rat.catalog")
                return {no: d for no, d in cur.fetchall()}
            finally:
                cur.close()

        return run(args, fetch_answers, fetch_descriptions)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
