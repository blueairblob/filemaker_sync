"""Load a scored model run (scripts/model_score.py output) into the model-check tables.

Dry run by default (read-only transaction, nothing written); --apply loads in
one transaction. Never touches rat.data_fix_* (Golden Rule 7).

    venv/bin/python scripts/load_model_run.py --task scripts/model_tasks/rat_subject_type.json \\
        --scores scores.csv --model clip-vit-l --version v1 --target-profile oci [--apply]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Iterator

VALID_KEYS = set("1234567890")


def task_row(task: dict) -> dict:
    return {
        "id": task["task"],
        "question": task["question"],
        "answers": [{k: v for k, v in a.items() if k != "prompts"} for a in task["answers"]],
        "sure_threshold": task.get("sure_threshold", 0.9),
        "links": task.get("links"),
        "sort_order": task.get("sort_order", 0),
        "client": task.get("client"),
        "note": task.get("note"),
    }


def task_file_errors(task: dict) -> list[str]:
    errors: list[str] = []
    answers = task.get("answers") or []
    if not 1 <= len(answers) <= 10:
        errors.append(f"need 1-10 answers, found {len(answers)}")
    keys = [a.get("key") for a in answers]
    ids = [a.get("id") for a in answers]
    for k in keys:
        if k not in VALID_KEYS:
            errors.append(f"answer key {k!r} is not one of '1'..'9','0'")
    if len(set(keys)) != len(keys):
        errors.append("answer keys are not unique")
    if len(set(ids)) != len(ids):
        errors.append("answer ids are not unique")
    return errors


def answer_list_errors(stored: list[dict], new: list[dict]) -> list[str]:
    new_keys = {a["id"]: a["key"] for a in new}
    errors = []
    for a in stored:
        if a["id"] not in new_keys:
            errors.append(f"answer {a['id']!r} was removed")
        elif new_keys[a["id"]] != a["key"]:
            errors.append(f"answer {a['id']!r} changed key {a['key']!r} -> {new_keys[a['id']]!r}")
    return errors


def read_scores(path: Path, answer_ids: list[str]) -> Iterator[tuple[str, str, float, list[float]]]:
    expected = ["image_no", "answer", "p"] + ["p_" + i for i in answer_ids]
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        if header != expected:
            raise ValueError(f"scores header mismatch: expected {expected}, found {header}")
        for row in reader:
            if row:
                yield row[0], row[1], float(row[2]), [float(x) for x in row[3:]]


def _pg_array(vals: list[float]) -> str:
    return "{" + ",".join(repr(v) for v in vals) + "}"


def _refusals(cur, row: dict, version: str) -> list[str]:
    cur.execute("select answers from rat.model_task where id = %s", (row["id"],))
    errors: list[str] = []
    stored = cur.fetchone()
    if stored:
        cur.execute("select exists (select 1 from rat.model_answer where task_id = %s)", (row["id"],))
        if cur.fetchone()[0]:
            errors += answer_list_errors(stored[0], row["answers"])
    cur.execute("select 1 from rat.model_run where task_id = %s and version = %s",
                (row["id"], version))
    if cur.fetchone():
        errors.append(f"run version {version!r} already exists for task {row['id']!r}")
    return errors


def run(conn, args: argparse.Namespace) -> int:
    task = json.loads(Path(args.task).read_text(encoding="utf-8"))
    errors = task_file_errors(task)
    if errors:
        print("task file invalid: " + "; ".join(errors), file=sys.stderr)
        return 2
    row = task_row(task)
    ids = [a["id"] for a in row["answers"]]
    try:
        scores = list(read_scores(Path(args.scores), ids))
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    conn.rollback()  # callers commit their own state first; start a clean transaction
    apply = bool(args.apply)
    try:
        with conn.cursor() as cur:
            if not apply:
                cur.execute("SET TRANSACTION READ ONLY")
            errors = _refusals(cur, row, args.version)
            if errors:
                print("refused: " + "; ".join(errors), file=sys.stderr)
                conn.rollback()
                return 1
            image_nos = [s[0] for s in scores]
            if apply:
                cur.execute(
                    """INSERT INTO rat.model_task
                         (id, question, answers, sure_threshold, links, sort_order, client, note)
                       VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s)
                       ON CONFLICT (id) DO UPDATE SET
                         question = EXCLUDED.question, answers = EXCLUDED.answers,
                         sure_threshold = EXCLUDED.sure_threshold, links = EXCLUDED.links,
                         sort_order = EXCLUDED.sort_order, client = EXCLUDED.client,
                         note = EXCLUDED.note""",
                    (row["id"], row["question"], json.dumps(row["answers"]),
                     row["sure_threshold"], json.dumps(row["links"]), row["sort_order"],
                     row["client"], row["note"]))
                cur.execute(
                    """INSERT INTO rat.model_run (task_id, model, version, notes, answers)
                       VALUES (%s, %s, %s, %s, %s::jsonb) RETURNING id""",
                    (row["id"], args.model, args.version, args.notes, json.dumps(row["answers"])))
                run_id = cur.fetchone()[0]
                cur.execute("CREATE TEMP TABLE _model_scores (image_no text, answer text,"
                            " p real, probs real[]) ON COMMIT DROP")
                buf = io.StringIO()
                w = csv.writer(buf)
                for image_no, answer, p, probs in scores:
                    w.writerow([image_no, answer, p, _pg_array(probs)])
                buf.seek(0)
                cur.copy_expert("COPY _model_scores (image_no, answer, p, probs)"
                                " FROM STDIN WITH (FORMAT csv)", buf)
                cur.execute(
                    """INSERT INTO rat.model_prediction (run_id, image_no, answer, p, probs)
                       SELECT %s, s.image_no, s.answer, s.p, s.probs
                       FROM _model_scores s
                       JOIN rat.catalog c ON c.image_no = s.image_no
                       ON CONFLICT (run_id, image_no) DO NOTHING""", (run_id,))
                loaded = cur.rowcount
                cur.execute("UPDATE rat.model_run SET n_predictions = %s WHERE id = %s",
                            (loaded, run_id))
            else:
                cur.execute("SELECT count(DISTINCT image_no) FROM rat.catalog"
                            " WHERE image_no = ANY(%s)", (image_nos,))
                loaded = cur.fetchone()[0]
        if apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    skipped = len(scores) - loaded
    verb = "loaded" if apply else "would load"
    print(f"task {row['id']} version {args.version}: read {len(scores)}, "
          f"{verb} {loaded}, {skipped} skipped (no catalog row)")
    if not apply:
        print("dry run: nothing written (use --apply)")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--task", required=True)
    p.add_argument("--scores", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--notes", default=None)
    p.add_argument("--config", default="config.toml")
    p.add_argument("--target-profile", default=None)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args(argv)
    import reject_log
    conn = reject_log.open_connection(reject_log.load_config(args.config), args.target_profile)
    try:
        return run(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
