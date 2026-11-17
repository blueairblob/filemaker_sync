"""Blind-sample metrics and Markdown report for trained subject-type models.

Pure module: no database, no file I/O. Probability columns follow the order of
the ``answers`` list passed in.
"""
import numpy as np


def blind_metrics(probs, image_nos, blind, answers, sure=0.9):
    ids = [a["id"] for a in answers]
    loco = {a["id"]: bool(a.get("loco")) for a in answers}
    probs = np.asarray(probs)
    recall = {i: [0, 0] for i in ids}
    n = correct = loco_correct = n_sure = sure_correct = 0
    correct_set = set()
    for row, no in zip(probs, image_nos):
        if no not in blind:
            continue
        truth = blind[no]
        pred = ids[int(np.argmax(row))]
        ok = pred == truth
        n += 1
        correct += ok
        if ok:
            correct_set.add(no)
        if loco.get(pred) == loco.get(truth):
            loco_correct += 1
        if truth in recall:
            recall[truth][1] += 1
            recall[truth][0] += ok
        if row.max() >= sure:
            n_sure += 1
            sure_correct += ok
    return {
        "n": n,
        "correct": correct,
        "accuracy": correct / n if n else 0.0,
        "loco_correct": loco_correct,
        "loco_accuracy": loco_correct / n if n else 0.0,
        "recall": {k: tuple(v) for k, v in recall.items()},
        "n_sure": n_sure,
        "sure_correct": sure_correct,
        "correct_set": correct_set,
    }


def _pct(x):
    return f"{100 * x:.1f}%"


def render_report(version, rows, zero_shot_key, unseen, notes):
    by_name = dict(rows)
    zs = by_name[zero_shot_key]
    graded = {"photo only", "photo + description"}
    out = [f"# Subject-type model report ({version})", ""]
    out += [f"Blind sample: {zs['n']} photos.", ""]
    out += ["| Model | Accuracy | Loco / not | Sure (>= 0.9) | Result |",
            "|---|---|---|---|---|"]
    for name, m in rows:
        if name in graded:
            result = "PASS" if m["accuracy"] > zs["accuracy"] else "FAIL"
        else:
            result = ""
        sure = f"{m['sure_correct']}/{m['n_sure']}"
        out.append(f"| {name} | {_pct(m['accuracy'])} | {_pct(m['loco_accuracy'])} "
                   f"| {sure} | {result} |")
    out += ["", f"A variant passes only if its accuracy beats {zero_shot_key}'s on the same photos.", ""]
    out += ["## Against zero-shot", ""]
    for name, m in rows:
        if name == zero_shot_key:
            continue
        right = len(m["correct_set"] - zs["correct_set"])
        wrong = len(zs["correct_set"] - m["correct_set"])
        out.append(f"- {name}: right where zero-shot was wrong: {right} · "
                   f"wrong where zero-shot was right: {wrong}")
    if "photo only" in by_name and "photo + description" in by_name:
        po, pd_ = by_name["photo only"], by_name["photo + description"]
        out.append(f"- photo + description vs photo only: right where photo only was wrong: "
                   f"{len(pd_['correct_set'] - po['correct_set'])} · "
                   f"wrong where photo only was right: {len(po['correct_set'] - pd_['correct_set'])}")
    out += ["", "## Recall per answer", ""]
    names = [n for n, _ in rows]
    out += ["| Answer | " + " | ".join(names) + " |",
            "|---|" + "---|" * len(names)]
    for aid in zs["recall"]:
        label = f"{aid} (zero-shot fallback)" if aid in unseen else aid
        cells = []
        for _, m in rows:
            r, t = m["recall"].get(aid, (0, 0))
            cells.append(f"{r}/{t}")
        out.append(f"| {label} | " + " | ".join(cells) + " |")
    if notes:
        out += ["", "## Notes", ""] + [f"- {s}" for s in notes]
    return "\n".join(out) + "\n"
