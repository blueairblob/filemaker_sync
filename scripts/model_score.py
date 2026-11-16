"""Score every photo against a task's answer list with an open CLIP-family model, run locally.

Read-only: reads thumbnail files and writes a CSV (and optionally the image embeddings). Never
touches the database, and no photo leaves this machine (model weights are downloaded once).

The task file (e.g. scripts/model_tasks/rat_subject_type.json) holds the question and its answers;
each answer has one or more text prompts. A photo's score for an answer combines its prompts
(log-mean-exp of the prompt logits, so an answer with more prompts isn't favoured), then a softmax
over the answers gives one probability per answer.

--save-embeddings writes <prefix>.npy (float16, one L2-normalised row per scored photo) and
<prefix>.image_nos.txt (row order), so later models can be trained on them without re-running the
image encoder.

Run with the CUDA venv (Windows, native paths):
    ..\\clip_venv\\Scripts\\python.exe scripts\\model_score.py ^
        --task scripts/model_tasks/rat_subject_type.json ^
        --images C:/dev/RAT_Trains_Project_Exports/exports/images/webp_mobile ^
        --out exports/ai_pilot/subject_scores.csv --save-embeddings exports/ai_pilot/emb_siglip_so400m
"""
import argparse
import csv
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import open_clip
import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset

TEMPLATES = [
    "a photo of {}.",
    "a black and white photo of {}.",
    "an old photograph of {}.",
]


class Thumbs(Dataset):
    def __init__(self, paths, preprocess):
        self.paths = paths
        self.preprocess = preprocess

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, i):
        try:
            return self.preprocess(Image.open(self.paths[i]).convert("RGB")), i, True
        except Exception:
            return torch.zeros(3, 1, 1), i, False


def collate(batch):
    good = [b for b in batch if b[2]]
    imgs = torch.stack([b[0] for b in good]) if good else None
    return imgs, [b[1] for b in good], [b[1] for b in batch if not b[2]]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--task", required=True, help="task JSON (question + answers with prompts)")
    ap.add_argument("--images", required=True, help="folder of thumbnails (<image_no>.webp)")
    ap.add_argument("--out", required=True, help="CSV to write")
    ap.add_argument("--save-embeddings", help="path prefix for <prefix>.npy + <prefix>.image_nos.txt")
    ap.add_argument("--model", default="ViT-SO400M-14-SigLIP-384")
    ap.add_argument("--pretrained", default="webli")
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0, help="score only the first N files (0 = all)")
    ap.add_argument("--image-nos-file", help="score only these image_nos (one per line)")
    args = ap.parse_args()

    task = json.loads(Path(args.task).read_text(encoding="utf-8"))
    answers = task["answers"]

    paths = sorted(Path(args.images).glob("*.webp"))
    if args.image_nos_file:
        wanted = {l.strip() for l in Path(args.image_nos_file).read_text(encoding="utf-8").splitlines() if l.strip()}
        paths = [p for p in paths if p.stem in wanted]
    if args.limit:
        paths = paths[: args.limit]
    if not paths:
        sys.exit("no images found")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"{len(paths)} images, task {task['task']}, model {args.model}/{args.pretrained} on {device}", flush=True)
    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained, device=device)
    model.eval()
    tokenizer = open_clip.get_tokenizer(args.model)

    # One embedding per prompt (averaged over the templates); owner[j] = answer index of prompt j.
    prompt_emb, owner = [], []
    with torch.no_grad(), torch.autocast(device_type=device, enabled=device == "cuda"):
        for a_i, a in enumerate(answers):
            for prompt in a["prompts"]:
                t = model.encode_text(tokenizer([tpl.format(prompt) for tpl in TEMPLATES]).to(device))
                t = t / t.norm(dim=-1, keepdim=True)
                m = t.mean(dim=0)
                prompt_emb.append(m / m.norm())
                owner.append(a_i)
        text = torch.stack(prompt_emb).float()
        scale = model.logit_scale.exp().float()
    owner_t = torch.tensor(owner, device=device)
    n_prompts = torch.bincount(owner_t, minlength=len(answers)).float()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    loader = DataLoader(
        Thumbs(paths, preprocess), batch_size=args.batch_size, num_workers=args.workers, collate_fn=collate
    )
    embs, emb_ids = [], []
    t0, done, failed = time.time(), 0, []
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_no", "answer", "p"] + [f"p_{a['id']}" for a in answers])
        for imgs, idx, bad in loader:
            failed += [paths[i].name for i in bad]
            if imgs is None:
                continue
            with torch.no_grad(), torch.autocast(device_type=device, enabled=device == "cuda"):
                e = model.encode_image(imgs.to(device))
            e = e.float()
            e = e / e.norm(dim=-1, keepdim=True)
            logits = scale * e @ text.T  # (batch, prompts)
            # log-mean-exp of each answer's prompt logits
            per_answer = torch.full((logits.shape[0], len(answers)), -math.inf, device=device)
            for a_i in range(len(answers)):
                cols = logits[:, owner_t == a_i]
                per_answer[:, a_i] = torch.logsumexp(cols, dim=-1) - torch.log(n_prompts[a_i])
            probs = per_answer.softmax(dim=-1)
            top_p, top_i = probs.max(dim=-1)
            for row, i in enumerate(idx):
                w.writerow(
                    [paths[i].stem, answers[top_i[row]]["id"], f"{top_p[row]:.4f}"]
                    + [f"{v:.4f}" for v in probs[row].tolist()]
                )
            if args.save_embeddings:
                embs.append(e.half().cpu().numpy())
                emb_ids += [paths[i].stem for i in idx]
            done += len(idx)
            if done % (args.batch_size * 50) < args.batch_size:
                print(f"{done}/{len(paths)}  {done / (time.time() - t0):.0f} img/s", flush=True)

    if args.save_embeddings:
        prefix = Path(args.save_embeddings)
        prefix.parent.mkdir(parents=True, exist_ok=True)
        np.save(prefix.with_suffix(".npy"), np.concatenate(embs))
        prefix.with_suffix(".image_nos.txt").write_text("\n".join(emb_ids) + "\n", encoding="utf-8")
        print(f"embeddings: {len(emb_ids)} rows -> {prefix.with_suffix('.npy')}", flush=True)
    print(f"done: {done} scored, {len(failed)} unreadable, {time.time() - t0:.0f}s -> {out}", flush=True)
    for name in failed[:20]:
        print(f"  unreadable: {name}")


if __name__ == "__main__":
    main()
