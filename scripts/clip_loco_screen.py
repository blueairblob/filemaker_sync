"""Zero-shot "is there a locomotive in this photo?" screen, run locally with an open CLIP model.

Read-only: reads thumbnail files from disk and writes a CSV. Never touches the database, and no
photo leaves this machine (the model weights are downloaded once; nothing is uploaded).

Each photo is scored against a set of written labels. p_loco is the share of the probability that
falls on the "locomotive / train" labels (labels compete); sig_loco is SigLIP's own independent
yes/no probability for the best loco label (no competition, so wagons or a bridge in shot don't
dilute it). The rest is spread over "no loco" scenes (carriages only,
empty station, tram, bus, building, ...).

Run with a CUDA venv (Windows, native paths):
    clip_venv\\Scripts\\python.exe scripts\\clip_loco_screen.py ^
        --images C:/dev/RAT_Trains_Project_Exports/exports/images/webp_mobile ^
        --out exports/ai_pilot/clip_scores.csv
"""
import argparse
import csv
import sys
import time
from pathlib import Path

import open_clip
import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset

# (label, counts as a loco?) -- a loco includes self-propelled units (DMU/EMU, railcar).
LABELS = [
    ("a steam locomotive", True),
    ("a diesel locomotive", True),
    ("an electric locomotive", True),
    ("a diesel or electric multiple unit train", True),
    ("a small industrial locomotive", True),
    # No negations ("with no locomotive"): CLIP-style models ignore "no" and match the nouns.
    ("a line of parked railway carriages", False),
    ("parked goods wagons", False),
    ("an empty railway station platform", False),
    ("empty railway tracks", False),
    ("a signal box or railway building", False),
    ("an empty railway bridge or viaduct", False),
    ("a tram", True),  # RAT records tram fleet numbers, so a tram counts
    ("a bus or lorry", False),
    ("a ship or boat", False),
    ("a group of people", False),
    ("a building or street scene", False),
    ("a map, document or ticket", False),
]
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
        p = self.paths[i]
        try:
            img = self.preprocess(Image.open(p).convert("RGB"))
            ok = True
        except Exception:
            img = torch.zeros(3, 1, 1)
            ok = False
        return img, i, ok


def collate(batch):
    good = [b for b in batch if b[2]]
    bad = [b[1] for b in batch if not b[2]]
    imgs = torch.stack([b[0] for b in good]) if good else None
    return imgs, [b[1] for b in good], bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--images", required=True, help="folder of thumbnails (<image_no>.webp)")
    ap.add_argument("--out", required=True, help="CSV to write")
    ap.add_argument("--model", default="ViT-SO400M-14-SigLIP-384")
    ap.add_argument("--pretrained", default="webli")
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0, help="score only the first N files (0 = all)")
    ap.add_argument("--image-nos-file", help="score only these image_nos (one per line)")
    args = ap.parse_args()

    paths = sorted(Path(args.images).glob("*.webp"))
    if args.image_nos_file:
        wanted = {l.strip() for l in Path(args.image_nos_file).read_text(encoding="utf-8").splitlines() if l.strip()}
        paths = [p for p in paths if p.stem in wanted]
    if args.limit:
        paths = paths[: args.limit]
    if not paths:
        sys.exit("no images found")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"{len(paths)} images, model {args.model}/{args.pretrained} on {device}", flush=True)
    model, _, preprocess = open_clip.create_model_and_transforms(args.model, pretrained=args.pretrained, device=device)
    model.eval()
    tokenizer = open_clip.get_tokenizer(args.model)

    with torch.no_grad(), torch.autocast(device_type=device, enabled=device == "cuda"):
        label_emb = []
        for label, _ in LABELS:
            t = model.encode_text(tokenizer([tpl.format(label) for tpl in TEMPLATES]).to(device))
            t = t / t.norm(dim=-1, keepdim=True)
            m = t.mean(dim=0)
            label_emb.append(m / m.norm())
        text = torch.stack(label_emb)
        scale = model.logit_scale.exp()
        # SigLIP is trained with a per-pair sigmoid, so it gives each label its own yes/no probability.
        bias = model.logit_bias if getattr(model, "logit_bias", None) is not None else None
    loco_mask = torch.tensor([is_loco for _, is_loco in LABELS], device=device)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    loader = DataLoader(
        Thumbs(paths, preprocess), batch_size=args.batch_size, num_workers=args.workers, collate_fn=collate
    )
    short = [l.replace(" ", "_").replace(",", "") for l, _ in LABELS]
    t0, done, failed = time.time(), 0, []
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_no", "p_loco", "sig_loco", "top_label", "top_p"] + short)
        for imgs, idx, bad in loader:
            failed += [paths[i].name for i in bad]
            if imgs is None:
                continue
            with torch.no_grad(), torch.autocast(device_type=device, enabled=device == "cuda"):
                e = model.encode_image(imgs.to(device))
                e = e / e.norm(dim=-1, keepdim=True)
                logits = (scale * e @ text.T).float()
            probs = logits.softmax(dim=-1)
            p_loco = (probs * loco_mask).sum(dim=-1)
            # Independent "is there a loco?" score: best sigmoid over the loco labels (SigLIP only).
            if bias is not None:
                sig_loco = (logits + bias.float()).sigmoid()[:, loco_mask].max(dim=-1).values
            else:
                sig_loco = torch.full_like(p_loco, float("nan"))
            top_p, top_i = probs.max(dim=-1)
            for row, i in enumerate(idx):
                w.writerow(
                    [paths[i].stem, f"{p_loco[row]:.4f}", f"{sig_loco[row]:.4f}", LABELS[top_i[row]][0], f"{top_p[row]:.4f}"]
                    + [f"{v:.4f}" for v in probs[row].tolist()]
                )
            done += len(idx)
            if done % (args.batch_size * 50) < args.batch_size:
                rate = done / (time.time() - t0)
                print(f"{done}/{len(paths)}  {rate:.0f} img/s", flush=True)
    print(f"done: {done} scored, {len(failed)} unreadable, {time.time() - t0:.0f}s -> {out}", flush=True)
    for name in failed[:20]:
        print(f"  unreadable: {name}")


if __name__ == "__main__":
    main()
