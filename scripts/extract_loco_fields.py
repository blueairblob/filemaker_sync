"""Extract loco numbers and names from rat.catalog descriptions.

--dry-run reads the catalog in a read-only session and writes a CSV report;
it never writes to the database. (--apply arrives in a later task.)

    venv/bin/python scripts/extract_loco_fields.py --dry-run --target-profile oci \
        --out loco_dryrun.csv --sample-high 50
"""
import argparse
import csv
import random
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from loco_extract import description_hash, extract_loco_names, extract_loco_numbers

FIX_IDS = ("loco_number", "loco_name")
VALUE_SEP = "; "


@dataclass(frozen=True)
class FixRow:
    image_no: str
    fix_id: str
    values: tuple[str, ...]
    confidence: str
    description_hash: str


def build_rows(records: Iterable[tuple[str, str | None]],
               place_names: frozenset[str]) -> list[FixRow]:
    """Two rows per record: fix_id 'loco_number' then 'loco_name'."""
    rows: list[FixRow] = []
    for image_no, description in records:
        h = description_hash(description)
        num = extract_loco_numbers(description)
        name = extract_loco_names(description, place_names)
        rows.append(FixRow(image_no, "loco_number", num.values, num.confidence, h))
        rows.append(FixRow(image_no, "loco_name", name.values, name.confidence, h))
    return rows


def load_place_names(conn) -> frozenset[str]:
    """lower(name) from rat.location, rat.route and rat.organisation."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT lower(name) FROM rat.location WHERE name IS NOT NULL "
            "UNION SELECT lower(name) FROM rat.route WHERE name IS NOT NULL "
            "UNION SELECT lower(name) FROM rat.organisation WHERE name IS NOT NULL")
        return frozenset(r[0].strip() for r in cur.fetchall() if r[0] and r[0].strip())


def summarise(rows: list[FixRow]) -> dict[str, dict[str, int]]:
    """{fix_id: {confidence: count}}."""
    out: dict[str, Counter] = {}
    for r in rows:
        out.setdefault(r.fix_id, Counter())[r.confidence] += 1
    return {fix: dict(c) for fix, c in out.items()}


def _read_image_nos(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def _fetch_records(conn, image_nos: list[str] | None) -> list[tuple[str, str | None]]:
    with conn.cursor() as cur:
        if image_nos is None:
            cur.execute("SELECT image_no, description FROM rat.catalog")
        else:
            cur.execute("SELECT image_no, description FROM rat.catalog "
                        "WHERE image_no = ANY(%s)", (image_nos,))
        return cur.fetchall()


def write_csv(path: Path, rows: list[FixRow], descriptions: dict[str, str | None]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_no", "fix_id", "confidence", "values", "description"])
        for r in rows:
            w.writerow([r.image_no, r.fix_id, r.confidence, VALUE_SEP.join(r.values),
                        descriptions.get(r.image_no) or ""])


def print_report(rows: list[FixRow], descriptions: dict[str, str | None],
                 sample_high: int, seed: int = 2026) -> None:
    summary = summarise(rows)
    print("Summary:")
    for fix in FIX_IDS:
        counts = summary.get(fix, {})
        total = sum(counts.values())
        parts = ", ".join(f"{c} {counts.get(c, 0):,}" for c in ("high", "med", "low", "none"))
        print(f"  {fix}: total {total:,} -- {parts}")
    if sample_high <= 0:
        return
    rng = random.Random(seed)
    for fix in FIX_IDS:
        high = [r for r in rows if r.fix_id == fix and r.confidence == "high"]
        picks = rng.sample(high, min(sample_high, len(high)))
        print(f"\n{len(picks)} random high {fix} rows (seed {seed}):")
        for i, r in enumerate(picks, 1):
            desc = " ".join((descriptions.get(r.image_no) or "").split())
            print(f"  {i:2d}. {r.image_no} | {VALUE_SEP.join(r.values)} | {desc}")


def dry_run(cfg: dict, target_profile: str | None, image_nos: list[str] | None,
            out_csv: Path | None, sample_high: int, seed: int) -> dict:
    import reject_log
    conn = reject_log.open_connection(cfg, target_profile)
    try:
        conn.set_session(readonly=True)
        place_names = load_place_names(conn)
        records = _fetch_records(conn, image_nos)
    finally:
        conn.close()
    descriptions = dict(records)
    rows = build_rows(records, place_names)
    if out_csv:
        write_csv(out_csv, rows, descriptions)
        print(f"Wrote {len(rows):,} rows to {out_csv}")
    print_report(rows, descriptions, sample_high, seed)
    return {"rows": len(rows), "summary": summarise(rows)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="read-only report, no DB writes")
    mode.add_argument("--apply", action="store_true", help="write rat.data_fix_value (later task)")
    p.add_argument("--config", default="config.toml")
    p.add_argument("--target-profile", default=None)
    p.add_argument("--image-nos-file", default=None, help="one image_no per line")
    p.add_argument("--out", type=Path, default=None, help="CSV report path")
    p.add_argument("--sample-high", type=int, default=0, help="print N random high rows per fix")
    p.add_argument("--seed", type=int, default=2026, help="sample seed (default 2026)")
    args = p.parse_args(argv)

    if args.apply:
        p.error("--apply is not implemented yet")

    import reject_log
    cfg = reject_log.load_config(args.config)
    image_nos = _read_image_nos(args.image_nos_file) if args.image_nos_file else None
    dry_run(cfg, args.target_profile, image_nos, args.out, args.sample_high, args.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
