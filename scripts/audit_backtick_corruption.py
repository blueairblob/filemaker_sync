#!/usr/bin/env python3
"""
audit_backtick_corruption.py -- retrospective scan for the historical
backtick->doublequote DML corruption bug (fixed 2026-09-11 in
filemaker_extract.py's adjust_sql_syntax() call sites -- see its own
docstring and CLAUDE.md's "Loader status" section for the full story).

THE BUG, briefly: adjust_sql_syntax() blindly rewrote every backtick
character to a double-quote across an entire DML text blob, for this
pipeline's whole history before the fix -- meant to convert MySQL-style
backtick-quoted *identifiers*, but by the time it ran, df_to_sql_bulk_insert()
had already emitted correct Postgres double-quoted identifiers, so the only
backticks left for it to find were literal ones inside actual DATA values.
Confirmed live once already for image_no specifically (a backtick typo in a
real image_no landed as a doublequote, silently breaking that row's own
natural key). CLAUDE.md flags as still open: whether OTHER fields (not just
image_no) have the same latent corruption, sitting unnoticed because nothing
depends on those fields matching exactly the way image_no's Storage lookup
does. This script answers that, precisely -- not by guessing, but by testing
the bug's own exact signature.

METHOD: the bug is fixed now, so a FRESH FileMaker extract into
rat_migration.ratcatalogue correctly preserves every backtick. Diff each
text column of rat.catalog (image_no-joined) against that fresh staging
value. An ordinary diff would false-positive on every legitimate edit made
since a row was last loaded -- so this doesn't just flag "differs", it tests
the bug's precise mechanism: a row is CONFIRMED corrupted only when

    fresh_value.replace('`', '"') == currently_stored_value
    and '`' in fresh_value

i.e. replacing every backtick in the current FileMaker value with a
doublequote reproduces EXACTLY what's sitting in rat.catalog right now. That
can only happen via this specific bug -- an unrelated edit producing the
identical character-for-character substitution by coincidence is not a
realistic alternative explanation.

PREREQUISITE: rat_migration.ratcatalogue must hold a fresh extract (not
whatever's left over from whenever it was last run) --
    python.exe scripts/filemaker_extract.py --db-exp --ddl --dml --del-data --target-profile oci
This script itself is Postgres-only (no FileMaker ODBC) -- runs fine from
WSL or Windows either way.

USAGE:
    python scripts/audit_backtick_corruption.py --target-profile oci
    python scripts/audit_backtick_corruption.py --target-profile oci --no-log

Confirmed hits are logged into rat_migration.reject_log (severity='reject',
source_script='audit_backtick_corruption.py', stage='audit') so they show up
in reject_log.py's own --list/--export tooling alongside every other known
data-quality issue, not just this script's own console output. Pass --no-log
to just print the report without writing anything.

REMEDIATION is deliberately NOT done here -- this script only finds and
reports. The fresh, correct value is already sitting in
rat_migration.ratcatalogue by the time this runs; re-running Stage 2
(db_dml_loader.py --mode migration_schema) would upsert it into rat.catalog
for every row, fixing every confirmed hit as a side effect. That's a write
to the live rat.* schema and a separate, explicit decision -- not something
to do silently as part of a read-only audit.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reject_log
import db_sync_manifest as dsm

# staging column (rat_migration.ratcatalogue, FileMaker's own raw names) ->
# rat.catalog column. Excludes columns confirmed dead/never-populated
# (works_number/year_built/plant_code/bw_image_no all read 0 non-empty rows
# live, 2026-09-13 -- a separate, pre-existing gap, not this bug; comparing
# them here would be meaningless noise) and 'picture', which
# df_to_sql_bulk_insert() deliberately overwrites with image_no rather than
# carrying source data through, so it's not a real source field to audit.
COLUMN_MAP = {
    "image_no": "image_no",
    "category": "category",
    "circa": "circa",
    "imprecise_date": "imprecise_date",
    "description": "description",
    "condition": "condition",
    "owners_ref": "owners_ref",
    "cd_no": "cd_no",
    "cd_no_hr": "cd_no_hr",
    "bw_cd_no": "bw_cd_no",
    "gauge": "gauge",
    "active_area": "active_area",
    "corporate_body": "corporate_body",
    "facility": "facility",
    "parent_folder": "parent_folder",
    "imgref_stem": "imgref_stem",
    "website": "website",
}


def fetch_staging(conn) -> dict[str, dict]:
    """{image_no: {staging_col: value}} from the freshly-extracted
    rat_migration.ratcatalogue. Last-one-wins on a duplicate image_no
    (13 known, see CLAUDE.md's "Verified facts") -- not this script's
    concern to resolve, just needs to not crash on one."""
    cols = list(COLUMN_MAP.keys())
    quoted = ", ".join(f'"{c}"' for c in cols)
    out = {}
    with conn.cursor() as c:
        c.execute(f'SELECT {quoted} FROM rat_migration.ratcatalogue')
        for row in c.fetchall():
            rec = dict(zip(cols, row))
            image_no = rec.get("image_no")
            if image_no:
                out[image_no] = rec
    return out


def fetch_catalog(conn) -> dict[str, dict]:
    """{image_no: {catalog_col: value, 'id': uuid}} from the currently-live
    rat.catalog -- the thing being audited for latent corruption."""
    cols = ["id"] + list(dict.fromkeys(COLUMN_MAP.values()))
    quoted = ", ".join(f'"{c}"' for c in cols)
    out = {}
    with conn.cursor() as c:
        c.execute(f'SELECT {quoted} FROM rat.catalog')
        for row in c.fetchall():
            rec = dict(zip(cols, row))
            out[rec["image_no"]] = rec
    return out


def audit(staging: dict, catalog: dict) -> tuple[list[dict], int]:
    """Returns (confirmed_hits, other_diff_count). confirmed_hits is a list
    of {image_no, catalog_id, column, fresh_value, stored_value} -- each one
    passes the exact bug-signature test, not just "these differ".

    other_diff_count compares .strip()'d values -- db_dml_loader.py's own
    stripy()/clean_record_data() trims leading/trailing whitespace on load,
    so a raw comparison massively overcounts (sampled live, 2026-09-13:
    112,475 "diffs" against raw staging, the overwhelming majority just
    ' ' -> '' or a trailing space trimmed -- not edits, not corruption, an
    artifact of this script not replicating a step the loader already
    does). The backtick-signature test above is unaffected either way
    (whitespace noise can't produce that exact substitution pattern), this
    only cleans up the secondary "how much real drift is there" count."""
    hits = []
    other_diffs = 0
    for image_no, srow in staging.items():
        crow = catalog.get(image_no)
        if crow is None:
            continue  # never loaded / rejected elsewhere -- not this audit's concern
        for staging_col, catalog_col in COLUMN_MAP.items():
            if staging_col == "image_no":
                continue  # image_no's own corruption is the ALREADY-known case
            fresh = srow.get(staging_col)
            stored = crow.get(catalog_col)
            if fresh is None or stored is None:
                continue
            if fresh == stored:
                continue
            if "`" in fresh and fresh.replace("`", '"') == stored:
                hits.append({
                    "image_no": image_no,
                    "catalog_id": str(crow["id"]),
                    "column": catalog_col,
                    "fresh_value": fresh,
                    "stored_value": stored,
                })
            elif fresh.strip() != stored.strip():
                other_diffs += 1
    return hits, other_diffs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--target-profile", help="Target DB profile from config.toml's "
                     "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE)")
    ap.add_argument("--no-log", action="store_true", help="Print the report only -- don't write to reject_log")
    args = ap.parse_args()

    cfg = dsm.load_toml(args.config)
    conn = reject_log.open_connection(cfg, args.target_profile)

    print("Reading fresh staging (rat_migration.ratcatalogue)...")
    staging = fetch_staging(conn)
    print(f"  {len(staging)} keyed rows")

    print("Reading live rat.catalog...")
    catalog = fetch_catalog(conn)
    print(f"  {len(catalog)} rows")

    print("Comparing (exact backtick-substitution signature only)...")
    hits, other_diffs = audit(staging, catalog)

    print()
    print(f"CONFIRMED backtick corruption: {len(hits)} field(s) across "
          f"{len(set(h['image_no'] for h in hits))} row(s)")
    print(f"Other field differences (legitimate edits since last load, not this bug): {other_diffs}")
    print()

    if hits:
        by_col: dict[str, int] = {}
        for h in hits:
            by_col[h["column"]] = by_col.get(h["column"], 0) + 1
        print("By column:")
        for col, n in sorted(by_col.items(), key=lambda kv: -kv[1]):
            print(f"  {col}: {n}")
        print()
        print("First 10:")
        for h in hits[:10]:
            print(f"  {h['image_no']} / {h['column']}: "
                  f"{h['fresh_value']!r} -> stored as {h['stored_value']!r}")

    if hits and not args.no_log:
        run_id = reject_log.new_run_id()
        print(f"\nLogging {len(hits)} confirmed hit(s) to rat_migration.reject_log (run_id={run_id})...")
        for h in hits:
            reject_log.log_reject(
                cfg, args.target_profile, source_script="audit_backtick_corruption.py",
                stage="audit", severity="reject", image_no=h["image_no"],
                catalog_id=h["catalog_id"], table_name="catalog",
                reason=(f"Historical backtick->doublequote DML corruption confirmed in "
                        f"catalog.{h['column']}: current FileMaker value is "
                        f"{h['fresh_value']!r}, stored value is {h['stored_value']!r} "
                        f"(exact match for fresh_value.replace('`', '\"')). "
                        f"Fix: re-run Stage 2 (db_dml_loader.py --mode migration_schema) "
                        f"now that rat_migration.ratcatalogue holds the fresh value."),
                source_data=h, run_id=run_id, conn=conn,
            )
        print("Review with: python scripts/reject_log.py --list --target-profile "
              f"{args.target_profile or 'oci'}")

    conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        reject_log.log_crash("audit_backtick_corruption.py")
        raise
