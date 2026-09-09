#!/usr/bin/env python3
"""
run_incremental_sync.py — Increment 2: the actual periodic sync
=================================================================
Ties together the three existing tools instead of reinventing them:

  - db_sync_manifest.py's scan/diff engine, imported directly -- its
    scan/classify/PgManifest functions are already clean and
    side-effect-scoped, with no problematic module-global setup.
  - filemaker_extract.py and db_dml_loader.py, invoked as subprocesses --
    the same pattern gui/gui_operations.py already uses successfully to
    drive these two scripts. Both rely on globals().update(vars(args))
    and dozens of implicit module-level globals set up through their own
    __main__ blocks; importing and calling into them mid-flow would be
    fragile in a way subprocess isolation sidesteps entirely.

FLOW
  1. Skinny-scan FileMaker, diff against the manifest -> new + changed.
     ALSO check target completeness: any image_no the manifest believes is
     already loaded, but that's actually missing from rat.catalog right now
     (e.g. an accidental delete), gets added as "repaired" -- bypassing the
     ROWMODID check entirely, since direct proof of absence beats "hasn't
     changed since last load." Nothing to do (no new/changed/repaired)? Log
     and exit 0. (--dry-run stops here, but still reports what it found,
     repaired included.)
  2. Extract exactly those image_nos (filemaker_extract.py
     --image-nos-file) into rat_migration.ratcatalogue; refresh the small
     reference tables (ratbuilders/ratroutes/ratcollections/prompts) in
     full -- cheap, keeps them current, and they aren't image_no-scoped
     data to begin with.
  3. Load through the unmodified loader (db_dml_loader.py --mode
     migration_schema) -- it reads whatever's now in staging, which is
     just the delta.
  4. Verify against rat.catalog directly (DB truth, not an assumption
     from the loader's exit code) -- whatever image_no actually landed
     is the verified set. Anything else was rejected/quarantined --
     written out as a browsable .xlsx (write_quarantine_report(), every
     ratcatalogue column, not just image_no) so whoever has FileMaker
     access can find and correct the real record.
  5. For each verified row, compare its freshly-extracted content hash
     (from rat_migration.ratcatalogue, which now holds exactly this
     row's newly-extracted data) against the manifest's stored hash. A
     match means ROWMODID moved but the content didn't -- a FileMaker
     *import* touching the row, not a real edit. Still safe to have
     upserted (idempotent), but not counted as a real change.
  6. Advance the manifest (mark_loaded) for exactly the verified set,
     with the new rowmodid/row_hash. Anything NOT verified is left
     untouched -- it naturally reappears as new/changed next run. Never
     advance from the scan/diff step itself.
  7. Images, folded into the same run: extract + upload exactly the
     verified set's images (filemaker_extract.py --get-images
     --image-nos-file, upload_images_oci.py --image-nos --force). A new
     record needs both metadata and image; a changed record's image may
     or may not actually have changed alongside its metadata, and there's
     no cheap way to tell without re-fetching it -- so every verified row
     gets its image re-pulled and force-uploaded (x-upsert overwrites).
     Proportional to the sync's own delta, not a full-catalog rescan --
     see filemaker_extract.py's --get-images scoping. A separate, full-
     catalog "Upload Images" pass (no --image-nos-file) still exists
     for backfilling images that predate this step, or disaster recovery.

REQUIRES native Windows Python (python.exe) -- step 1's live FileMaker
scan and step 2's extract both need the FileMaker ODBC driver, same
constraint as db_sync_manifest.py/fm_metadata_probe.py.

USAGE
  python.exe scripts/run_incremental_sync.py --target-profile oci
  python.exe scripts/run_incremental_sync.py --target-profile oci --dry-run
  python.exe scripts/run_incremental_sync.py --target-profile oci --user-id sync-cron

Cron/Task Scheduler wiring is deliberately out of scope -- this makes the
sync itself correct and runnable on demand, not automatic.
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_sync_manifest as dsm

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_subprocess(script_args, label):
    """Run a sibling script the same way gui/gui_operations.py does -- a
    plain subprocess, not an import, since both target scripts rely on
    module-level globals set up through their own __main__ block."""
    cmd = [sys.executable, os.path.join(REPO_ROOT, "scripts", script_args[0])] + script_args[1:]
    print(f"--- {label} ---")
    print(" ".join(cmd))
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise SystemExit(f"{label} failed (exit {result.returncode})")


def fetch_staging_rows(conn, mig_schema: str, image_nos: list) -> dict:
    """Read rat_migration.ratcatalogue's just-extracted rows for these
    image_nos, keyed by image_no, for row_hash() comparison."""
    with conn.cursor() as c:
        c.execute(f'SELECT * FROM {mig_schema}.ratcatalogue WHERE image_no = ANY(%s)', (image_nos,))
        cols = [d[0] for d in c.description]
        return {row[cols.index('image_no')]: dict(zip(cols, row)) for row in c.fetchall()}


def _xlsx_safe(v):
    """openpyxl writes str/int/float/bool/None/date/datetime natively but
    chokes on other DB types (e.g. Decimal) -- coerce anything else to
    something it can hold rather than letting the whole report fail over
    one odd cell."""
    if v is None or isinstance(v, (str, int, float, bool, datetime.date, datetime.datetime)):
        return v
    if isinstance(v, Decimal):
        return float(v)
    return str(v)


def write_quarantine_report(staging_rows: dict, rejected: set, by_image: dict, cfg: dict) -> str | None:
    """A browsable .xlsx of every rejected/quarantined row's full
    freshly-extracted data (every ratcatalogue column, not just image_no) --
    so whoever has FileMaker access can actually find and correct the real
    record instead of squinting at a bare id in a log. .xlsx, not legacy
    .xls -- opens identically in Excel, and .xls's writer (xlwt) is
    unmaintained and caps out at 65,536 rows. Same generic "rejects_*.xlsx"
    name and image_no/id/reason lead-in columns as upload_images_oci.py's
    own rejects report -- one report shape a volunteer needs to learn,
    regardless of which pipeline stage caught the problem.

    `id` here is FileMaker's own ROWID (from by_image, the live skinny
    scan), not a rat.catalog UUID -- these rows never made it into
    rat.catalog, so no UUID exists yet.

    Best-effort: openpyxl not installed, or the write itself failing, never
    fails the sync over it -- the row stays correctly un-advanced in the
    manifest either way and will show up again next run."""
    if not rejected:
        return None
    try:
        from openpyxl import Workbook
    except ImportError:
        print("(rejects report skipped: openpyxl not installed -- pip install openpyxl)", file=sys.stderr)
        return None

    rows = [(img, staging_rows[img]) for img in sorted(rejected) if img in staging_rows]
    if not rows:
        return None
    try:
        extra_cols = [c for c in rows[0][1].keys() if c != 'image_no']
        wb = Workbook()
        ws = wb.active
        ws.title = "Rejected records"
        ws.append(["image_no", "id", "reason"] + extra_cols)
        for img, row in rows:
            reason = ("Rejected by the catalog loader (db_dml_loader.py) -- extracted from "
                      "FileMaker but did not verify in rat.catalog after Sync attempted to load it.")
            ws.append([img, _xlsx_safe(by_image.get(img, {}).get('rowid')), reason]
                      + [_xlsx_safe(row.get(c)) for c in extra_cols])

        export_path = cfg.get("export", {}).get("path", "export")
        out_dir = Path(export_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"rejects_{dsm.NOW.strftime('%Y%m%d_%H%M%S')}.xlsx"
        wb.save(out_path)
        return str(out_path)
    except Exception as e:
        print(f"(quarantine report write failed: {e})", file=sys.stderr)
        return None


def fetch_verified(conn, image_nos: list) -> set:
    """Which of the attempted image_nos actually landed in rat.catalog --
    DB truth, not an assumption from the loader's return value."""
    with conn.cursor() as c:
        c.execute('SELECT image_no FROM rat.catalog WHERE image_no = ANY(%s)', (image_nos,))
        return {row[0] for row in c.fetchall()}


def fetch_catalog_image_nos(conn) -> set:
    """Every image_no actually present in rat.catalog right now -- used to
    detect target-side data loss (e.g. an accidental delete) independent of
    whether FileMaker's own ROWMODID has moved. The skinny-scan diff alone
    is blind to this: it only compares FileMaker's current state against
    the manifest's last-loaded state, so a row the manifest still believes
    is loaded, but that's actually gone from rat.catalog, would otherwise
    never be flagged as new/changed and would never get re-synced (confirmed
    live, 2026-09-09: deleted a real catalog row, ran Check Sync, got
    "new: 0, changed: 0" -- the row stayed missing)."""
    with conn.cursor() as c:
        c.execute('SELECT image_no FROM rat.catalog WHERE image_no IS NOT NULL')
        return {row[0] for row in c.fetchall()}


def write_sync_status(conn, summary: dict) -> None:
    """Best-effort freshness record for picaloco-web's admin page --
    rat.sync_status must exist and grant anon SELECT (see devlog/worksheet.md
    for the DDL); if it doesn't yet, don't fail the sync over it."""
    try:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO rat.sync_status
                    (run_type, dry_run, new_count, changed_count, delta_count,
                     verified_count, rejected_count, real_changes, manifest_advanced,
                     ok, detail)
                VALUES ('incremental_sync', %s, %s, %s, %s, %s, %s, %s, %s, true, %s)
                """,
                (
                    bool(summary.get("dry_run", False)),
                    summary.get("new"), summary.get("changed"), summary.get("delta"),
                    summary.get("verified"), summary.get("rejected"),
                    summary.get("real_changes"), summary.get("manifest_advanced"),
                    json.dumps(summary),
                ),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"(sync-status write skipped: {e})", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description="Incremental sync: scan -> extract delta -> load -> verify -> advance manifest")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--dsn"); ap.add_argument("--user"); ap.add_argument("--pwd")
    ap.add_argument("--table", default="ratcatalogue")
    ap.add_argument("--key-col", default="image_no")
    ap.add_argument("--pg-host"); ap.add_argument("--pg-port")
    ap.add_argument("--pg-user"); ap.add_argument("--pg-pwd"); ap.add_argument("--pg-db")
    ap.add_argument("--target-profile", help="Target DB profile from config.toml's "
                    "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE)")
    ap.add_argument("--from-snapshot", help="scan a snapshot instead of live FileMaker (offline test)")
    ap.add_argument("--manifest-snapshot", help="use a snapshot instead of the live manifest (offline test)")
    ap.add_argument("--user-id", default="incremental-sync", help="audit user id passed to the loader")
    ap.add_argument("--dry-run", action="store_true", help="scan+diff only; don't extract, load, or advance")
    ap.add_argument("--debug", action="store_true", help="Pass --debug through to every filemaker_extract.py "
                     "subprocess call this makes -- raises its own log level AND un-suppresses its per-image "
                     "export progress bars (normally hidden since they flood a captured/streamed log).")
    args = ap.parse_args()

    cfg = dsm.load_toml(args.config) if os.path.exists(args.config) else {}
    report = dsm.Report()
    report.line("Incremental sync")
    report.line(f"Run at {dsm.NOW.isoformat()}")

    # 1. Scan + diff.
    records = dsm.get_current_scan(report, args, cfg)
    by_image, unkeyed, dups = dsm.build_scan(records)
    manifest, pg = dsm.get_manifest(report, args, cfg)
    if pg is None:
        raise SystemExit("ERROR: incremental sync needs the live manifest (no --manifest-snapshot support "
                          "here -- mark_loaded() writes to the live DB).")
    try:
        result = dsm.classify_sync(by_image, unkeyed, dups, manifest)

        # Target-completeness check: image_nos the manifest believes are
        # already loaded, still present in FileMaker (nothing to extract
        # otherwise), but missing from rat.catalog right now. Independent of
        # the skinny-scan diff above -- catches target-side data loss that
        # ROWMODID comparison alone is blind to. See fetch_catalog_image_nos()'s
        # own docstring for how this was confirmed live.
        already_covered = set(result["new"]) | set(result["changed"])
        catalog_image_nos = fetch_catalog_image_nos(pg.cnxn)
        repaired = sorted((set(manifest.keys()) & set(by_image.keys()))
                           - catalog_image_nos - already_covered)

        delta = result["new"] + result["changed"] + repaired
        report.kv("new:", len(result["new"]))
        report.kv("changed:", len(result["changed"]))
        report.kv("repaired (in manifest, missing from rat.catalog):", len(repaired))
        report.kv("delta (new+changed+repaired):", len(delta))

        if not delta:
            report.line("Nothing to do.")
            summary = {
                "new": len(result["new"]), "changed": len(result["changed"]), "repaired": 0, "delta": 0,
                "verified": 0, "rejected": 0, "false_positives": 0, "real_changes": 0,
                "manifest_advanced": 0,
            }
            write_sync_status(pg.cnxn, summary)
            print(json.dumps(summary))
            return 0
        if args.dry_run:
            report.line(f"--dry-run: would extract/load {len(delta)} row(s), stopping here.")
            summary = {
                "new": len(result["new"]), "changed": len(result["changed"]), "repaired": len(repaired),
                "delta": len(delta), "dry_run": True,
            }
            write_sync_status(pg.cnxn, summary)
            print(json.dumps(summary))
            return 0

        profile = dsm.resolve_active_profile(cfg, args.target_profile)
        debug_flag = ["--debug"] if args.debug else []

        # 2. Extract exactly the delta, then refresh the small reference tables in full.
        fd, delta_file = tempfile.mkstemp(suffix=".txt", text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write("\n".join(delta))
            run_subprocess([
                "filemaker_extract.py", "--db-exp", "--ddl", "--dml",
                "--tables-to-export", "ratcatalogue", "--del-data",
                "--image-nos-file", delta_file, "--target-profile", profile,
            ] + debug_flag, "filemaker_extract.py (delta)")
        finally:
            os.unlink(delta_file)
        run_subprocess([
            "filemaker_extract.py", "--db-exp", "--ddl", "--dml",
            "--tables-to-export", "ratbuilders,ratroutes,ratcollections,prompts", "--del-data",
            "--target-profile", profile,
        ] + debug_flag, "filemaker_extract.py (reference tables)")

        # 3. Load (unmodified entry point; reads whatever's now in staging).
        run_subprocess([
            "db_dml_loader.py", "--mode", "migration_schema",
            "--export-path", "unused", "--user-id", args.user_id, "--target-profile", profile,
        ], "db_dml_loader.py")

        # 4. Verify against rat.catalog directly, and 5. row-hash compare against
        # what's now in staging (exactly this run's freshly-extracted delta rows).
        mig_schema = cfg["database"]["target"]["schema"][cfg["database"]["target"]["mig_schema"]]
        verified = fetch_verified(pg.cnxn, delta)
        # Fetch staging data for the WHOLE delta, not just verified -- rejected
        # rows need their full freshly-extracted row too, for the quarantine
        # report below (row_hash() below only ever looks up verified rows,
        # unaffected by fetching the wider set).
        staging_rows = fetch_staging_rows(pg.cnxn, mig_schema, delta)

        rejected = set(delta) - verified
        report.kv("verified (committed to rat.catalog):", len(verified))
        quarantine_report_path = None
        if rejected:
            report.kv("rejected/quarantined (not advanced):", len(rejected))
            quarantine_report_path = write_quarantine_report(staging_rows, rejected, by_image, cfg)
            if quarantine_report_path:
                report.line(f"Quarantine report (for FileMaker-side correction): {quarantine_report_path}")

        # 6. Advance the manifest for exactly the verified set.
        repaired_set = set(repaired)
        mark = {}
        false_positives = 0
        repaired_verified = 0
        for img in verified:
            new_hash = dsm.row_hash(staging_rows.get(img, {}))
            old_hash = manifest.get(img, {}).get("row_hash")
            if img in repaired_set:
                # Content is expected to match (FileMaker never changed) --
                # the row was just missing from the target, not stale. Kept
                # out of false_positives so the two don't get conflated in
                # the summary: one means "harmless re-verify," the other
                # means "we just restored a deleted row."
                repaired_verified += 1
            elif old_hash is not None and old_hash == new_hash:
                false_positives += 1
            mark[img] = {
                "rowid": by_image[img]["rowid"],
                "rowmodid": by_image[img]["rowmodid"],
                "row_hash": new_hash,
            }
        pg.mark_loaded(mark)

        report.kv("repaired (restored to rat.catalog):", repaired_verified)
        report.kv("false positives (ROWMODID moved, content didn't):", false_positives)
        report.kv("real changes:", len(verified) - false_positives - repaired_verified)
        report.line("")
        report.line(f"Manifest advanced for {len(mark)} row(s).")

        # 7. Images for exactly the verified set -- see module docstring point 7.
        # Best-effort: catalog data is already committed and the manifest already
        # advanced above, so a flaky image upload shouldn't fail an otherwise-
        # successful sync. Logged, not silent -- worth noticing if it recurs.
        images_uploaded = 0
        if verified:
            try:
                fd2, img_delta_file = tempfile.mkstemp(suffix=".txt", text=True)
                try:
                    with os.fdopen(fd2, "w", encoding="utf-8") as f:
                        f.write("\n".join(verified))
                    run_subprocess([
                        "filemaker_extract.py", "--get-images",
                        "--image-nos-file", img_delta_file, "--target-profile", profile,
                    ] + debug_flag, "filemaker_extract.py (images, delta)")
                finally:
                    os.unlink(img_delta_file)

                export_path = cfg.get("export", {}).get("path", "export")
                webp_dir = os.path.join(export_path, "images", "webp")
                run_subprocess([
                    "upload_images_oci.py", "--webp-dir", webp_dir,
                    "--image-nos", ",".join(verified), "--force",
                ] + debug_flag, "upload_images_oci.py (images, delta)")
                images_uploaded = len(verified)
                report.kv("images extracted + uploaded:", images_uploaded)
            except SystemExit as e:
                report.line(f"WARNING: image sync step failed, catalog sync itself is unaffected: {e}")
                print(f"(image sync skipped: {e})", file=sys.stderr)

        summary = {
            "new": len(result["new"]), "changed": len(result["changed"]), "repaired": len(repaired),
            "delta": len(delta),
            "verified": len(verified), "rejected": len(rejected), "false_positives": false_positives,
            "repaired_verified": repaired_verified,
            "real_changes": len(verified) - false_positives - repaired_verified,
            "manifest_advanced": len(mark),
            "images_uploaded": images_uploaded,
            "quarantine_report": quarantine_report_path,
        }
        write_sync_status(pg.cnxn, summary)
        print(json.dumps(summary))
    finally:
        pg.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
