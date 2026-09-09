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
     Nothing to do? Log and exit 0. (--dry-run stops here.)
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
     is the verified set. Anything else was rejected/quarantined.
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
import json
import os
import subprocess
import sys
import tempfile

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


def fetch_verified(conn, image_nos: list) -> set:
    """Which of the attempted image_nos actually landed in rat.catalog --
    DB truth, not an assumption from the loader's return value."""
    with conn.cursor() as c:
        c.execute('SELECT image_no FROM rat.catalog WHERE image_no = ANY(%s)', (image_nos,))
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
        delta = result["new"] + result["changed"]
        report.kv("new:", len(result["new"]))
        report.kv("changed:", len(result["changed"]))
        report.kv("delta (new+changed):", len(delta))

        if not delta:
            report.line("Nothing to do.")
            summary = {
                "new": len(result["new"]), "changed": len(result["changed"]), "delta": 0,
                "verified": 0, "rejected": 0, "false_positives": 0, "real_changes": 0,
                "manifest_advanced": 0,
            }
            write_sync_status(pg.cnxn, summary)
            print(json.dumps(summary))
            return 0
        if args.dry_run:
            report.line(f"--dry-run: would extract/load {len(delta)} row(s), stopping here.")
            summary = {
                "new": len(result["new"]), "changed": len(result["changed"]), "delta": len(delta),
                "dry_run": True,
            }
            write_sync_status(pg.cnxn, summary)
            print(json.dumps(summary))
            return 0

        profile = dsm.resolve_active_profile(cfg, args.target_profile)

        # 2. Extract exactly the delta, then refresh the small reference tables in full.
        fd, delta_file = tempfile.mkstemp(suffix=".txt", text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write("\n".join(delta))
            run_subprocess([
                "filemaker_extract.py", "--db-exp", "--ddl", "--dml",
                "--tables-to-export", "ratcatalogue", "--del-data",
                "--image-nos-file", delta_file, "--target-profile", profile,
            ], "filemaker_extract.py (delta)")
        finally:
            os.unlink(delta_file)
        run_subprocess([
            "filemaker_extract.py", "--db-exp", "--ddl", "--dml",
            "--tables-to-export", "ratbuilders,ratroutes,ratcollections,prompts", "--del-data",
            "--target-profile", profile,
        ], "filemaker_extract.py (reference tables)")

        # 3. Load (unmodified entry point; reads whatever's now in staging).
        run_subprocess([
            "db_dml_loader.py", "--mode", "migration_schema",
            "--export-path", "unused", "--user-id", args.user_id, "--target-profile", profile,
        ], "db_dml_loader.py")

        # 4. Verify against rat.catalog directly, and 5. row-hash compare against
        # what's now in staging (exactly this run's freshly-extracted delta rows).
        mig_schema = cfg["database"]["target"]["schema"][cfg["database"]["target"]["mig_schema"]]
        verified = fetch_verified(pg.cnxn, delta)
        staging_rows = fetch_staging_rows(pg.cnxn, mig_schema, list(verified))

        rejected = set(delta) - verified
        report.kv("verified (committed to rat.catalog):", len(verified))
        if rejected:
            report.kv("rejected/quarantined (not advanced):", len(rejected))

        # 6. Advance the manifest for exactly the verified set.
        mark = {}
        false_positives = 0
        for img in verified:
            new_hash = dsm.row_hash(staging_rows.get(img, {}))
            old_hash = manifest.get(img, {}).get("row_hash")
            if old_hash is not None and old_hash == new_hash:
                false_positives += 1
            mark[img] = {
                "rowid": by_image[img]["rowid"],
                "rowmodid": by_image[img]["rowmodid"],
                "row_hash": new_hash,
            }
        pg.mark_loaded(mark)

        report.kv("false positives (ROWMODID moved, content didn't):", false_positives)
        report.kv("real changes:", len(verified) - false_positives)
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
                    ], "filemaker_extract.py (images, delta)")
                finally:
                    os.unlink(img_delta_file)

                export_path = cfg.get("export", {}).get("path", "export")
                webp_dir = os.path.join(export_path, "images", "webp")
                run_subprocess([
                    "upload_images_oci.py", "--webp-dir", webp_dir,
                    "--image-nos", ",".join(verified), "--force",
                ], "upload_images_oci.py (images, delta)")
                images_uploaded = len(verified)
                report.kv("images extracted + uploaded:", images_uploaded)
            except SystemExit as e:
                report.line(f"WARNING: image sync step failed, catalog sync itself is unaffected: {e}")
                print(f"(image sync skipped: {e})", file=sys.stderr)

        summary = {
            "new": len(result["new"]), "changed": len(result["changed"]), "delta": len(delta),
            "verified": len(verified), "rejected": len(rejected), "false_positives": false_positives,
            "real_changes": len(verified) - false_positives, "manifest_advanced": len(mark),
            "images_uploaded": images_uploaded,
        }
        write_sync_status(pg.cnxn, summary)
        print(json.dumps(summary))
    finally:
        pg.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
