#!/usr/bin/env python3
"""
audit_key_debris.py -- turns the long-standing "16 flagged source records"
(db_sync_manifest.py's UNKEYED/DUPLICATE buckets -- see CLAUDE.md's "Verified
facts": 13 duplicate image_no, 3 NULL image_no) into an actual hand-off
artifact for a RAT volunteer to fix in FileMaker, instead of just a report-only
console count nobody ever turned into something actionable.

THE GAP THIS CLOSES: db_sync_manifest.py --preview has reported these same 16
rows every run since Session 5 -- but only as bare image_no values or FileMaker
ROWIDs, with no description/date/collection to help a volunteer actually find
the record in the FileMaker UI, and no persistent record once the terminal
output scrolled away. This script re-scans the live source for exactly those
rows, pulls enough context to be useful (description, category, date_taken,
entry_date, collection, organisation, route, location), and logs each one into
rat_migration.reject_log (severity='reject', source_script=this file,
resolution left NULL == still open) so it shows up in reject_log.py's own
--list/--export tooling alongside every other known data-quality issue.

TWO DISTINCT ISSUES, logged with different reasons:
  - DUPLICATE image_no (13 values, 29 source rows): the SAME image_no was
    typed onto multiple genuinely-different photos (confirmed live 2026-09-15
    -- each duplicate group's rows have distinct descriptions/dates, these are
    not accidental double-scans of one photo). Fix: a volunteer needs to
    retype one of each pair/group to a unique image_no in FileMaker.
  - UNKEYED (3 ROWIDs): blank stub records (all fields NULL except an
    entry_date) with no image_no at all. Fix: either give them a real
    image_no, or delete them if they're abandoned drafts -- a volunteer/RAT
    admin call, not this pipeline's.

REMEDIATION is NOT done here -- FileMaker-side data entry is out of this
pipeline's control by design (Golden Rule: quarantine, never coerce). This
script only finds, contextualises, and persists a hand-off record.

USAGE (needs native Windows Python for the FileMaker ODBC half):
    python.exe scripts/audit_key_debris.py --target-profile oci
    python.exe scripts/audit_key_debris.py --target-profile oci --export rejects_key_debris.xlsx
    python.exe scripts/audit_key_debris.py --target-profile oci --no-log

Re-running is safe and idempotent in effect: each run inserts fresh rows (this
table is an append-only log, like the rest of reject_log.py), so re-run after
a volunteer fixes some of these and the now-corrected rows simply won't be
found by scan_debris() any more -- nothing to clean up on this end.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reject_log
import db_sync_manifest as dsm

CONTEXT_COLS = [
    "image_no", "accession_no", "category", "description", "date_taken",
    "entry_date", "collection", "organisation", "route", "location",
]


def scan_debris(dsn: str, user: str, pwd: str, table: str) -> dict:
    """Re-runs db_sync_manifest.py's own skinny scan to find the current
    UNKEYED/DUPLICATE rowids (same logic, not duplicated), then does one
    follow-up query per bucket to pull real context columns for just those
    rows -- the skinny scan itself only ever reads image_no/ROWID/ROWMODID,
    not enough to be useful to a human.

    Returns {"duplicate": {image_no: [row_dict, ...]}, "unkeyed": [row_dict, ...]}."""
    import pyodbc
    conn_str = f"DSN={dsn};UID={user};PWD={pwd};CHARSET='UTF-8';ansi=True"
    cnxn = pyodbc.connect(conn_str, timeout=60)
    try:
        cur = cnxn.cursor()
        cur.execute(f'SELECT "image_no", ROWID FROM {table}')
        by_key: dict = {}
        unkeyed_rowids: list[str] = []
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for key, rowid in batch:
                if key is None or str(key).strip() == "":
                    unkeyed_rowids.append(str(rowid))
                else:
                    by_key.setdefault(str(key), []).append(str(rowid))
        dup_keys = {k: v for k, v in by_key.items() if len(v) > 1}

        quoted = ", ".join(f'"{c}"' for c in CONTEXT_COLS)
        out: dict = {"duplicate": {}, "unkeyed": []}

        for key in dup_keys:
            cur.execute(f'SELECT ROWID, ROWMODID, {quoted} FROM {table} WHERE "image_no" = ?', key)
            rows = []
            for r in cur.fetchall():
                rec = dict(zip(["fm_rowid", "fm_rowmodid"] + CONTEXT_COLS, r))
                rows.append(rec)
            out["duplicate"][key] = rows

        for rowid in unkeyed_rowids:
            cur.execute(f'SELECT ROWID, ROWMODID, {quoted} FROM {table} WHERE ROWID = ?', rowid)
            r = cur.fetchone()
            if r:
                out["unkeyed"].append(dict(zip(["fm_rowid", "fm_rowmodid"] + CONTEXT_COLS, r)))
        return out
    finally:
        cnxn.close()


def _fmt_context(rec: dict) -> str:
    bits = []
    for col in ("description", "category", "date_taken", "entry_date", "collection",
                "organisation", "route", "location"):
        v = rec.get(col)
        if v not in (None, ""):
            bits.append(f"{col}={v!r}")
    return "; ".join(bits) if bits else "(all context fields blank)"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--target-profile", help="Target DB profile from config.toml's "
                     "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE)")
    ap.add_argument("--dsn"); ap.add_argument("--user"); ap.add_argument("--pwd")
    ap.add_argument("--table", default="ratcatalogue")
    ap.add_argument("--no-log", action="store_true", help="Print the report only -- don't write to reject_log")
    ap.add_argument("--export", metavar="OUTFILE.xlsx",
                     help="Also write a standalone .xlsx report (reject_log.py's own shape) for a "
                          "volunteer without DB access.")
    args = ap.parse_args()

    cfg = dsm.load_toml(args.config)
    src = cfg["database"]["source"]
    from env_secrets import resolve_secret
    dsn = args.dsn or src.get("dsn")
    user = args.user or src.get("user")
    pwd = resolve_secret("RAT_SOURCE_PWD", src.get("pwd", ""), args.pwd)

    print(f"Scanning live FileMaker source ({args.table}) for key debris...")
    debris = scan_debris(dsn, user, pwd, args.table)
    n_dup_groups = len(debris["duplicate"])
    n_dup_rows = sum(len(v) for v in debris["duplicate"].values())
    n_unkeyed = len(debris["unkeyed"])
    print(f"  DUPLICATE image_no: {n_dup_groups} value(s) across {n_dup_rows} row(s)")
    print(f"  UNKEYED (NULL image_no): {n_unkeyed} row(s)")
    print()

    log_rows = []
    for image_no, rows in debris["duplicate"].items():
        for rec in rows:
            reason = (f"Duplicate image_no {image_no!r} ({len(rows)} FileMaker rows share this exact "
                      f"key, distinct photos -- fm_rowid={rec['fm_rowid']}). Fix: retype this row's "
                      f"image_no to a unique value in FileMaker. Context: {_fmt_context(rec)}")
            print(f"  [dup] {image_no}  fm_rowid={rec['fm_rowid']}  {_fmt_context(rec)}")
            log_rows.append({
                "severity": "reject", "image_no": image_no, "fm_rowid": rec["fm_rowid"],
                "table_name": args.table, "reason": reason, "source_data": rec,
            })

    for rec in debris["unkeyed"]:
        reason = (f"NULL image_no (fm_rowid={rec['fm_rowid']}, entry_date={rec.get('entry_date')!r}) -- "
                  f"blank stub record, no natural key to load against. Fix: give it a real image_no in "
                  f"FileMaker, or delete it if it's an abandoned draft (RAT/volunteer call). "
                  f"Context: {_fmt_context(rec)}")
        print(f"  [unkeyed] fm_rowid={rec['fm_rowid']}  {_fmt_context(rec)}")
        log_rows.append({
            "severity": "reject", "image_no": None, "fm_rowid": rec["fm_rowid"],
            "table_name": args.table, "reason": reason, "source_data": rec,
        })

    print()

    if not args.no_log and log_rows:
        conn = reject_log.open_connection(cfg, args.target_profile)
        run_id = reject_log.new_run_id()
        print(f"Logging {len(log_rows)} row(s) to rat_migration.reject_log (run_id={run_id})...")
        for lr in log_rows:
            reject_log.log_reject(
                cfg, args.target_profile, source_script="audit_key_debris.py", stage="audit",
                severity=lr["severity"], image_no=lr["image_no"], fm_rowid=lr["fm_rowid"],
                table_name=lr["table_name"], reason=lr["reason"], source_data=lr["source_data"],
                run_id=run_id, conn=conn,
            )
        print("Review with: python scripts/reject_log.py --list --target-profile "
              f"{args.target_profile or 'oci'}")

        if args.export:
            print(f"Exporting to {args.export}...")
            rows = reject_log._query(cfg, args.target_profile, unresolved_only=False, since=None,
                                      image_no=None, source_script="audit_key_debris.py", limit=1000)
            reject_log._export_xlsx(rows, Path(args.export))
            print(f"  wrote {len(rows)} row(s)")

        conn.close()
    elif args.export:
        print("--export needs logging to have run first (drop --no-log to use --export).")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        reject_log.log_crash("audit_key_debris.py")
        raise
