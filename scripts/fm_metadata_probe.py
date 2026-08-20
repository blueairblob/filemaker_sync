#!/usr/bin/env python3
"""
fm_metadata_probe.py — FileMaker Pro ODBC metadata & change-signal probe
========================================================================
A READ-ONLY diagnostic for the RAT / PicaLoco migration. It answers, against
the LIVE FileMaker file, the questions the incremental-sync design depends on:

  1. What system-catalog metadata is reachable over ODBC?
       FileMaker_Tables, FileMaker_BaseTableFields / FileMaker_Fields,
       FileMaker_ValueLists  (your controlled vocabularies)
  2. Does the target table expose ROWID / ROWMODID as usable change signals?
       Checked BOTH ways: a direct SELECT, and the SQLSpecialColumns catalog
       function (SQL_BEST_ROWID / SQL_ROWVER) via pyodbc.
  3. Are there any *designed* audit fields (ModificationTimestamp, CreatedBy…)?
  4. How does ROWMODID actually BEHAVE when a record is edited or imported?
       Two-run snapshot / compare.

SAFETY
  Every statement issued is a SELECT or an ODBC catalog call. The probe never
  writes to FileMaker. It only writes local report / snapshot files.

USAGE
  # Full probe (reads config.toml [database.source] from the current dir):
  python fm_metadata_probe.py

  # Override connection / target:
  python fm_metadata_probe.py --dsn rat --user train --pwd SECRET \
      --table ratcatalogue --key-col image_no

  # ROWMODID behaviour test (the important one):
  python fm_metadata_probe.py                          # run 1 -> writes a snapshot
  #   ... now edit ONE record in FileMaker (or run an import) ...
  python fm_metadata_probe.py --compare LATEST          # run 2 -> shows the delta

  # Machine-readable report alongside the console output:
  python fm_metadata_probe.py --json report.json

  # Verify the diff logic offline, with no database:
  python fm_metadata_probe.py --selftest

Requires: pyodbc, and tomllib (Python 3.11+) or tomli — both already available
in this project's environment.
"""

from __future__ import annotations
import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

# --- config loader: prefer stdlib tomllib (3.11+), fall back to tomli ---------
try:
    import tomllib as _toml            # Python 3.11+
except ModuleNotFoundError:            # pragma: no cover
    import tomli as _toml              # project dependency

NOW = datetime.now(timezone.utc)
STAMP = NOW.strftime("%Y%m%d_%H%M%S")


# =============================================================================
# Small output helper: prints to console AND accumulates a text transcript.
# =============================================================================
class Report:
    def __init__(self):
        self.lines: list[str] = []
        self.data: dict = {"probe": "fm_metadata_probe", "run_at": NOW.isoformat()}

    def line(self, s: str = ""):
        print(s)
        self.lines.append(s)

    def section(self, title: str):
        bar = "=" * 74
        self.line("")
        self.line(bar)
        self.line(title)
        self.line(bar)

    def kv(self, k: str, v):
        self.line(f"  {k:<34} {v}")


def safe(report: Report, label: str, fn):
    """Run a probe step; capture any failure into the report instead of dying.
    Discovery is the whole point, so one dead query must not stop the rest."""
    try:
        return fn()
    except Exception as e:                       # noqa: BLE001 (want everything)
        report.line(f"  [!] {label}: not available — {type(e).__name__}: {e}")
        report.data.setdefault("errors", {})[label] = f"{type(e).__name__}: {e}"
        return None


# =============================================================================
# Pure change-classification logic (shared by --compare and --selftest).
# records map: { rowid(str): {"image_no": str|None, "rowmodid": int|None} }
# =============================================================================
def classify(prev: dict, curr: dict) -> dict:
    prev_ids, curr_ids = set(prev), set(curr)
    new = sorted(curr_ids - prev_ids)
    missing = sorted(prev_ids - curr_ids)          # candidate deletes
    edited, unchanged = [], []
    for rid in curr_ids & prev_ids:
        if curr[rid].get("rowmodid") != prev[rid].get("rowmodid"):
            edited.append(rid)
        else:
            unchanged.append(rid)
    return {
        "new": new,
        "edited": sorted(edited),
        "unchanged": sorted(unchanged),
        "missing": missing,
    }


# =============================================================================
# Snapshot I/O
# =============================================================================
def snapshot_path(table: str) -> str:
    return f"fm_probe_snapshot_{table}_{STAMP}.json"


def write_snapshot(path: str, meta: dict, records: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "records": records}, f)
    return path


def resolve_snapshot(arg: str, table: str) -> str:
    """Accept an explicit path, a glob, or the literal 'LATEST'."""
    if arg == "LATEST":
        arg = f"fm_probe_snapshot_{table}_*.json"
    matches = sorted(glob.glob(arg))
    if not matches:
        raise FileNotFoundError(f"no snapshot matched: {arg}")
    return matches[-1]                              # newest by name (timestamped)


def load_snapshot(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Connection
# =============================================================================
def load_source_config(cfg_path: str) -> dict:
    with open(cfg_path, "rb") as f:
        cfg = _toml.load(f)
    return cfg.get("database", {}).get("source", {})


def build_conn_str(dsn: str, user: str, pwd: str) -> str:
    # Mirrors the project's existing connection string (FileMaker specifics).
    return f"DSN={dsn};UID={user};PWD={pwd};CHARSET='UTF-8';ansi=True"


# =============================================================================
# Query helper
# =============================================================================
def run_query(cur, sql: str, limit: int | None = None):
    cur.execute(sql)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchmany(limit) if limit else cur.fetchall()
    return cols, rows


# =============================================================================
# Probe sections
# =============================================================================
def probe_connection(report: Report, cnxn, pyodbc):
    report.section("[1] CONNECTION & DRIVER")
    report.line("  (all statements below are SELECT or ODBC catalog calls — read only)")
    info = {}
    for label, key in [
        ("DBMS name", "SQL_DBMS_NAME"),
        ("DBMS version", "SQL_DBMS_VER"),
        ("Driver name", "SQL_DRIVER_NAME"),
        ("Driver version", "SQL_DRIVER_VER"),
        ("ODBC version", "SQL_DRIVER_ODBC_VER"),
    ]:
        val = safe(report, label, lambda k=key: cnxn.getinfo(getattr(pyodbc, k)))
        if val is not None:
            report.kv(label + ":", val)
            info[label] = str(val)
    report.data["driver"] = info


def probe_catalog(report: Report, cur):
    report.section("[2] SYSTEM CATALOG (schema metadata over ODBC)")
    cat = {}

    def dump_tables():
        cols, rows = run_query(cur, "SELECT * FROM FileMaker_Tables")
        report.kv("FileMaker_Tables:", f"{len(rows)} table occurrence(s)")
        report.line(f"      columns: {', '.join(cols)}")
        for r in rows[:12]:
            report.line(f"        - {tuple(r)}")
        if len(rows) > 12:
            report.line(f"        … +{len(rows) - 12} more")
        cat["FileMaker_Tables"] = {"count": len(rows), "columns": cols}
    safe(report, "FileMaker_Tables", dump_tables)

    # Prefer the faster/newer BaseTableFields (FM 19.4.1+); fall back to Fields.
    def dump_fields():
        try:
            cols, rows = run_query(cur, "SELECT * FROM FileMaker_BaseTableFields")
            src = "FileMaker_BaseTableFields"
        except Exception:
            cols, rows = run_query(cur, "SELECT * FROM FileMaker_Fields")
            src = "FileMaker_Fields (BaseTableFields unavailable — older FileMaker)"
        report.kv(f"{src}:", f"{len(rows)} field row(s)")
        report.line(f"      columns: {', '.join(cols)}")
        cat["field_catalog"] = {"source": src, "count": len(rows), "columns": cols}
        cat["_field_rows"] = [dict(zip(cols, tuple(r))) for r in rows]
    safe(report, "field catalog", dump_fields)

    def dump_valuelists():
        cols, rows = run_query(cur, "SELECT * FROM FileMaker_ValueLists")
        report.kv("FileMaker_ValueLists:", f"{len(rows)} value list(s) (controlled vocab)")
        names = [str(tuple(r)[0]) for r in rows[:20]]
        if names:
            report.line("      e.g. " + ", ".join(names))
        cat["FileMaker_ValueLists"] = {"count": len(rows), "columns": cols,
                                       "sample": names}
    safe(report, "FileMaker_ValueLists", dump_valuelists)

    report.data["catalog"] = cat
    return cat


AUDIT_HINTS = ("modif", "modstamp", "mod_ts", "modified", "creat", "created",
               "timestamp", "time_stamp", "updated", "amended", "lastchange",
               "last_change", "createdby", "modifiedby", "created_by",
               "modified_by")


def probe_target_table(report: Report, cur, catalog: dict, table: str):
    report.section(f"[3] TARGET TABLE: {table}")
    result = {"table": table}

    # Column inventory via the generic SQLColumns catalog call (version-agnostic).
    def cols_via_catalog():
        cur.columns(table=table)
        rows = cur.fetchall()
        names = [(r.column_name, r.type_name) for r in rows]
        report.kv("columns (SQLColumns):", str(len(names)))
        for n, t in names:
            report.line(f"        - {n:<40} {t}")
        result["columns"] = [{"name": n, "type": t} for n, t in names]
        return [n for n, _ in names]
    colnames = safe(report, "column inventory", cols_via_catalog) or []

    # Audit-field candidates (designed CreationTimestamp/ModifiedBy-style fields).
    if colnames:
        hits = [c for c in colnames
                if any(h in c.lower().replace(" ", "") for h in AUDIT_HINTS)]
        result["audit_field_candidates"] = hits
        report.line("")
        if hits:
            report.kv("audit-field candidates:", ", ".join(hits))
            report.line("      -> if a modification timestamp is here you get the")
            report.line("         WHEN of a change for free, on top of ROWMODID.")
        else:
            report.kv("audit-field candidates:", "none found")
            report.line("      -> no designed modification timestamp; rely on ROWMODID.")

    # ModCount for this base table (schema-drift baseline) from the field catalog.
    field_rows = catalog.get("_field_rows") or []
    modcounts = {r.get("ModCount") for r in field_rows
                 if str(r.get("BaseTableName", "")).lower() == table.lower()
                 and "ModCount" in r}
    modcounts.discard(None)
    if modcounts:
        report.kv("schema ModCount (drift baseline):", sorted(modcounts))
        report.line("      -> store this; if it changes between runs, the source")
        report.line("         schema was altered — warn before migrating.")
        result["schema_modcount"] = sorted(str(m) for m in modcounts)

    report.data["target_table"] = result
    return colnames


def probe_special_columns(report: Report, cur, table: str):
    report.section("[4] CHANGE SIGNALS via SQLSpecialColumns (authoritative)")
    out = {}

    def rowid():
        cur.rowIdColumns(table)                      # SQL_BEST_ROWID
        rows = cur.fetchall()
        vals = [tuple(r) for r in rows]
        report.kv("rowIdColumns (SQL_BEST_ROWID):",
                  ", ".join(r.column_name for r in rows) or "none reported")
        for v in vals:
            report.line(f"        {v}")
        out["rowIdColumns"] = [r.column_name for r in rows]
    safe(report, "rowIdColumns", rowid)

    def rowver():
        cur.rowVerColumns(table)                     # SQL_ROWVER
        rows = cur.fetchall()
        report.kv("rowVerColumns (SQL_ROWVER):",
                  ", ".join(r.column_name for r in rows) or "NONE reported")
        for r in rows:
            report.line(f"        {tuple(r)}")
        out["rowVerColumns"] = [r.column_name for r in rows]
        if not rows:
            report.line("      -> driver did not advertise a row-version column here.")
            report.line("         ROWMODID may still work via SELECT (checked next).")
    safe(report, "rowVerColumns", rowver)

    report.data["special_columns"] = out


def probe_skinny_scan(report: Report, cur, table: str, key_col: str,
                      do_snapshot: bool):
    report.section(f"[5] SKINNY SCAN — SELECT {key_col}, ROWID, ROWMODID FROM {table}")
    records: dict = {}
    stats = {"rows": 0, "rowmodid_null": 0, "rowmodid_zero": 0,
             "rowmodid_1_5": 0, "rowmodid_gt5": 0, "rowmodid_max": None,
             "distinct_rowid": 0, "key_null": 0}

    # Try with the key column; if that name is wrong, fall back to ROWID/ROWMODID only.
    sql_with_key = f'SELECT "{key_col}", ROWID, ROWMODID FROM {table}'
    sql_no_key = f"SELECT ROWID, ROWMODID FROM {table}"

    def scan(sql, has_key):
        cur.execute(sql)
        seen_rowid = set()
        max_mod = None
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for r in batch:
                t = tuple(r)
                if has_key:
                    key, rowid, rowmod = t[0], t[1], t[2]
                else:
                    key, rowid, rowmod = None, t[0], t[1]
                stats["rows"] += 1
                if key is None:
                    stats["key_null"] += 1
                rid = str(rowid)
                seen_rowid.add(rid)
                if rowmod is None:
                    stats["rowmodid_null"] += 1
                else:
                    rm = int(rowmod)
                    if rm == 0:
                        stats["rowmodid_zero"] += 1
                    elif rm <= 5:
                        stats["rowmodid_1_5"] += 1
                    else:
                        stats["rowmodid_gt5"] += 1
                    max_mod = rm if max_mod is None else max(max_mod, rm)
                records[rid] = {"image_no": (str(key) if key is not None else None),
                                "rowmodid": (int(rowmod) if rowmod is not None else None)}
        stats["rowmodid_max"] = max_mod
        stats["distinct_rowid"] = len(seen_rowid)

    used_key = True
    try:
        scan(sql_with_key, True)
    except Exception as e:
        report.line(f"  [!] key column '{key_col}' unusable ({type(e).__name__}: {e})")
        report.line(f"      falling back to: {sql_no_key}")
        used_key = False
        records.clear()
        for k in list(stats):
            stats[k] = 0 if isinstance(stats[k], int) else None
        scan(sql_no_key, False)

    report.kv("rows scanned:", stats["rows"])
    report.kv("distinct ROWID:", stats["distinct_rowid"])
    if used_key:
        report.kv(f"NULL {key_col}:", stats["key_null"])
    report.line("")
    if stats["rows"] and stats["rowmodid_null"] == stats["rows"]:
        report.kv("ROWMODID:", "NULL for ALL rows  ✗  (not usable via SELECT here)")
    elif stats["rowmodid_null"]:
        report.kv("ROWMODID:", f"present, but NULL on {stats['rowmodid_null']} row(s)")
    else:
        report.kv("ROWMODID:", "present on every row  ✓")
    report.line(f"      = 0 (never edited since creation): {stats['rowmodid_zero']}")
    report.line(f"      1–5 commits:                       {stats['rowmodid_1_5']}")
    report.line(f"      > 5 commits:                       {stats['rowmodid_gt5']}")
    report.line(f"      max commit count seen:             {stats['rowmodid_max']}")

    report.data["skinny_scan"] = {"used_key": used_key, "stats": stats}

    snap_file = None
    if do_snapshot:
        meta = {"table": table, "key_col": key_col if used_key else None,
                "captured_at": NOW.isoformat(), "rows": stats["rows"]}
        snap_file = write_snapshot(snapshot_path(table), meta, records)
        report.line("")
        report.kv("snapshot written:", snap_file)
        report.line("      -> edit ONE record in FileMaker (or run an import), then:")
        report.line(f"         python {os.path.basename(sys.argv[0])} --compare LATEST")
        report.data["skinny_scan"]["snapshot"] = snap_file

    return records, stats


def do_compare(report: Report, table: str, curr_records: dict, snap_arg: str):
    report.section("[6] COMPARE — how ROWMODID moved since the snapshot")
    path = resolve_snapshot(snap_arg, table)
    snap = load_snapshot(path)
    prev = snap.get("records", {})
    meta = snap.get("meta", {})
    report.kv("snapshot:", path)
    report.kv("captured at:", meta.get("captured_at"))
    report.kv("snapshot rows:", meta.get("rows"))
    report.kv("current rows:", len(curr_records))

    result = classify(prev, curr_records)
    report.line("")
    report.kv("NEW (unseen ROWID):", len(result["new"]))
    report.kv("EDITED (ROWMODID moved):", len(result["edited"]))
    report.kv("UNCHANGED:", len(result["unchanged"]))
    report.kv("MISSING (candidate delete):", len(result["missing"]))

    def show(label, ids, n=15):
        if not ids:
            return
        report.line(f"\n  {label} (first {min(n, len(ids))}):")
        for rid in ids[:n]:
            p = prev.get(rid, {})
            c = curr_records.get(rid, {})
            img = (c or p).get("image_no")
            report.line(f"        ROWID {rid}  image_no={img}  "
                        f"rowmodid {p.get('rowmodid')} -> {c.get('rowmodid')}")
    show("EDITED", result["edited"])
    show("NEW", result["new"])
    show("MISSING", result["missing"])

    report.line("")
    report.line("  INTERPRETATION")
    e = len(result["edited"])
    if e == 0:
        report.line("    • Nothing moved. If you DID edit a record, ROWMODID is not")
        report.line("      tracking edits over ODBC here — fall back to row-hash diffing.")
    elif e <= 3:
        report.line("    • A small, specific EDITED set — ROWMODID tracks real edits. ✓")
        report.line("      This is the green light for the two-pass incremental design.")
    else:
        report.line(f"    • {e} rows moved. If you only edited one but imported a batch,")
        report.line("      this is the documented 'import bumps modification' effect —")
        report.line("      keep the row-hash as a second check to filter false positives.")
    report.data["compare"] = {
        "snapshot": path,
        "new": len(result["new"]), "edited": len(result["edited"]),
        "unchanged": len(result["unchanged"]), "missing": len(result["missing"]),
        "edited_sample": result["edited"][:50],
    }


def print_verdict(report: Report):
    report.section("VERDICT")
    ss = report.data.get("skinny_scan", {}).get("stats", {})
    special = report.data.get("special_columns", {})
    rows = ss.get("rows", 0)
    null_all = rows and ss.get("rowmodid_null", 0) == rows
    advertised = bool(special.get("rowVerColumns"))

    if rows and not null_all:
        report.line("  ROWMODID is readable per row via SELECT.  ✓")
    elif null_all:
        report.line("  ROWMODID came back NULL for every row via SELECT.  ✗")
    else:
        report.line("  Skinny scan did not complete — see errors above.")
    report.line(f"  Driver advertises a row-version column (SQL_ROWVER): "
                f"{'yes' if advertised else 'no'}")
    report.line("")
    if rows and not null_all:
        report.line("  Next: run the two-run test to confirm it MOVES on a real edit —")
        report.line("    1) (this run already wrote a snapshot)")
        report.line("    2) edit one record through the volunteers' forms app")
        report.line("    3) re-run with  --compare LATEST")
        report.line("  If EDITED == 1, the two-pass incremental design is cleared to build.")
    else:
        report.line("  ROWMODID looks unusable here; the fallback is full-scan row-hash")
        report.line("  diffing. Re-run with a corrected --key-col if the scan failed on the key.")


# =============================================================================
# Self-test (no database) — proves the classify() logic end to end.
# =============================================================================
def selftest() -> int:
    print("SELF-TEST: change classification (no database)")
    prev = {
        "1": {"image_no": "arc00001", "rowmodid": 3},
        "2": {"image_no": "arc00002", "rowmodid": 0},
        "3": {"image_no": "arc00003", "rowmodid": 7},
        "4": {"image_no": "arc00004", "rowmodid": 1},   # will go missing
    }
    curr = {
        "1": {"image_no": "arc00001", "rowmodid": 3},   # unchanged
        "2": {"image_no": "arc00002", "rowmodid": 1},   # edited (0 -> 1)
        "3": {"image_no": "arc00003", "rowmodid": 7},   # unchanged
        "5": {"image_no": "arc00005", "rowmodid": 0},   # new
    }
    r = classify(prev, curr)
    ok = (r["new"] == ["5"] and r["edited"] == ["2"]
          and r["unchanged"] == ["1", "3"] and r["missing"] == ["4"])
    print(f"  new={r['new']} edited={r['edited']} "
          f"unchanged={r['unchanged']} missing={r['missing']}")
    print("  RESULT:", "PASS ✓" if ok else "FAIL ✗")
    return 0 if ok else 1


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only FileMaker ODBC metadata probe")
    ap.add_argument("--config", default="config.toml", help="path to config.toml")
    ap.add_argument("--dsn"); ap.add_argument("--user"); ap.add_argument("--pwd")
    ap.add_argument("--table", default="ratcatalogue", help="target table to probe")
    ap.add_argument("--key-col", default="image_no", help="natural key column")
    ap.add_argument("--compare", metavar="SNAPSHOT",
                    help="compare current scan to a prior snapshot "
                         "(path, glob, or the word LATEST)")
    ap.add_argument("--json", metavar="FILE", help="also write a JSON report")
    ap.add_argument("--selftest", action="store_true",
                    help="verify the diff logic offline, no DB")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    try:
        import pyodbc
    except ModuleNotFoundError:
        print("ERROR: pyodbc is not installed (pip install pyodbc).", file=sys.stderr)
        return 2

    # Resolve connection details: CLI overrides config.toml [database.source].
    # A BLANK password is valid — FileMaker accounts can have none — so we
    # require only DSN + user and treat an unset password as empty.
    src = load_source_config(args.config) if os.path.exists(args.config) else {}
    dsn = args.dsn or src.get("dsn")
    user = args.user or src.get("user")
    pwd = args.pwd if args.pwd is not None else src.get("pwd", "")
    if pwd is None:
        pwd = ""
    if not dsn or not user:
        print("ERROR: could not resolve DSN or user "
              "(note: a blank password is allowed).", file=sys.stderr)
        return 2

    report = Report()
    report.line("FileMaker Metadata Probe — RAT / PicaLoco")
    report.line(f"Run at {NOW.isoformat()}")
    report.line(f"Connecting: DSN={dsn}  UID={user}  (read-only)")

    try:
        cnxn = pyodbc.connect(build_conn_str(dsn, user, pwd), timeout=30)
    except Exception as e:                           # noqa: BLE001
        report.line(f"\nCONNECTION FAILED — {type(e).__name__}: {e}")
        report.line("Check the DSN exists (System DSN), FileMaker ODBC sharing is on,")
        report.line("and the file is open. Nothing was changed.")
        if args.json:
            report.data["connection_error"] = str(e)
            with open(args.json, "w", encoding="utf-8") as f:
                json.dump(report.data, f, indent=2, default=str)
        return 1

    try:
        cur = cnxn.cursor()
        probe_connection(report, cnxn, pyodbc)
        catalog = probe_catalog(report, cur) or {}
        probe_target_table(report, cur, catalog, args.table)
        probe_special_columns(report, cur, args.table)
        # Only write a fresh snapshot on a plain run, not during a compare.
        records, _ = probe_skinny_scan(report, cur, args.table, args.key_col,
                                       do_snapshot=(args.compare is None))
        if args.compare:
            do_compare(report, args.table, records, args.compare)
        print_verdict(report)
    finally:
        cnxn.close()

    # Always drop a text transcript; JSON on request.
    txt = f"fm_probe_report_{STAMP}.txt"
    with open(txt, "w", encoding="utf-8") as f:
        f.write("\n".join(report.lines))
    report.line("")
    report.line(f"(text report saved: {txt})")
    if args.json:
        # Strip the bulky internal field-row cache before serialising.
        report.data.get("catalog", {}).pop("_field_rows", None)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report.data, f, indent=2, default=str)
        report.line(f"(json report saved: {args.json})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
