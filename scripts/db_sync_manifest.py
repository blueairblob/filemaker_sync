#!/usr/bin/env python3
"""
db_sync_manifest.py — incremental-sync change-detection engine (Increment 1)
============================================================================
The cheap two-pass "what changed in FileMaker?" core for the RAT / PicaLoco
migration. It does NOT load anything yet (that is Increment 2 — wiring into
db_dml_loader.py). This module only *detects* the delta, so it can be proven
in isolation before anything touches the target.

DESIGN (validated by fm_metadata_probe.py against the live file)
  Pass 1 — skinny scan:  SELECT image_no, ROWID, ROWMODID FROM ratcatalogue
           ~141k tiny rows, seconds over ODBC.
  Diff   — compare each source row's ROWMODID against the manifest (what we
           last loaded), keyed on image_no (the durable archive id), sorting
           rows into:  new / changed / unchanged / candidate-delete
           plus two data-quality buckets for this volunteer-fed source:
             unkeyed      — NULL/blank image_no (can't be keyed; quarantine)
             duplicate_key — same image_no on >1 source row (quarantine)
  Pass 2 (Increment 2) — full extract of the new+changed set only.

INVARIANTS (agreed)
  • The manifest's loaded ROWMODID advances ONLY on a successful load.
  • Candidate-deletes are FLAGGED for review, NEVER auto-applied.
  • ROWMODID is the cheap primary signal; a row-hash backstop (Increment 2)
    filters the false positives that bulk *imports* introduce.

The manifest lives in Supabase/Postgres (rat_migration.sync_manifest) so it
survives across machines and is queryable.

MODES
  # Create the manifest table (idempotent DDL):
  python db_sync_manifest.py --init

  # Seed the manifest from the CURRENT source, asserting the target already
  # holds these rows (use once, after the initial migration):
  python db_sync_manifest.py --baseline --yes

  # DRY-RUN preview — scan source, diff against manifest, print counts, write
  # NOTHING. This is the GUI "Stage" path:
  python db_sync_manifest.py --preview

  # Fully offline: diff two probe/scan snapshots (no database at all).
  # The snapshots fm_metadata_probe.py writes work directly here:
  python db_sync_manifest.py --diff-snapshots prev.json curr.json

  # Preview against files instead of live systems (any combination):
  python db_sync_manifest.py --preview --from-snapshot curr.json \
                             --manifest-snapshot prev.json

  # Just capture a source snapshot to JSON:
  python db_sync_manifest.py --snapshot

  # Prove the diff logic offline, no DB:
  python db_sync_manifest.py --selftest

  # Print the DDL without touching anything:
  python db_sync_manifest.py --print-ddl

Connections come from config.toml ([database.source] over ODBC for the scan,
[database.target.supabase] over psycopg2 for the manifest), with CLI overrides.
Requires: pyodbc (scan), psycopg2 (manifest); tomllib/tomli for config.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

try:
    import tomllib as _toml            # Python 3.11+
except ModuleNotFoundError:            # pragma: no cover
    import tomli as _toml

# Secrets (passwords) resolve through the shared env_secrets mechanism (env / .env
# first, config.toml as fallback), so config.toml can be secret-free. The inline
# fallback keeps this working even if env_secrets.py isn't beside this file.
try:
    from env_secrets import resolve_secret, resolve_target_pwd
except ImportError:                       # self-contained fallback (identical behaviour)
    import os as _os
    def resolve_secret(env_key, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        try:
            from dotenv import load_dotenv; load_dotenv()
        except ModuleNotFoundError:
            pass
        _v = _os.environ.get(env_key)
        return _v if _v else (cfg_val if cfg_val is not None else default)
    def resolve_target_pwd(profile, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        pwd = resolve_secret(f"RAT_TARGET_PWD_{profile.upper()}", cfg_val=None)
        if pwd:
            return pwd
        if profile == "supabase":
            pwd = resolve_secret("RAT_TARGET_PWD", cfg_val=None)
            if pwd:
                return pwd
        return cfg_val if cfg_val is not None else default

NOW = datetime.now(timezone.utc)
STAMP = NOW.strftime("%Y%m%d_%H%M%S")
SAMPLE = 15                            # how many ids to show per bucket


# =============================================================================
# The manifest table DDL (idempotent, guarded).
# =============================================================================
SYNC_MANIFEST_DDL = """\
CREATE SCHEMA IF NOT EXISTS rat_migration;

CREATE TABLE IF NOT EXISTS rat_migration.sync_manifest (
    image_no       varchar PRIMARY KEY,          -- the durable archive id / natural key
    fm_rowid       varchar,                       -- FileMaker internal record id (ROWID)
    fm_rowmodid    bigint,                         -- ROWMODID we LAST LOADED (advances on load only)
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz,                    -- last scan in which this key was present
    last_loaded_at timestamptz,                    -- set ONLY on a successful load
    load_status    text NOT NULL DEFAULT 'pending', -- pending | loaded | rejected
    row_hash       text                            -- hash of the last-loaded row's full content, for the
                                                     -- import-inflation backstop (ROWMODID moved but the
                                                     -- data didn't) -- see row_hash() / mark_loaded()
);

-- Guarded ALTER for a manifest table that already existed before row_hash was added (CREATE TABLE IF NOT
-- EXISTS above is a no-op against it, so the column needs its own idempotent statement).
ALTER TABLE rat_migration.sync_manifest ADD COLUMN IF NOT EXISTS row_hash text;

COMMENT ON TABLE  rat_migration.sync_manifest IS
    'Per-record sync state for the FileMaker->rat migration. Keyed on image_no. '
    'fm_rowmodid is the modification counter as of the last successful load.';
"""


# =============================================================================
# Output helper (console + transcript + json payload)
# =============================================================================
class Report:
    def __init__(self):
        self.lines: list[str] = []
        self.data: dict = {"tool": "db_sync_manifest", "run_at": NOW.isoformat()}

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
        self.line(f"  {k:<32} {v}")


# =============================================================================
# Config
# =============================================================================
def load_toml(path: str) -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def source_conf(cfg: dict) -> dict:
    return cfg.get("database", {}).get("source", {})


def resolve_active_profile(cfg: dict, cli_val: str | None = None) -> str:
    """Which [database.target.<profile>] is active. Precedence: CLI > env
    RAT_TARGET_PROFILE > config.toml active_profile (or legacy 'db' key) > 'supabase'."""
    tgt = cfg.get("database", {}).get("target", {})
    return resolve_secret(
        "RAT_TARGET_PROFILE",
        cfg_val=tgt.get("active_profile") or tgt.get("db"),
        cli_val=cli_val,
        default="supabase",
    )


def target_conf(cfg: dict, profile: str | None = None) -> dict:
    # Connection details are split: shared keys live in [database.target] while
    # host/user/pwd/port live in [database.target.<profile>]. Merge them, with the
    # active profile's sub-table overriding the parent for any overlapping key.
    tgt = dict(cfg.get("database", {}).get("target", {}))
    active = resolve_active_profile(cfg, profile)
    sub = tgt.get(active, {})
    if not isinstance(sub, dict):
        sub = {}
    merged = {**tgt, **sub}
    merged["_active_profile"] = active
    return merged


# =============================================================================
# Small utilities
# =============================================================================
def as_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(str(v).strip())
        except (TypeError, ValueError):
            return None


def norm_key(v):
    """Canonicalise an image_no: strip, and treat blank as unkeyed (None)."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def row_hash(record: dict) -> str:
    """Deterministic content hash of a source row, for the import-inflation
    backstop: FileMaker bumps ROWMODID on *import* even when a row's actual
    data didn't change, so ROWMODID alone can't tell a real edit from that.
    Comparing this hash against the manifest's stored value (only meaningful
    once the row's full data has actually been extracted -- the cheap skinny
    scan has no content to hash) catches the false positive."""
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# =============================================================================
# Snapshot I/O — SAME format as fm_metadata_probe.py:
#   {"meta": {...}, "records": { rowid_str: {"image_no": .., "rowmodid": ..} }}
# =============================================================================
def write_snapshot(path: str, meta: dict, records: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "records": records}, f)
    return path


def load_snapshot(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def records_from_snapshot(snap: dict) -> dict:
    return snap.get("records", {})


# =============================================================================
# Turn a raw {rowid: {image_no, rowmodid}} record set into a keyed scan,
# surfacing the two data-quality buckets as we go.
# =============================================================================
def build_scan(records: dict):
    by_image: dict = {}
    unkeyed: list = []
    dup_counts: dict = {}
    for rid, rec in records.items():
        img = norm_key(rec.get("image_no"))
        rowmod = rec.get("rowmodid")
        if img is None:
            unkeyed.append({"rowid": rid, "rowmodid": rowmod})
            continue
        if img in by_image:
            dup_counts[img] = dup_counts.get(img, 1) + 1
            # keep the highest ROWMODID among duplicates so any edit still trips
            if (as_int(rowmod) or -1) > (as_int(by_image[img]["rowmodid"]) or -1):
                by_image[img] = {"rowid": rid, "rowmodid": rowmod}
        else:
            by_image[img] = {"rowid": rid, "rowmodid": rowmod}
    return by_image, unkeyed, dup_counts


def manifest_from_records(records: dict) -> dict:
    """Build a manifest-shaped map {image_no: {rowmodid}} from a snapshot's
    records — used to stand in for the DB manifest in fully-offline diffs."""
    m: dict = {}
    for rec in records.values():
        img = norm_key(rec.get("image_no"))
        if img is None:
            continue
        rm = as_int(rec.get("rowmodid"))
        # last write wins; fine for an offline baseline stand-in
        m[img] = {"rowmodid": rm}
    return m


# =============================================================================
# The diff engine (pure) — shared by --preview, --diff-snapshots, --selftest.
# =============================================================================
def classify_sync(by_image: dict, unkeyed: list, dup_counts: dict,
                  manifest: dict) -> dict:
    # Duplicated image_no rows can't be safely keyed, so they are pulled out of
    # the clean classification and reported only as a quarantine bucket. This keeps
    # (new + changed) == "rows we will actually attempt to load".
    dup_keys = set(dup_counts)
    present = set(by_image)                 # all keys present in source (incl dups)
    clean = present - dup_keys              # keys safe to classify
    man = set(manifest)
    new = sorted(clean - man)
    candidate_delete = sorted(man - present)   # in manifest, absent from source entirely
    changed, unchanged = [], []
    for img in clean & man:
        c = as_int(by_image[img]["rowmodid"])
        m = as_int(manifest[img].get("rowmodid"))
        if c is None or m is None or c != m:
            changed.append(img)
        else:
            unchanged.append(img)
    changed.sort(); unchanged.sort()
    return {
        "counts": {
            "source_rows": sum(dup_counts.get(k, 1) for k in by_image) + len(unkeyed),
            "keyed": len(by_image),
            "manifest": len(manifest),
            "new": len(new),
            "changed": len(changed),
            "unchanged": len(unchanged),
            "candidate_delete": len(candidate_delete),
            "unkeyed": len(unkeyed),
            "duplicate_key": len(dup_counts),
        },
        "new": new,
        "changed": changed,
        "candidate_delete": candidate_delete,
        "duplicate_key": sorted(dup_counts),
        "unkeyed": [u["rowid"] for u in unkeyed],
        "_by_image": by_image,
        "_manifest": manifest,
    }


def report_diff(report: Report, result: dict):
    c = result["counts"]
    report.section("SYNC PREVIEW — delta vs manifest (dry run, nothing written)")
    report.kv("source rows scanned:", c["source_rows"])
    report.kv("  keyed (usable):", c["keyed"])
    report.kv("manifest rows:", c["manifest"])
    report.line("")
    report.kv("NEW (load):", c["new"])
    report.kv("CHANGED (reload):", c["changed"])
    report.kv("UNCHANGED (skip):", c["unchanged"])
    report.kv("CANDIDATE DELETE (review):", c["candidate_delete"])
    report.line("")
    report.kv("UNKEYED (NULL image_no):", c["unkeyed"])
    report.kv("DUPLICATE image_no:", c["duplicate_key"])

    def show(label, ids, extra=None):
        if not ids:
            return
        report.line(f"\n  {label} (first {min(SAMPLE, len(ids))} of {len(ids)}):")
        for i in ids[:SAMPLE]:
            if extra:
                report.line(f"        {i}   {extra(i)}")
            else:
                report.line(f"        {i}")

    by_image, manifest = result["_by_image"], result["_manifest"]
    show("NEW", result["new"])
    show("CHANGED", result["changed"],
         extra=lambda i: f"rowmodid {manifest[i].get('rowmodid')} -> "
                         f"{by_image[i]['rowmodid']}")
    show("CANDIDATE DELETE", result["candidate_delete"])
    show("DUPLICATE image_no", result["duplicate_key"])
    show("UNKEYED (rowid)", result["unkeyed"])

    to_process = c["new"] + c["changed"]
    report.line("")
    report.line(f"  => Pass 2 would extract {to_process} row(s) "
                f"({c['new']} new + {c['changed']} changed); "
                f"{c['unchanged']} skipped.")
    if c["candidate_delete"] or c["unkeyed"] or c["duplicate_key"]:
        report.line("     Review buckets are FLAGGED ONLY — nothing is deleted or "
                    "coerced automatically.")


# =============================================================================
# Live source scan (FileMaker over ODBC) — mirrors the probe.
# =============================================================================
def scan_source(dsn, user, pwd, table, key_col) -> dict:
    import pyodbc
    conn_str = f"DSN={dsn};UID={user};PWD={pwd};CHARSET='UTF-8';ansi=True"
    cnxn = pyodbc.connect(conn_str, timeout=60)
    records: dict = {}
    try:
        cur = cnxn.cursor()
        cur.execute(f'SELECT "{key_col}", ROWID, ROWMODID FROM {table}')
        while True:
            batch = cur.fetchmany(5000)
            if not batch:
                break
            for r in batch:
                key, rowid, rowmod = tuple(r)[0], tuple(r)[1], tuple(r)[2]
                records[str(rowid)] = {
                    "image_no": (str(key) if key is not None else None),
                    "rowmodid": (as_int(rowmod)),
                }
    finally:
        cnxn.close()
    return records


# =============================================================================
# Manifest store (Postgres / Supabase over psycopg2).
# =============================================================================
class PgManifest:
    def __init__(self, host, port, user, pwd, dbname):
        import psycopg2
        self._pg = psycopg2
        self.cnxn = psycopg2.connect(
            host=host, port=port or 5432, user=user, password=pwd or "",
            dbname=dbname or "postgres", sslmode="prefer", connect_timeout=30,
        )
        self.cnxn.autocommit = False

    def init(self):
        with self.cnxn.cursor() as c:
            c.execute(SYNC_MANIFEST_DDL)
        self.cnxn.commit()

    def read_all(self) -> dict:
        with self.cnxn.cursor() as c:
            c.execute("SELECT image_no, fm_rowmodid, row_hash "
                      "FROM rat_migration.sync_manifest")
            return {row[0]: {"rowmodid": as_int(row[1]), "row_hash": row[2]}
                    for row in c.fetchall()}

    def baseline(self, by_image: dict):
        """Assert the target already holds these rows at their current ROWMODID."""
        from psycopg2.extras import execute_values
        rows = [(img, rec["rowid"], as_int(rec["rowmodid"]))
                for img, rec in by_image.items()]
        with self.cnxn.cursor() as c:
            execute_values(c, """
                INSERT INTO rat_migration.sync_manifest
                    (image_no, fm_rowid, fm_rowmodid, last_seen_at,
                     last_loaded_at, load_status)
                VALUES %s
                ON CONFLICT (image_no) DO UPDATE SET
                    fm_rowid       = EXCLUDED.fm_rowid,
                    fm_rowmodid    = EXCLUDED.fm_rowmodid,
                    last_seen_at   = EXCLUDED.last_seen_at,
                    last_loaded_at = EXCLUDED.last_loaded_at,
                    load_status    = 'loaded'
            """, [(img, rid, rm, NOW, NOW) for (img, rid, rm) in rows],
                template="(%s, %s, %s, %s, %s, 'loaded')")
        self.cnxn.commit()
        return len(rows)

    def mark_loaded(self, by_image: dict):
        """Advance the manifest for exactly these image_nos, after -- and only
        after -- the caller has independently verified (against rat.catalog
        itself, not the scan/diff step) that each one actually committed.
        Never call this from the scan/diff path; that would defeat the whole
        point of tracking *verified* loads. Rows not passed here (rejected/
        quarantined) are left untouched, so they naturally reappear as
        new/changed on the next preview -- no separate "still pending" write
        needed.

        by_image: {image_no: {"rowid": ..., "rowmodid": ..., "row_hash": ...}}
        """
        if not by_image:
            return 0
        from psycopg2.extras import execute_values
        rows = [(img, rec.get("rowid"), as_int(rec.get("rowmodid")), rec.get("row_hash"))
                for img, rec in by_image.items()]
        with self.cnxn.cursor() as c:
            execute_values(c, """
                INSERT INTO rat_migration.sync_manifest
                    (image_no, fm_rowid, fm_rowmodid, row_hash, last_seen_at,
                     last_loaded_at, load_status)
                VALUES %s
                ON CONFLICT (image_no) DO UPDATE SET
                    fm_rowid       = EXCLUDED.fm_rowid,
                    fm_rowmodid    = EXCLUDED.fm_rowmodid,
                    row_hash       = EXCLUDED.row_hash,
                    last_seen_at   = EXCLUDED.last_seen_at,
                    last_loaded_at = EXCLUDED.last_loaded_at,
                    load_status    = 'loaded'
            """, [(img, rid, rm, rh, NOW, NOW) for (img, rid, rm, rh) in rows],
                template="(%s, %s, %s, %s, %s, %s, 'loaded')")
        self.cnxn.commit()
        return len(rows)

    def close(self):
        try:
            self.cnxn.close()
        except Exception:
            pass


# =============================================================================
# Resolvers
# =============================================================================
def resolve_source(args, cfg):
    src = source_conf(cfg) if cfg else {}
    dsn = args.dsn or src.get("dsn")
    user = args.user or src.get("user")
    pwd = resolve_secret("RAT_SOURCE_PWD", src.get("pwd", ""), args.pwd)
    return dsn, user, (pwd or "")


def resolve_target(args, cfg):
    t = target_conf(cfg, getattr(args, "target_profile", None)) if cfg else {}
    profile = t.get("_active_profile", "supabase")
    return {
        "host": args.pg_host or t.get("host"),
        "port": args.pg_port or t.get("port"),
        "user": args.pg_user or t.get("user"),
        "pwd": resolve_target_pwd(profile, t.get("pwd", ""), args.pg_pwd),
        # Supabase's database is always 'postgres'; parent keys like dsn/db/name
        # are labels (and 'name' is a list), so don't use them as the dbname.
        "dbname": args.pg_db or t.get("dbname") or "postgres",
    }


def get_current_scan(report: Report, args, cfg):
    """Return records{} either from a snapshot file or a live source scan."""
    if args.from_snapshot:
        report.kv("source (snapshot):", args.from_snapshot)
        return records_from_snapshot(load_snapshot(args.from_snapshot))
    dsn, user, pwd = resolve_source(args, cfg)
    if not dsn or not user:
        raise SystemExit("ERROR: no source DSN/user (config or --dsn/--user); "
                         "or use --from-snapshot.")
    report.kv("source (live ODBC):", f"DSN={dsn} UID={user}")
    return scan_source(dsn, user, pwd, args.table, args.key_col)


def get_manifest(report: Report, args, cfg):
    """Return manifest map either from a snapshot file or the live PG manifest."""
    if args.manifest_snapshot:
        report.kv("manifest (snapshot):", args.manifest_snapshot)
        return manifest_from_records(
            records_from_snapshot(load_snapshot(args.manifest_snapshot))), None
    t = resolve_target(args, cfg)
    if not t["host"] or not t["user"]:
        raise SystemExit("ERROR: no target host/user (config or --pg-*); "
                         "or use --manifest-snapshot.")
    report.kv("manifest (live PG):", f"{t['user']}@{t['host']}:{t['port'] or 5432}")
    pg = PgManifest(**t)
    return pg.read_all(), pg


# =============================================================================
# Self-test (no DB) — proves classify_sync end to end incl. quality buckets.
# =============================================================================
def selftest() -> int:
    print("SELF-TEST: sync classification (no database)")
    # snapshot-style records keyed by rowid
    curr = {
        "10": {"image_no": "arc001", "rowmodid": 5},    # unchanged
        "11": {"image_no": "arc002", "rowmodid": 9},    # changed (7->9)
        "12": {"image_no": "arc003", "rowmodid": 1},    # new
        "13": {"image_no": None,      "rowmodid": 2},   # unkeyed
        "14": {"image_no": "arc004", "rowmodid": 3},    # duplicate key ...
        "15": {"image_no": "arc004", "rowmodid": 4},    # ... same image_no
    }
    manifest = {
        "arc001": {"rowmodid": 5},
        "arc002": {"rowmodid": 7},
        "arc009": {"rowmodid": 1},   # in manifest, gone from source -> candidate delete
    }
    by_image, unkeyed, dups = build_scan(curr)
    r = classify_sync(by_image, unkeyed, dups, manifest)
    c = r["counts"]
    ok = (r["new"] == ["arc003"] and r["changed"] == ["arc002"]
          and r["candidate_delete"] == ["arc009"]
          and c["unchanged"] == 1 and c["unkeyed"] == 1
          and r["duplicate_key"] == ["arc004"])
    print(f"  new={r['new']} changed={r['changed']} "
          f"candidate_delete={r['candidate_delete']}")
    print(f"  unchanged={c['unchanged']} unkeyed={c['unkeyed']} "
          f"duplicate_key={r['duplicate_key']}")
    print("  RESULT:", "PASS ✓" if ok else "FAIL ✗")
    return 0 if ok else 1


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="Incremental-sync change detector (Increment 1)")
    ap.add_argument("--config", default="config.toml")
    # modes
    ap.add_argument("--init", action="store_true", help="create the manifest table")
    ap.add_argument("--baseline", action="store_true",
                    help="seed manifest from current source (asserts target holds them)")
    ap.add_argument("--preview", action="store_true", help="dry-run delta (default)")
    ap.add_argument("--snapshot", action="store_true", help="write a source snapshot and exit")
    ap.add_argument("--list-image-nos", metavar="FILE", help="scan the live source and write just its "
                    "current image_no list to FILE, one per line -- for a caller that needs to filter a "
                    "target-side list down to what's actually still in the source right now (e.g. "
                    "upload_images_oci.py's Storage-gap list can include catalog rows with no matching "
                    "live source row any more -- confirmed live, 2026-09-09).")
    ap.add_argument("--diff-snapshots", nargs=2, metavar=("PREV", "CURR"),
                    help="offline: diff two snapshots, no DB")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--print-ddl", action="store_true")
    ap.add_argument("--yes", action="store_true", help="confirm a writing baseline")
    # source overrides
    ap.add_argument("--dsn"); ap.add_argument("--user"); ap.add_argument("--pwd")
    ap.add_argument("--table", default="ratcatalogue")
    ap.add_argument("--key-col", default="image_no")
    ap.add_argument("--from-snapshot", help="use a snapshot as the current source")
    # target overrides
    ap.add_argument("--pg-host"); ap.add_argument("--pg-port")
    ap.add_argument("--pg-user"); ap.add_argument("--pg-pwd"); ap.add_argument("--pg-db")
    ap.add_argument("--target-profile", help="Target DB profile from config.toml's "
                    "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE)")
    ap.add_argument("--manifest-snapshot", help="use a snapshot as the manifest baseline")
    ap.add_argument("--json", metavar="FILE")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if args.print_ddl:
        print(SYNC_MANIFEST_DDL)
        return 0

    cfg = load_toml(args.config) if os.path.exists(args.config) else {}
    report = Report()
    report.line("PicaLoco sync manifest — change detector")
    report.line(f"Run at {NOW.isoformat()}")

    # ---- offline snapshot diff -------------------------------------------
    if args.diff_snapshots:
        prev, curr = args.diff_snapshots
        report.kv("manifest (snapshot):", prev)
        report.kv("source (snapshot):", curr)
        manifest = manifest_from_records(records_from_snapshot(load_snapshot(prev)))
        by_image, unkeyed, dups = build_scan(
            records_from_snapshot(load_snapshot(curr)))
        result = classify_sync(by_image, unkeyed, dups, manifest)
        report_diff(report, result)
        _emit(report, args)
        return 0

    # ---- snapshot only ----------------------------------------------------
    if args.snapshot:
        dsn, user, pwd = resolve_source(args, cfg)
        if not dsn or not user:
            report.line("ERROR: no source DSN/user."); return 2
        report.kv("scanning:", f"DSN={dsn} UID={user}")
        records = scan_source(dsn, user, pwd, args.table, args.key_col)
        path = write_snapshot(
            f"sync_snapshot_{args.table}_{STAMP}.json",
            {"table": args.table, "captured_at": NOW.isoformat(),
             "rows": len(records)}, records)
        report.kv("rows:", len(records))
        report.kv("snapshot written:", path)
        _emit(report, args)
        return 0

    # ---- list-image-nos only -----------------------------------------------
    if args.list_image_nos:
        dsn, user, pwd = resolve_source(args, cfg)
        if not dsn or not user:
            print("ERROR: no source DSN/user."); return 2
        print(f"scanning: DSN={dsn} UID={user}")
        records = scan_source(dsn, user, pwd, args.table, args.key_col)
        by_image, unkeyed, dups = build_scan(records)
        with open(args.list_image_nos, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(by_image.keys())))
        print(f"{len(by_image)} image_no(s) currently in the source -- written to {args.list_image_nos}")
        return 0

    # ---- init -------------------------------------------------------------
    if args.init:
        t = resolve_target(args, cfg)
        if not t["host"] or not t["user"]:
            report.line("ERROR: no target host/user for --init."); return 2
        report.kv("target:", f"{t['user']}@{t['host']}:{t['port'] or 5432}")
        pg = PgManifest(**t)
        try:
            pg.init()
            report.line("manifest table ready (rat_migration.sync_manifest).")
        finally:
            pg.close()
        _emit(report, args)
        return 0

    # ---- baseline (writes) ------------------------------------------------
    if args.baseline:
        records = get_current_scan(report, args, cfg)
        by_image, unkeyed, dups = build_scan(records)
        report.kv("keyed rows to baseline:", len(by_image))
        report.kv("skipped (unkeyed):", len(unkeyed))
        report.kv("duplicate image_no:", len(dups))
        if not args.yes:
            report.line("")
            report.line("DRY RUN — would seed the above as load_status='loaded'.")
            report.line("Re-run with --yes to write the baseline.")
            _emit(report, args)
            return 0
        t = resolve_target(args, cfg)
        if not t["host"] or not t["user"]:
            report.line("ERROR: no target host/user for --baseline."); return 2
        pg = PgManifest(**t)
        try:
            pg.init()
            n = pg.baseline(by_image)
            report.line(f"baseline written: {n} manifest row(s) marked 'loaded'.")
        finally:
            pg.close()
        _emit(report, args)
        return 0

    # ---- preview (default) ------------------------------------------------
    records = get_current_scan(report, args, cfg)
    by_image, unkeyed, dups = build_scan(records)
    manifest, pg = get_manifest(report, args, cfg)
    try:
        result = classify_sync(by_image, unkeyed, dups, manifest)
        report_diff(report, result)
    finally:
        if pg:
            pg.close()
    _emit(report, args)
    return 0


def _emit(report: Report, args):
    txt = f"sync_report_{STAMP}.txt"
    with open(txt, "w", encoding="utf-8") as f:
        f.write("\n".join(report.lines))
    report.line("")
    report.line(f"(text report saved: {txt})")
    if args.json:
        report.data.setdefault("lines", report.lines)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report.data, f, indent=2, default=str)
        report.line(f"(json report saved: {args.json})")


if __name__ == "__main__":
    raise SystemExit(main())
