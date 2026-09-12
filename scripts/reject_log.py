#!/usr/bin/env python3
"""
reject_log.py — persists loader/upload rejects into rat_migration.reject_log
================================================================================
Every reject in this pipeline has, until now, only ever existed as a one-off
local file (db_dml_loader.py's legacy .sql.reject, run_incremental_sync.py's
and upload_images_oci.py's rejects_*.xlsx) or a log line nobody's watching. A
real reject on a picaloco_agent install at a remote site is invisible until
someone finds and forwards that file. This module is the fix: a small,
queryable, cross-run history in Postgres, next to everything else this
pipeline already writes there.

Two independent halves:
  1. log_reject() — the writer. Called from db_dml_loader.py,
     run_incremental_sync.py, and upload_images_oci.py at the points they
     already catch or classify a bad row — this doesn't replace any of
     those scripts' own local rejects_*.xlsx reports (a volunteer without
     DB access still needs something they can just open), it's additive.
     Best-effort by design: a failed reject_log write must never turn an
     already-classified reject, or a caller's own exception handler, into
     a fresh, worse failure.
  2. This module's own CLI (--list / --export / --resolve / --init) — the
     reader, for an admin reviewing what's accumulated across runs. Same
     rejects_*.xlsx shape as the other two scripts' reports for --export,
     so there's still only one report format to learn.

SEVERITY, and why there are three, not a plain pass/fail:
  - "reject"    a well-classified, known failure (e.g. upload_images_oci.py's
                InvalidKeyError — we know exactly what's wrong and why).
  - "ambiguous" caught, but the reason is a catch-all, not a root cause.
                run_incremental_sync.py's existing quarantine report is
                exactly this today: "didn't verify in rat.catalog" doesn't
                say WHY the loader rejected it.
  - "crash"     the process died instead of rejecting cleanly. Each calling
                script's own __main__ block wraps main() in a top-level
                try/except that logs one crash row (traceback in `reason`,
                whatever partial context was available in `source_data`)
                before still letting the process exit non-zero — the run
                keeps failing loudly, we just stop losing the diagnostic.

DATA REF: image_no, fm_rowid, catalog_id — deliberately three separate
nullable columns, not one. image_no is this pipeline's natural key
everywhere else, so it's the primary ref, but it can't be the ONLY one:
this codebase already has real cases where image_no itself is exactly what's
broken or absent (3 NULL image_no rows, 13 duplicates — see CLAUDE.md's
"Verified facts"). fm_rowid (FileMaker's own ROWID) is always available even
when image_no is the problem — the one universal source-side PK. catalog_id
(rat.catalog's UUID) is set only when the row actually landed there (e.g. a
Storage-stage InvalidKeyError, where the catalog row is fine and only the
image upload failed) — lets an admin jump straight to the live record.

Table lives in rat_migration, not rat — this is pipeline-internal diagnostic
data, read only via direct Postgres connections (this CLI, or picaloco_agent
running the same vendored module), never through PostgREST. Same reasoning
that already governs rat_migration.sync_manifest.

Design discussion + full rationale: devlog/worksheet.md Session 20.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from env_secrets import resolve_target_pwd
except ImportError:                    # self-contained fallback (identical behaviour)
    def resolve_target_pwd(profile, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        try:
            from dotenv import load_dotenv; load_dotenv()
        except ModuleNotFoundError:
            pass
        pwd = os.environ.get(f"RAT_TARGET_PWD_{profile.upper()}")
        if pwd:
            return pwd
        if profile == "supabase":
            pwd = os.environ.get("RAT_TARGET_PWD")
            if pwd:
                return pwd
        return cfg_val if cfg_val is not None else default

try:
    import tomllib as _toml            # Python 3.11+
except ModuleNotFoundError:            # pragma: no cover
    import tomli as _toml

# db_sync_manifest.py is always vendored alongside this script (same
# convention upload_images_oci.py/run_incremental_sync.py rely on) -- reused
# here purely for its config-parsing helpers (resolve_active_profile/
# target_conf), not its FileMaker-facing code.
import db_sync_manifest as dsm

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# =============================================================================
# The reject_log table DDL (idempotent, guarded) -- same convention as
# db_sync_manifest.py's SYNC_MANIFEST_DDL.
# =============================================================================
REJECT_LOG_DDL = """\
CREATE SCHEMA IF NOT EXISTS rat_migration;

CREATE TABLE IF NOT EXISTS rat_migration.reject_log (
    id            bigserial PRIMARY KEY,
    run_at        timestamptz NOT NULL DEFAULT now(),
    run_id        text,               -- groups every reject from one Sync/Upload invocation
    source_script text NOT NULL,       -- 'db_dml_loader.py' | 'run_incremental_sync.py' | 'upload_images_oci.py'
    stage         text NOT NULL,       -- 'extract' | 'load' | 'verify' | 'storage_upload'
    severity      text NOT NULL DEFAULT 'reject',  -- 'reject' | 'ambiguous' | 'crash'
    image_no      text,
    fm_rowid      text,
    catalog_id    uuid,
    table_name    text,
    reason        text NOT NULL,
    source_data   jsonb,              -- whatever's available: full row dict, or raw unparseable text
    resolved      boolean NOT NULL DEFAULT false,
    resolved_at   timestamptz,
    notes         text
);

CREATE INDEX IF NOT EXISTS reject_log_image_no_idx   ON rat_migration.reject_log (image_no);
CREATE INDEX IF NOT EXISTS reject_log_run_at_idx     ON rat_migration.reject_log (run_at);
CREATE INDEX IF NOT EXISTS reject_log_unresolved_idx ON rat_migration.reject_log (resolved) WHERE NOT resolved;

COMMENT ON TABLE rat_migration.reject_log IS
    'Structured, cross-run history of loader/upload rejects -- see scripts/reject_log.py. '
    'Lets an admin spot a record failing the same way across many runs, or diagnose a '
    'crash remotely, without anyone finding and forwarding a local rejects_*.xlsx file.';
"""


def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def _mig_schema(cfg: dict) -> str:
    tgt = cfg["database"]["target"]
    return tgt["schema"][tgt["mig_schema"]]


def _connect(cfg: dict, target_profile: str | None = None):
    """A plain psycopg2 connection to the active [database.target.<profile>].
    Each script that needs a direct connection keeps its own tiny copy of
    this rather than sharing one -- existing convention in this codebase
    (see upload_images_oci.py's _connect_target(), db_sync_manifest.py's
    PgManifest.__init__())."""
    import psycopg2
    profile = dsm.resolve_active_profile(cfg, target_profile)
    tconf = dsm.target_conf(cfg, profile)
    pwd = resolve_target_pwd(profile, tconf.get("pwd", ""))
    return psycopg2.connect(
        host=tconf["host"], port=tconf.get("port") or 5432,
        user=tconf["user"], password=pwd or "",
        dbname=tconf.get("dbname") or "postgres", sslmode="prefer", connect_timeout=30,
    )


def open_connection(cfg: dict, target_profile: str | None = None):
    """Public wrapper around _connect() for a caller that expects to log
    MANY rejects in one run (e.g. db_dml_loader.py's per-row loops, which
    run over the entire catalog every time) and wants to open one
    connection up front and pass it as log_reject()'s `conn` rather than
    opening/closing a fresh one per row."""
    return _connect(cfg, target_profile)


def init_table(cfg: dict, target_profile: str | None = None) -> None:
    conn = _connect(cfg, target_profile)
    try:
        with conn.cursor() as c:
            c.execute(REJECT_LOG_DDL)
        conn.commit()
    finally:
        conn.close()


def new_run_id() -> str:
    """One id shared by every reject_log row a single script invocation
    writes, so an admin can see "these N rejects all came from the same
    Sync run" -- a plain timestamp+random suffix, not a DB sequence (no
    round-trip needed to hand every caller one before any row exists)."""
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"


def _json_safe(v):
    """Recursively sanitize a value for jsonb storage. Postgres's jsonb
    strictly follows RFC 7159 (no NaN/Infinity literals) but Python's json
    module happily *emits* them for a float NaN -- and this pipeline's own
    rows (read via pandas from a hand-entered FileMaker source) are full of
    exactly that for a blank cell. Without this, logging a reject whose
    source_data contains one NaN cell would itself fail the INSERT.
    Anything else not natively JSON-safe falls back to str()."""
    import math
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _str_or_none(v):
    """Coerce a caller-supplied ref/text value to a plain str or None before
    binding it to a text/uuid column -- callers throughout this pipeline
    routinely hand this raw pandas/numpy scalars (a blank cell reads as a
    float NaN, not None), and this whole module exists to survive messy
    data, not add a second place it can crash on it."""
    if v is None:
        return None
    if isinstance(v, float):
        import math
        if math.isnan(v):
            return None
    return v if isinstance(v, str) else str(v)


def log_reject(cfg: dict, target_profile: str | None = None, *, source_script: str, stage: str,
               reason: str, severity: str = "reject", image_no: str | None = None,
               fm_rowid: str | None = None, catalog_id: str | None = None,
               table_name: str | None = None, source_data=None, run_id: str | None = None,
               conn=None) -> None:
    """Best-effort insert into rat_migration.reject_log. NEVER raises -- see
    module docstring. Pass an existing `conn` (any open psycopg2 connection
    to the target) to reuse it and skip opening a new one; otherwise this
    opens and closes its own, short-lived.

    image_no/fm_rowid/catalog_id/table_name are coerced via _str_or_none()
    -- see its own docstring for why. source_data is sanitized via
    _json_safe() then JSON-serialized -- pass whatever's actually available
    at the failure point, even if partial (a raw pandas row dict, a partial
    parse, etc); default=str is kept as a final safety net for anything
    _json_safe()'s isinstance checks miss."""
    own_conn = conn is None
    try:
        if own_conn:
            conn = _connect(cfg, target_profile)
        schema = _mig_schema(cfg)
        with conn.cursor() as c:
            c.execute(
                f"""INSERT INTO {schema}.reject_log
                    (run_id, source_script, stage, severity, image_no, fm_rowid,
                     catalog_id, table_name, reason, source_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (run_id, source_script, stage, severity, _str_or_none(image_no), _str_or_none(fm_rowid),
                 _str_or_none(catalog_id), _str_or_none(table_name), reason,
                 json.dumps(_json_safe(source_data), default=str) if source_data is not None else None),
            )
        conn.commit()
    except Exception as e:
        print(f"(reject_log write failed -- not fatal: {e})", file=sys.stderr)
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def log_crash(source_script: str, config_path: str = "config.toml",
              target_profile: str | None = None, run_id: str | None = None) -> None:
    """Call this from a top-level `except Exception:` block, right before
    re-raising, to record a severity='crash' row with the current
    exception's traceback -- see module docstring's SEVERITY section.
    Loads its own fresh config (the caller may have crashed before loading
    one itself); if THAT fails too, prints to stderr and gives up silently
    -- same failure mode as log_reject() itself. Never raises, never
    suppresses the caller's own re-raise."""
    import traceback
    tb = traceback.format_exc()
    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"(reject_log crash-logging skipped -- couldn't load config: {e})", file=sys.stderr)
        return
    log_reject(cfg, target_profile, source_script=source_script, stage="crash",
               severity="crash", reason=tb, run_id=run_id)


# =============================================================================
# Admin CLI -- the reader/triage half.
# =============================================================================
def _query(cfg: dict, target_profile: str | None, *, unresolved_only: bool,
           since: str | None, image_no: str | None, limit: int) -> list[dict]:
    conn = _connect(cfg, target_profile)
    try:
        schema = _mig_schema(cfg)
        where, params = [], []
        if unresolved_only:
            where.append("resolved = false")
        if since:
            where.append("run_at >= %s")
            params.append(since)
        if image_no:
            where.append("image_no = %s")
            params.append(image_no)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        with conn.cursor() as c:
            c.execute(
                f"""SELECT id, run_at, run_id, source_script, stage, severity, image_no,
                           fm_rowid, catalog_id, table_name, reason, resolved, resolved_at, notes
                    FROM {schema}.reject_log {clause}
                    ORDER BY run_at DESC LIMIT %s""",
                (*params, limit),
            )
            cols = [d.name for d in c.description]
            return [dict(zip(cols, row)) for row in c.fetchall()]
    finally:
        conn.close()


def _resolve(cfg: dict, target_profile: str | None, reject_id: int, note: str | None) -> int:
    conn = _connect(cfg, target_profile)
    try:
        schema = _mig_schema(cfg)
        with conn.cursor() as c:
            c.execute(
                f"UPDATE {schema}.reject_log SET resolved = true, resolved_at = now(), "
                f"notes = COALESCE(%s, notes) WHERE id = %s",
                (note, reject_id),
            )
            updated = c.rowcount
        conn.commit()
        return updated
    finally:
        conn.close()


def _print_table(rows: list[dict]) -> None:
    if not rows:
        print("No matching rejects.")
        return
    for r in rows:
        print(f"[{r['id']}] {r['run_at']}  {r['severity']:<9} {r['source_script']}/{r['stage']}  "
              f"image_no={r['image_no'] or '-'}  fm_rowid={r['fm_rowid'] or '-'}  "
              f"resolved={'yes' if r['resolved'] else 'no'}")
        print(f"      {r['reason']}")


def _export_xlsx(rows: list[dict], out_path: Path) -> None:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Rejects"
    cols = ["id", "run_at", "run_id", "source_script", "stage", "severity", "image_no",
            "fm_rowid", "catalog_id", "table_name", "reason", "resolved", "resolved_at", "notes"]
    ws.append(cols)
    for r in rows:
        ws.append([str(r.get(c)) if r.get(c) is not None else None for c in cols])
    wb.save(out_path)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Review rat_migration.reject_log -- the DB-persisted history of loader/upload "
                     "rejects across every Sync/Upload Images run (see module docstring).")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--target-profile")
    ap.add_argument("--init", action="store_true",
                     help="Create rat_migration.reject_log if it doesn't already exist, then exit.")
    ap.add_argument("--print-ddl", action="store_true", help="Print REJECT_LOG_DDL and exit -- "
                     "no config or connection needed, for inspecting before --init.")
    ap.add_argument("--list", action="store_true", help="Print matching rejects to the console.")
    ap.add_argument("--export", metavar="OUTFILE.xlsx",
                     help="Write matching rejects to an .xlsx file instead of printing.")
    ap.add_argument("--unresolved-only", action="store_true")
    ap.add_argument("--since", help="Only rejects at/after this ISO date/time, e.g. 2026-09-01")
    ap.add_argument("--image-no")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--resolve", type=int, metavar="ID", help="Mark one reject_log row resolved.")
    ap.add_argument("--note", help="Note to attach when resolving (with --resolve).")
    args = ap.parse_args()

    if args.print_ddl:
        print(REJECT_LOG_DDL)
        return 0

    os.chdir(REPO_ROOT)
    cfg = load_config(args.config)

    if args.init:
        init_table(cfg, args.target_profile)
        print("rat_migration.reject_log ready.")
        return 0

    if args.resolve is not None:
        updated = _resolve(cfg, args.target_profile, args.resolve, args.note)
        if updated:
            print(f"Resolved reject_log id={args.resolve}.")
            return 0
        print(f"No reject_log row with id={args.resolve}.", file=sys.stderr)
        return 1

    if args.export or args.list:
        rows = _query(cfg, args.target_profile, unresolved_only=args.unresolved_only,
                      since=args.since, image_no=args.image_no, limit=args.limit)
        if args.export:
            _export_xlsx(rows, Path(args.export))
            print(f"{len(rows)} reject(s) written to {args.export}")
        else:
            _print_table(rows)
        return 0

    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
