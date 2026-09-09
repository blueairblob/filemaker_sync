#!/usr/bin/env python3
"""
upload_images_oci.py — push locally-exported images to oci's Supabase Storage
================================================================================
The durable half of severing this project's reliance on an old Supabase.com
cloud project for image hosting (the one-off migration of what was already up
there lives in migrate_storage_images_from_cloud.py, a separate script).

This one is the ongoing pipeline step: filemaker_extract.py --get-images (also
the GUI's "Export Images" button) already writes local .webp files to
{export.path}/{export.image_path}/webp/{image_no}.webp -- this script uploads
whatever's in that folder to oci's Storage, using the service_role key (never
anon -- anon's grants on this project are deliberately read-only, both on
Postgres and here).

SCALE (2026-09-07): the real local export turned out to hold 141,243 files
(effectively the whole archive) in TWO variants -- {export.path}/images/webp
(full-size, ~13KB/file) and {export.path}/images/webp_mobile (thumbnail-size,
~5.6KB/file, confirmed byte-identical to what trainpixelfolio/picaloco_web
already expect at the "images/<image_no>.webp" path). Use webp_mobile, not
webp, unless picaloco_web's image handling ever changes to want full-size.
Also: config.toml's [export].path did not match where these files actually
live on this machine -- use --webp-dir to point at the real location rather
than fixing config.toml blind (that path may be correct for the Windows-side
FileMaker extraction machine's own layout; not touched here).

At this scale, a per-file existence check (141k HEAD/GET requests) would be
its own bottleneck -- this diffs one full listing of the destination bucket
against the local file list up front, then uploads only what's missing,
in parallel (ThreadPoolExecutor, --workers).

Run order matters: Export Images -> Upload Images (this script) -> Load to
Target. db_dml_loader.py's picture_metadata.file_location is built
deterministically from image_no, not from confirming an upload succeeded --
so a file that's exported but never uploaded will get a URL that 404s until
this script actually runs for it.

USAGE
  python scripts/upload_images_oci.py --init            # one-time bucket bootstrap
  python scripts/upload_images_oci.py --webp-dir /path/to/images/webp_mobile --dry-run
  python scripts/upload_images_oci.py --webp-dir /path/to/images/webp_mobile --limit 5
  python scripts/upload_images_oci.py --webp-dir /path/to/images/webp_mobile --workers 24

Requires RAT_OCI_SERVICE_KEY (see scripts/env_secrets.py) -- the oci instance's
service_role key, needed for Storage writes. Get it from whoever administers
the oci Docker Compose stack; it is not derivable from anything already in
this repo.
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import tomllib as _toml            # Python 3.11+
except ModuleNotFoundError:            # pragma: no cover
    import tomli as _toml

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

try:
    from env_secrets import resolve_secret, resolve_target_pwd
except ImportError:                    # self-contained fallback (identical behaviour)
    def resolve_secret(env_key, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        try:
            from dotenv import load_dotenv; load_dotenv()
        except ModuleNotFoundError:
            pass
        v = os.environ.get(env_key)
        return v if v else (cfg_val if cfg_val is not None else default)

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

try:
    from paths import resolve_export_path
except ImportError:                    # self-contained fallback (identical behaviour)
    import re as _re
    def resolve_export_path(raw_path):
        m = _re.match(r"^([A-Za-z]):[/\\](.*)$", raw_path)
        if os.name != "nt" and m:
            return f"/mnt/{m.group(1).lower()}/{m.group(2).replace('\\\\', '/')}"
        return raw_path

# db_sync_manifest.py is always vendored alongside this script (same convention
# run_incremental_sync.py relies on) -- reused here purely for its config-parsing
# helpers (resolve_active_profile/target_conf), not its FileMaker-facing code, so
# this script stays importable/runnable from WSL with no ODBC dependency.
import db_sync_manifest as dsm

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIST_PAGE_SIZE = 1500  # oci's Storage list endpoint's real per-request cap -- see
                       # migrate_storage_images_from_cloud.py's module docstring for how this
                       # was confirmed; using a higher value just gets silently clamped back down.

_session = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retry, pool_maxsize=64, pool_connections=64)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def local_webp_dir(config: dict) -> Path:
    """The thumbnail-sized webp variant (config's [export].thumbnail_path,
    "webp_mobile") -- what's actually served publicly, not the full-size
    "webp" folder db_dml_loader.py's process_image_folder() reads for
    metadata. See config.toml's [export] section comments."""
    export_path = resolve_export_path(config["export"]["path"])
    thumb = config["export"].get("thumbnail_path", "webp_mobile")
    return Path(f"{export_path}/{config['export']['image_path']}/{thumb}").resolve()


def _list_page_curl(base_url: str, bucket: str, prefix: str, headers: dict, offset: int) -> list[dict]:
    """One page of a Storage `object/list` call, via curl rather than `requests`.

    `requests`/urllib3 were observed (2026-09-07) to intermittently stall for
    minutes against a *different* host (the old cloud project) with no
    exception ever raised. Not confirmed against oci specifically, but using
    the same curl-based approach here too for consistency and since it's
    proven reliable everywhere it's been tried this session.
    """
    body = json.dumps({"prefix": prefix, "limit": LIST_PAGE_SIZE, "offset": offset})
    cmd = ["curl", "-s", "--max-time", "30", "-X", "POST", f"{base_url}/storage/v1/object/list/{bucket}"]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd += ["-H", "Content-Type: application/json", "-d", body]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr.strip()}")
    return json.loads(result.stdout)


def list_dest_objects(base_url: str, bucket: str, headers: dict, debug: bool = False) -> set[str]:
    """Every object already on oci's bucket under images/, as bucket-relative
    paths -- diffed against the local file list up front instead of a
    HEAD-per-file existence check (which would itself be 141k+ requests).

    At full scale this is ~95 pages (141k objects / 1,500-item pages) -- the
    per-page print is real progress evidence for a call that otherwise looks
    hung for a while, but it flooded a captured/streamed log the same way
    export_images()'s tqdm bar did (see filemaker_extract.py), so it's now
    gated behind --debug too. A retry failure is a genuine anomaly, not
    routine progress, so it always prints regardless."""
    names: list[str] = []
    offset = 0
    pages = 0
    while True:
        page = None
        last_err = None
        for attempt in range(4):
            try:
                page = _list_page_curl(base_url, bucket, "images/", headers, offset)
                break
            except Exception as e:
                last_err = e
                print(f"  page at offset={offset} attempt {attempt + 1} failed: {e}", flush=True)
        if page is None:
            raise RuntimeError(f"page at offset={offset} failed after retries: {last_err}")
        names.extend(item["name"] for item in page if item["name"] != ".emptyFolderPlaceholder")
        pages += 1
        if debug:
            print(f"  offset={offset}: {len(page)} items ({len(names)} total so far)", flush=True)
        if len(page) < LIST_PAGE_SIZE:
            break
        offset += LIST_PAGE_SIZE
    if not debug:
        print(f"  {len(names)} object(s) across {pages} page(s)")
    return {f"images/{n}" for n in names}


def _connect_target(cfg: dict, target_profile: str | None):
    """A plain psycopg2 connection to the active [database.target.<profile>],
    resolved the same way every other script in this pipeline does (shared
    with compute_missing_image_nos() and write_invalid_key_report(), the two
    callers that need to query rat.catalog directly)."""
    profile = dsm.resolve_active_profile(cfg, target_profile)
    tconf = dsm.target_conf(cfg, profile)
    pwd = resolve_target_pwd(profile, tconf.get("pwd", ""))
    import psycopg2
    return psycopg2.connect(
        host=tconf["host"], port=tconf.get("port") or 5432,
        user=tconf["user"], password=pwd or "",
        dbname=tconf.get("dbname") or "postgres", sslmode="prefer", connect_timeout=30,
    )


def compute_missing_image_nos(base_url: str, bucket: str, headers: dict, cfg: dict,
                               target_profile: str | None, debug: bool = False) -> list[str]:
    """Every image_no in rat.catalog that doesn't already have an object in
    oci's Storage -- the actual gap a caller needs to fill, computed WITHOUT
    touching FileMaker at all. On a healthy system this is just the
    permanently-quarantined InvalidKey rows (see InvalidKeyError's
    docstring), not the whole archive -- letting a caller (e.g.
    picaloco_agent's Upload Images) extract+upload only these instead of
    walking all 141k+ rows over ODBC every single run.

    Deliberately reads rat.catalog directly rather than rat_migration's
    staging tables or sync_manifest -- catalog is what's actually live, and
    is what picaloco_web/the mobile_catalog_view serve from."""
    schema = cfg["database"]["target"]["schema"][cfg["database"]["target"]["tgt_schema"]]
    conn = _connect_target(cfg, target_profile)
    try:
        with conn.cursor() as c:
            c.execute(f"SELECT image_no FROM {schema}.catalog WHERE image_no IS NOT NULL")
            catalog_image_nos = {row[0] for row in c.fetchall()}
    finally:
        conn.close()
    if debug:
        print(f"  {len(catalog_image_nos)} image_no(s) in {schema}.catalog")

    existing = list_dest_objects(base_url, bucket, headers, debug=debug)
    existing_stems = {e[len("images/"):-len(".webp")] for e in existing}
    return sorted(catalog_image_nos - existing_stems)


def ensure_bucket(base_url: str, bucket: str, headers: dict, cfg_storage: dict) -> None:
    r = _session.get(f"{base_url}/storage/v1/bucket/{bucket}", headers=headers, timeout=15)
    if r.status_code == 200:
        print(f"Bucket '{bucket}' already exists -- nothing to do.")
        return
    body = {
        "id": bucket,
        "name": bucket,
        "public": True,
        "allowed_mime_types": cfg_storage.get("allowed_mime_types", ["image/webp"]),
        "file_size_limit": cfg_storage.get("file_size_limit", 900000),
    }
    r = _session.post(f"{base_url}/storage/v1/bucket", headers=headers, json=body, timeout=15)
    if r.status_code == 409:
        print(f"Bucket '{bucket}' already exists (race with another run) -- fine.")
        return
    r.raise_for_status()
    print(f"Created bucket '{bucket}': {body}")


class InvalidKeyError(Exception):
    """A known, permanent, un-fixable-here rejection: Supabase Storage's
    object-key validation refuses image_nos containing certain characters
    (a literal backtick, in most cases; at least one accented character).
    See devlog/worksheet.md Session 16 -- deliberately quarantined rather
    than sanitized, since renaming the key would break db_dml_loader.py's
    deterministic image_no -> URL convention for a 0.03% edge case. This
    will reproduce identically on every future run for the same rows; kept
    distinct from a generic failure so callers don't have to keep treating
    an expected, unactionable rejection as a fresh problem."""


def write_invalid_key_report(invalid_key_names: list, cfg: dict, target_profile: str | None) -> str | None:
    """A browsable .xlsx of every image_no InvalidKeyError rejected this
    run -- printing them to the log (as this script already did) isn't
    enough on its own for a RAT volunteer to act on; they need something
    they can actually open and work through. Same generic "rejects_*.xlsx"
    name and image_no/id/reason shape as run_incremental_sync.py's own
    quarantine report (a different script, so this can't literally be the
    same file, but a volunteer shouldn't have to learn two different report
    formats depending on which pipeline stage caught the problem).

    `id` here is rat.catalog's own UUID -- unlike a loader-rejected row,
    these DID land in rat.catalog (their only failure is the Storage
    upload), so a real id is available with one extra query.

    Best-effort: missing openpyxl, the DB lookup failing, or the write
    itself failing all log a warning and don't fail the run -- these rows
    already aren't counted as failures (see InvalidKeyError's docstring),
    this is purely an added convenience."""
    if not invalid_key_names:
        return None
    try:
        from openpyxl import Workbook
    except ImportError:
        print("(rejects report skipped: openpyxl not installed -- pip install openpyxl)", file=sys.stderr)
        return None

    ids: dict = {}
    try:
        schema = cfg["database"]["target"]["schema"][cfg["database"]["target"]["tgt_schema"]]
        conn = _connect_target(cfg, target_profile)
        try:
            with conn.cursor() as c:
                c.execute(f"SELECT image_no, id FROM {schema}.catalog WHERE image_no = ANY(%s)",
                          (invalid_key_names,))
                ids = {row[0]: row[1] for row in c.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        print(f"(rejects report: couldn't look up rat.catalog ids: {e})", file=sys.stderr)

    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Rejected records"
        ws.append(["image_no", "id", "reason"])
        for img in sorted(invalid_key_names):
            ws.append([img, str(ids[img]) if img in ids else None,
                       "Supabase Storage rejected this image_no as an object key (a backtick or "
                       "similar character) -- open this record in FileMaker Pro (search by Image no.) "
                       "and retype the field without that character."])

        export_path = cfg.get("export", {}).get("path", "export")
        out_dir = Path(export_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"rejects_{dsm.NOW.strftime('%Y%m%d_%H%M%S')}.xlsx"
        wb.save(out_path)
        return str(out_path)
    except Exception as e:
        print(f"(rejects report write failed: {e})", file=sys.stderr)
        return None


def upload_one(base_url: str, bucket: str, image_no: str, local_path: Path, headers: dict) -> None:
    upload_headers = dict(headers)
    upload_headers["Content-Type"] = "image/webp"
    upload_headers["x-upsert"] = "true"
    with open(local_path, "rb") as f:
        data = f.read()
    r = _session.post(
        f"{base_url}/storage/v1/object/{bucket}/images/{image_no}.webp",
        headers=upload_headers,
        data=data,
        timeout=30,
    )
    if r.status_code == 400 and "InvalidKey" in r.text:
        raise InvalidKeyError(image_no)
    r.raise_for_status()


def main() -> int:
    ap = argparse.ArgumentParser(description="Upload locally-exported webp images to oci's Supabase Storage")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--webp-dir", help="Local folder of .webp files to upload (overrides config.toml's [export] path)")
    ap.add_argument("--service-key", help="Override RAT_OCI_SERVICE_KEY")
    ap.add_argument("--init", action="store_true", help="Create the storage bucket if it doesn't exist, then exit")
    ap.add_argument("--dry-run", action="store_true", help="Show counts; upload nothing")
    ap.add_argument("--limit", type=int, help="Only process the first N local files (testing)")
    ap.add_argument("--image-nos", help="Comma-separated image_no list -- only process these")
    ap.add_argument("--force", action="store_true", help="Skip the destination-existence check "
                     "(which only tells you a name is present, not whether its content changed) and "
                     "upload every selected file unconditionally -- x-upsert overwrites. For a small, "
                     "already-known set (e.g. a sync's own new+changed image_nos) this is cheaper than "
                     "listing the whole bucket AND is the only way a changed image whose image_no "
                     "already exists in Storage actually gets re-uploaded.")
    ap.add_argument("--workers", type=int, default=16, help="Concurrent uploads (default 16)")
    ap.add_argument("--debug", action="store_true", help="Show per-page destination-listing progress "
                     "(normally just a one-line total, since ~95 pages at full catalog scale floods a "
                     "captured/streamed log) and the per-file upload progress bar.")
    ap.add_argument("--target-profile", help="Target DB profile from config.toml's "
                     "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE) -- only "
                     "used by --list-missing, to query rat.catalog.")
    ap.add_argument("--list-missing", metavar="OUTFILE", help="Don't upload anything -- instead, compute "
                     "every image_no in rat.catalog that doesn't already have an object in oci's Storage "
                     "(a Postgres query + a bucket listing, no local files or FileMaker involved) and "
                     "write that list to OUTFILE, one per line. Lets a caller extract+upload only the "
                     "actual gap instead of walking the whole archive every run.")
    args = ap.parse_args()

    os.chdir(REPO_ROOT)
    config = load_config(args.config)
    storage = config["storage"]
    base_url = storage["public_url"]
    bucket = storage["bucket"]

    service_key = resolve_secret("RAT_OCI_SERVICE_KEY", cli_val=args.service_key)
    if not service_key:
        raise SystemExit(
            "RAT_OCI_SERVICE_KEY not set (env/.env or --service-key). This is the oci "
            "instance's service_role key -- get it from whoever administers the oci host."
        )
    headers = {"apikey": service_key, "Authorization": f"Bearer {service_key}"}

    if args.init:
        ensure_bucket(base_url, bucket, headers, storage)
        return 0

    if args.list_missing:
        missing = compute_missing_image_nos(base_url, bucket, headers, config,
                                             args.target_profile, debug=args.debug)
        Path(args.list_missing).write_text("\n".join(missing), encoding="utf-8")
        print(f"{len(missing)} image_no(s) missing from oci Storage -- written to {args.list_missing}")
        return 0

    webp_dir = Path(args.webp_dir).resolve() if args.webp_dir else local_webp_dir(config)
    if not webp_dir.is_dir():
        raise SystemExit(f"Local export folder not found: {webp_dir} -- run Export Images first, or pass --webp-dir.")

    files = sorted(webp_dir.glob("*.webp"))
    if args.image_nos:
        wanted = {s.strip() for s in args.image_nos.split(",") if s.strip()}
        files = [f for f in files if f.stem in wanted]
    if args.limit:
        files = files[: args.limit]

    print(f"{len(files)} local file(s) under {webp_dir}")
    if args.force:
        to_upload = files
        skipped = 0
        print(f"--force: skipping destination listing, uploading all {len(to_upload)} selected file(s)")
    else:
        print("Listing destination (oci)...")
        existing = list_dest_objects(base_url, bucket, headers, debug=args.debug)
        to_upload = [f for f in files if f"images/{f.stem}.webp" not in existing]
        skipped = len(files) - len(to_upload)
        print(f"{skipped} already on oci, {len(to_upload)} to upload")

    if args.dry_run:
        for f in to_upload[:20]:
            print(f"[dry-run] would upload {f.stem}")
        if len(to_upload) > 20:
            print(f"[dry-run] ... and {len(to_upload) - 20} more")
        return 0

    uploaded = 0
    known_invalid_key = 0
    failed = 0
    invalid_key_names: list[str] = []
    failed_names: list[tuple[str, str]] = []
    lock = threading.Lock()

    def _worker(f: Path) -> tuple[str, Exception | None]:
        try:
            upload_one(base_url, bucket, f.stem, f, headers)
            return f.stem, None
        except Exception as e:
            return f.stem, e

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_worker, f) for f in to_upload]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading", disable=not args.debug):
            image_no, err = future.result()
            with lock:
                if err is None:
                    uploaded += 1
                elif isinstance(err, InvalidKeyError):
                    known_invalid_key += 1
                    invalid_key_names.append(image_no)
                else:
                    failed += 1
                    failed_names.append((image_no, str(err)))

    print(f"\nDone. uploaded={uploaded} skipped(existing)={skipped} "
          f"known_invalid_key={known_invalid_key} failed={failed}")
    if invalid_key_names:
        # Expected, permanent, not actionable here -- see InvalidKeyError's docstring.
        # Printed every run for transparency (quarantine, not hide), never counted
        # toward `failed`/the exit code -- there's nothing to "fix" by re-running.
        print(f"{known_invalid_key} image_no(s) have a Storage-rejected character "
              f"(known issue, not new -- see devlog/worksheet.md Session 16):")
        for n in invalid_key_names[:20]:
            print(f"  {n}")
        if len(invalid_key_names) > 20:
            print(f"  ... and {len(invalid_key_names) - 20} more")
        invalid_key_report_path = write_invalid_key_report(invalid_key_names, config, args.target_profile)
        if invalid_key_report_path:
            print(f"Report (for FileMaker-side correction): {invalid_key_report_path}")
    if failed_names:
        print("Failed (unexpected -- re-run the script; already-uploaded files are skipped, so this is cheap):")
        for n, err in failed_names[:20]:
            print(f"  {n}: {err}")
        if len(failed_names) > 20:
            print(f"  ... and {len(failed_names) - 20} more")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
