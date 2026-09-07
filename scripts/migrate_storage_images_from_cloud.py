#!/usr/bin/env python3
"""
migrate_storage_images_from_cloud.py — one-off cutover, old cloud -> oci
================================================================================
Copies real archive thumbnails already sitting in an old, unrelated Supabase.com
cloud project's public Storage bucket over to oci's Storage, so picaloco_web can
stop depending on that old project entirely.

The old bucket turned out to hold 69,004 real images (2026-09-07), not the ~1,500
first assumed — a wildly wrong early estimate came from this same script's first
version silently hitting the list endpoint's per-request cap. See "Pagination"
below for the fix, and devlog/worksheet.md Session 15/16 for the full story.

This is a RUN-ONCE cutover tool, not part of the ongoing pipeline -- it is
deliberately NOT wired into the GUI. For images exported from FileMaker going
forward, see upload_images_oci.py instead.

Needs no FileMaker/ODBC access -- this is pure Supabase-to-Supabase HTTP, and
runs fine from WSL directly.

PAGINATION
  The old project's `object/list` endpoint silently clamps `limit` to 1,500
  regardless of what's requested -- confirmed by direct testing (limit=1000
  returns 1000, limit=1500/2000/5000 all return exactly 1500). A single
  generous-limit request (this script's first version) looked complete but
  wasn't. Real pagination (limit<=1500, increasing offset, stop once a page
  returns fewer than the limit) does work correctly -- confirmed live, offset
  advances through genuinely different results each page.

SCALE
  At 69,004 files, this is not a "run it and watch" job -- it's parallelized
  (ThreadPoolExecutor, --workers, default 16) and only transfers what's
  missing (diffs a full listing of both buckets up front, not a per-file
  existence check -- 69k HEAD requests would be its own bottleneck). Safe to
  re-run if interrupted or partially failed: already-present destination
  files are skipped via that same diff, not re-uploaded.

USAGE
  # bucket must already exist on oci first: python scripts/upload_images_oci.py --init
  python scripts/migrate_storage_images_from_cloud.py --dry-run           # see counts, copy nothing
  python scripts/migrate_storage_images_from_cloud.py --limit 5           # test against a handful first
  python scripts/migrate_storage_images_from_cloud.py --image-nos arc00002,ab0002
  python scripts/migrate_storage_images_from_cloud.py                     # the full run (parallel)
  python scripts/migrate_storage_images_from_cloud.py --workers 32        # more/less parallelism

Needs RAT_OCI_SERVICE_KEY (oci's service_role key, for writing) and
RAT_OLD_CLOUD_ANON_KEY (the old project's anon key, for its public bucket's
list/read side -- see scripts/env_secrets.py).
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
    import tomllib as _toml
except ModuleNotFoundError:
    import tomli as _toml

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# The old cloud project is noticeably less consistent than oci's Tailscale-local latency --
# seen requests occasionally stall for 10s+ against it during development. Retry transient
# failures rather than let one slow response abort an otherwise-fine migration run. pool_maxsize
# sized for --workers' default concurrency so threads aren't fighting over a small pool.
_session = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retry, pool_maxsize=64, pool_connections=64)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

try:
    from env_secrets import resolve_secret
except ImportError:
    def resolve_secret(env_key, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        try:
            from dotenv import load_dotenv; load_dotenv()
        except ModuleNotFoundError:
            pass
        v = os.environ.get(env_key)
        return v if v else (cfg_val if cfg_val is not None else default)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OLD_CLOUD_URL = "https://tvucfqzldbcghtxddtmq.supabase.co"
OLD_CLOUD_BUCKET = "picaloco"

# The list endpoint's real, confirmed-live per-request cap (see module docstring). Using
# anything higher just gets silently clamped back down to this -- don't "optimize" upward.
LIST_PAGE_SIZE = 1500


def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def _list_page_curl(url: str, bucket: str, prefix: str, headers: dict, offset: int) -> list[dict]:
    """One page of a Storage `object/list` call, via curl rather than `requests`.

    `requests`/urllib3 were observed (2026-09-07) to intermittently stall for
    minutes at a time against this specific host from this environment, with
    no exception ever raised (not even past their own `timeout=`) -- while a
    plain `curl` subprocess to the exact same endpoint was reliably fast on
    every single attempt throughout this session. Not fully explained (a WSL/
    urllib3/TLS interaction with this host's Cloudflare front, plausibly) --
    curl sidesteps it entirely, so listing (this function) uses it. The
    per-file transfer loop (migrate_one) does NOT show this symptom even at
    scale (1,497 files transferred successfully via `requests` earlier this
    session) and stays on `requests`/ThreadPoolExecutor for its concurrency.
    """
    body = json.dumps({"prefix": prefix, "limit": LIST_PAGE_SIZE, "offset": offset})
    cmd = ["curl", "-s", "--max-time", "30", "-X", "POST", f"{url}/storage/v1/object/list/{bucket}"]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd += ["-H", "Content-Type: application/json", "-d", body]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr.strip()}")
    return json.loads(result.stdout)


def _list_all(url: str, bucket: str, prefix: str, headers: dict) -> list[str]:
    """Paginate a Storage `object/list` call to completion. Stops when a page
    returns fewer than LIST_PAGE_SIZE -- confirmed correct behaviour below the
    server's cap (unlike relying on a single over-sized request, see module
    docstring). Each page gets its own small retry budget -- at 47+ pages for
    the full source listing, treating one slow/failed page as fatal would
    make the whole listing far less reliable than any single page actually is."""
    names: list[str] = []
    offset = 0
    while True:
        page = None
        last_err = None
        for attempt in range(4):
            try:
                page = _list_page_curl(url, bucket, prefix, headers, offset)
                break
            except Exception as e:
                last_err = e
                print(f"  page at offset={offset} attempt {attempt + 1} failed: {e}", flush=True)
        if page is None:
            raise RuntimeError(f"page at offset={offset} failed after retries: {last_err}")
        names.extend(item["name"] for item in page if item["name"] != ".emptyFolderPlaceholder")
        print(f"  offset={offset}: {len(page)} items ({len(names)} total so far)", flush=True)
        if len(page) < LIST_PAGE_SIZE:
            break
        offset += LIST_PAGE_SIZE
    return names


def list_source_objects(anon_key: str) -> list[str]:
    """Every object under the old project's picaloco/images/ folder, as full
    bucket-relative paths (the API's own "name" field is relative to the
    "prefix" filter, not the full path -- re-attach "images/" here so every
    downstream consumer can treat these as complete paths)."""
    headers = {"apikey": anon_key, "Authorization": f"Bearer {anon_key}"}
    names = _list_all(OLD_CLOUD_URL, OLD_CLOUD_BUCKET, "images/", headers)
    return [f"images/{n}" for n in names]


def list_dest_objects(base_url: str, bucket: str, headers: dict) -> set[str]:
    """Every object already on oci's bucket, as full bucket-relative paths --
    used to diff against the source list up front instead of a HEAD-per-file
    existence check (which would itself be 69k+ requests)."""
    names = _list_all(base_url, bucket, "images/", headers)
    return {f"images/{n}" for n in names}


def dest_bucket_exists(base_url: str, bucket: str, headers: dict) -> bool:
    r = _session.get(f"{base_url}/storage/v1/bucket/{bucket}", headers=headers, timeout=15)
    return r.status_code == 200


def migrate_one(filename: str, base_url: str, bucket: str, headers: dict) -> None:
    src = _session.get(f"{OLD_CLOUD_URL}/storage/v1/object/public/{OLD_CLOUD_BUCKET}/{filename}", timeout=30)
    src.raise_for_status()

    upload_headers = dict(headers)
    upload_headers["Content-Type"] = "image/webp"
    upload_headers["x-upsert"] = "true"
    dst = _session.post(
        f"{base_url}/storage/v1/object/{bucket}/{filename}",
        headers=upload_headers,
        data=src.content,
        timeout=30,
    )
    dst.raise_for_status()


def main() -> int:
    ap = argparse.ArgumentParser(description="One-off: copy images from the old cloud project's Storage to oci's")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--service-key", help="Override RAT_OCI_SERVICE_KEY")
    ap.add_argument("--anon-key", help="Override RAT_OLD_CLOUD_ANON_KEY")
    ap.add_argument("--dry-run", action="store_true", help="Show counts; copy nothing")
    ap.add_argument("--limit", type=int, help="Only process the first N source files (testing)")
    ap.add_argument("--image-nos", help="Comma-separated image_no list -- only process these (matches filename stem)")
    ap.add_argument("--workers", type=int, default=16, help="Concurrent transfers (default 16)")
    args = ap.parse_args()

    os.chdir(REPO_ROOT)
    config = load_config(args.config)
    storage = config["storage"]
    base_url = storage["public_url"]
    bucket = storage["bucket"]

    service_key = resolve_secret("RAT_OCI_SERVICE_KEY", cli_val=args.service_key)
    if not service_key:
        raise SystemExit("RAT_OCI_SERVICE_KEY not set (env/.env or --service-key).")
    anon_key = resolve_secret("RAT_OLD_CLOUD_ANON_KEY", cli_val=args.anon_key)
    if not anon_key:
        raise SystemExit("RAT_OLD_CLOUD_ANON_KEY not set (env/.env or --anon-key).")

    headers = {"apikey": service_key, "Authorization": f"Bearer {service_key}"}
    if not dest_bucket_exists(base_url, bucket, headers):
        raise SystemExit(
            f"Destination bucket '{bucket}' doesn't exist on oci yet. "
            f"Run: python scripts/upload_images_oci.py --init"
        )

    print("Listing source (old cloud project)...")
    names = list_source_objects(anon_key)
    print(f"Listing destination (oci)...")
    existing = list_dest_objects(base_url, bucket, headers)

    if args.image_nos:
        wanted = {s.strip() for s in args.image_nos.split(",") if s.strip()}
        names = [n for n in names if Path(n).stem in wanted]
    if args.limit:
        names = names[: args.limit]

    to_migrate = [n for n in names if n not in existing]
    skipped = len(names) - len(to_migrate)
    print(f"{len(names)} source file(s) considered, {skipped} already on oci, {len(to_migrate)} to transfer")

    if args.dry_run:
        for n in to_migrate[:20]:
            print(f"[dry-run] would migrate {n}")
        if len(to_migrate) > 20:
            print(f"[dry-run] ... and {len(to_migrate) - 20} more")
        return 0

    migrated = 0
    failed = 0
    failed_names: list[str] = []
    lock = threading.Lock()

    def _worker(name: str) -> tuple[str, Exception | None]:
        try:
            migrate_one(name, base_url, bucket, headers)
            return name, None
        except Exception as e:
            return name, e

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_worker, n) for n in to_migrate]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Migrating"):
            name, err = future.result()
            with lock:
                if err is None:
                    migrated += 1
                else:
                    failed += 1
                    failed_names.append(name)

    print(f"\nDone. migrated={migrated} skipped(existing)={skipped} failed={failed}")
    if failed_names:
        print("Failed (re-run the script -- already-migrated files are skipped, so this is cheap):")
        for n in failed_names[:20]:
            print(f"  {n}")
        if len(failed_names) > 20:
            print(f"  ... and {len(failed_names) - 20} more")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
