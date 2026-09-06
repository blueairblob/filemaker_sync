#!/usr/bin/env python3
"""
migrate_storage_images_from_cloud.py — one-off cutover, old cloud -> oci
================================================================================
Copies the ~1,003 real archive thumbnails already sitting in an old, unrelated
Supabase.com cloud project's public Storage bucket over to oci's Storage, so
picaloco_web can stop depending on that old project entirely.

This is a RUN-ONCE cutover tool, not part of the ongoing pipeline -- it is
deliberately NOT wired into the GUI. For images exported from FileMaker going
forward, see upload_images_oci.py instead.

Needs no FileMaker/ODBC access -- this is pure Supabase-to-Supabase HTTP, and
runs fine from WSL directly.

USAGE
  # bucket must already exist on oci first: python scripts/upload_images_oci.py --init
  python scripts/migrate_storage_images_from_cloud.py --limit 5      # test against a handful first
  python scripts/migrate_storage_images_from_cloud.py --image-nos arc00002,ab0002
  python scripts/migrate_storage_images_from_cloud.py                # the full run

Needs RAT_OCI_SERVICE_KEY (oci's service_role key, for writing) and
RAT_OLD_CLOUD_ANON_KEY (the old project's anon key, for its public bucket's
list/read side -- see scripts/env_secrets.py).
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path

try:
    import tomllib as _toml
except ModuleNotFoundError:
    import tomli as _toml

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# The old cloud project is noticeably less consistent than oci's Tailscale-local latency --
# seen requests occasionally stall for 10s+ against it during development. Retry transient
# failures rather than let one slow response abort an otherwise-fine migration run.
_session = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))

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


def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def list_source_objects(anon_key: str, fetch_limit: int = 20000) -> list[str]:
    """List every object under the old project's picaloco/images/ folder.

    A single request with a generous limit, not offset-based pagination: this
    endpoint was observed (2026-09-06) to keep returning full-size pages
    indefinitely once offset runs past the real end of the data, rather than
    a short final page or an empty one -- an offset-loop that trusts "got
    fewer than page_size back" as its stop condition can spin forever. A
    single big-limit request sidesteps that; the dataset here is a few
    thousand short filenames, trivially small for one JSON response.
    """
    headers = {"apikey": anon_key, "Authorization": f"Bearer {anon_key}"}
    r = _session.post(
        f"{OLD_CLOUD_URL}/storage/v1/object/list/{OLD_CLOUD_BUCKET}",
        headers=headers,
        json={"prefix": "images/", "limit": fetch_limit, "offset": 0},
        timeout=60,
    )
    r.raise_for_status()
    page = r.json()
    if len(page) >= fetch_limit:
        print(
            f"WARNING: got exactly fetch_limit ({fetch_limit}) results back -- there may be "
            f"more than this call captured. Re-run with a higher --fetch-limit if the final "
            f"migrated count looks short."
        )
    # The API's "name" field is relative to the "prefix" filter above, not the full bucket
    # path -- re-attach "images/" so every downstream path (dest_exists, migrate_one) can
    # treat these as full, bucket-relative paths without needing to know that convention itself.
    return [f"images/{item['name']}" for item in page if item["name"] != ".emptyFolderPlaceholder"]


def dest_exists(base_url: str, bucket: str, path: str) -> bool:
    r = _session.head(f"{base_url}/storage/v1/object/public/{bucket}/{path}", timeout=15)
    if r.status_code == 200:
        return True
    r = _session.get(f"{base_url}/storage/v1/object/public/{bucket}/{path}", timeout=15, stream=True)
    r.close()
    return r.status_code == 200


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
    ap.add_argument("--dry-run", action="store_true", help="List what would migrate; copy nothing")
    ap.add_argument("--limit", type=int, help="Only process the first N source files (testing)")
    ap.add_argument("--image-nos", help="Comma-separated image_no list -- only process these (matches filename stem)")
    ap.add_argument("--fetch-limit", type=int, default=20000,
                     help="Max source objects to list in one call (default 20000, generous headroom over the real count)")
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

    names = list_source_objects(anon_key, fetch_limit=args.fetch_limit)
    if args.image_nos:
        wanted = {s.strip() for s in args.image_nos.split(",") if s.strip()}
        names = [n for n in names if Path(n).stem in wanted]
    if args.limit:
        names = names[: args.limit]

    print(f"{len(names)} source file(s) to consider")

    downloaded_uploaded = skipped = failed = 0
    for name in names:
        try:
            if dest_exists(base_url, bucket, name):
                skipped += 1
                continue
            if args.dry_run:
                print(f"[dry-run] would migrate {name}")
                downloaded_uploaded += 1
                continue
            migrate_one(name, base_url, bucket, headers)
            downloaded_uploaded += 1
        except Exception as e:
            failed += 1
            print(f"FAILED {name}: {e}")

    print(f"\nDone. migrated={downloaded_uploaded} skipped(existing)={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
