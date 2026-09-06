#!/usr/bin/env python3
"""
upload_images_oci.py — push locally-exported images to oci's Supabase Storage
================================================================================
The durable half of severing this project's reliance on an old Supabase.com
cloud project for image hosting (the one-off migration of what's already up
there lives in migrate_storage_images_from_cloud.py, a separate script).

This one is the ongoing pipeline step: filemaker_extract.py --get-images (also
the GUI's "Export Images" button) already writes local .webp files to
{export.path}/{export.image_path}/webp/{image_no}.webp -- this script uploads
whatever's in that folder to oci's Storage, using the service_role key (never
anon -- anon's grants on this project are deliberately read-only, both on
Postgres and here).

Run order matters: Export Images -> Upload Images (this script) -> Load to
Target. db_dml_loader.py's picture_metadata.file_location is built
deterministically from image_no, not from confirming an upload succeeded --
so a file that's exported but never uploaded will get a URL that 404s until
this script actually runs for it.

USAGE
  python scripts/upload_images_oci.py --init            # one-time bucket bootstrap
  python scripts/upload_images_oci.py                   # upload everything not already there
  python scripts/upload_images_oci.py --dry-run          # show what would upload, do nothing
  python scripts/upload_images_oci.py --limit 5          # test against a handful first
  python scripts/upload_images_oci.py --image-nos arc00002,ab0002

Requires RAT_OCI_SERVICE_KEY (see scripts/env_secrets.py) -- the oci instance's
service_role key, needed for Storage writes. Not needed for --dry-run's
skip-check reads alone... but IS needed even then, since checking whether an
object already exists on a non-public-yet bucket (before --init has run)
would otherwise 404 ambiguously. Get it from whoever administers the oci
Docker Compose stack; it is not derivable from anything already in this repo.
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path

try:
    import tomllib as _toml            # Python 3.11+
except ModuleNotFoundError:            # pragma: no cover
    import tomli as _toml

import requests

try:
    from env_secrets import resolve_secret
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

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return _toml.load(f)


def local_webp_dir(config: dict) -> Path:
    return Path(f"{config['export']['path']}/{config['export']['image_path']}/webp").resolve()


def object_exists(base_url: str, bucket: str, path: str, headers: dict) -> bool:
    r = requests.head(f"{base_url}/storage/v1/object/public/{bucket}/{path}", timeout=15)
    if r.status_code == 200:
        return True
    # Some self-hosted setups don't implement HEAD cleanly on this route; fall back to GET.
    r = requests.get(f"{base_url}/storage/v1/object/public/{bucket}/{path}", headers=headers, timeout=15, stream=True)
    r.close()
    return r.status_code == 200


def ensure_bucket(base_url: str, bucket: str, headers: dict, cfg_storage: dict) -> None:
    r = requests.get(f"{base_url}/storage/v1/bucket/{bucket}", headers=headers, timeout=15)
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
    r = requests.post(f"{base_url}/storage/v1/bucket", headers=headers, json=body, timeout=15)
    if r.status_code == 409:
        print(f"Bucket '{bucket}' already exists (race with another run) -- fine.")
        return
    r.raise_for_status()
    print(f"Created bucket '{bucket}': {body}")


def upload_one(base_url: str, bucket: str, image_no: str, local_path: Path, headers: dict) -> None:
    upload_headers = dict(headers)
    upload_headers["Content-Type"] = "image/webp"
    upload_headers["x-upsert"] = "true"
    with open(local_path, "rb") as f:
        r = requests.post(
            f"{base_url}/storage/v1/object/{bucket}/images/{image_no}.webp",
            headers=upload_headers,
            data=f.read(),
            timeout=30,
        )
    r.raise_for_status()


def main() -> int:
    ap = argparse.ArgumentParser(description="Upload locally-exported webp images to oci's Supabase Storage")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--service-key", help="Override RAT_OCI_SERVICE_KEY")
    ap.add_argument("--init", action="store_true", help="Create the storage bucket if it doesn't exist, then exit")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be uploaded/skipped; upload nothing")
    ap.add_argument("--limit", type=int, help="Only process the first N local files (testing)")
    ap.add_argument("--image-nos", help="Comma-separated image_no list -- only process these")
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

    webp_dir = local_webp_dir(config)
    if not webp_dir.is_dir():
        raise SystemExit(f"Local export folder not found: {webp_dir} -- run Export Images first.")

    files = sorted(webp_dir.glob("*.webp"))
    if args.image_nos:
        wanted = {s.strip() for s in args.image_nos.split(",") if s.strip()}
        files = [f for f in files if f.stem in wanted]
    if args.limit:
        files = files[: args.limit]

    print(f"{len(files)} local file(s) to consider from {webp_dir}")

    uploaded = skipped = failed = 0
    for f in files:
        image_no = f.stem
        try:
            if object_exists(base_url, bucket, f"images/{image_no}.webp", headers):
                skipped += 1
                continue
            if args.dry_run:
                print(f"[dry-run] would upload {image_no}")
                uploaded += 1
                continue
            upload_one(base_url, bucket, image_no, f, headers)
            uploaded += 1
        except Exception as e:
            failed += 1
            print(f"FAILED {image_no}: {e}")

    print(f"\nDone. uploaded={uploaded} skipped(existing)={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
