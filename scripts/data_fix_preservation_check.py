"""Fingerprint of every reviewed (status <> 'extracted') data_fix_value row.

Run before and after a sync/extraction: the checksum must not change.
Read-only session.

    venv/bin/python scripts/data_fix_preservation_check.py --target-profile oci
"""
import argparse
import json
import sys

CHECKSUM_SQL = """
SELECT count(*),
       md5(coalesce(string_agg(image_no || '|' || fix_id || '|' || "values"::text || '|' || status,
                               E'\\n' ORDER BY image_no, fix_id), ''))
FROM rat.data_fix_value WHERE status <> 'extracted'
"""


def reviewed_checksum(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(CHECKSUM_SQL)
        n, h = cur.fetchone()
    return {"reviewed": n, "checksum": h}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--config", default="config.toml")
    p.add_argument("--target-profile", default=None)
    args = p.parse_args(argv)
    import reject_log
    conn = reject_log.open_connection(reject_log.load_config(args.config), args.target_profile)
    try:
        conn.set_session(readonly=True)
        print(json.dumps(reviewed_checksum(conn)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
