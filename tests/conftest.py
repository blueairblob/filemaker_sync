"""Shared pytest fixtures and helpers.

The `pg` fixture runs a throwaway postgres:17 container (the same major as the
live `oci` target, read via server_version_num) and applies, in order:
tests/sql/supabase_stubs.sql, supabase/schema/bootstrap_rat_schema.sql and
supabase/schema/add_data_fix_tables.sql. Tests never touch the live DB
(Golden Rule #4).

The fixture is session-scoped and shared: each test uses its own image_no
prefix (e.g. "s_anon_1") and never asserts on table-wide counts.

The connection connects as the `postgres` superuser, which also owns every
object, so plain statements run "as the owner" (like the extractor and the
pipeline scripts). Use as_anon()/as_user() to switch role for the current
transaction only. A per-test teardown rolls back whatever the test left open.
"""
from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
STUBS_SQL = REPO_ROOT / "tests" / "sql" / "supabase_stubs.sql"
BOOTSTRAP_SQL = REPO_ROOT / "supabase" / "schema" / "bootstrap_rat_schema.sql"
DATA_FIX_SQL = REPO_ROOT / "supabase" / "schema" / "add_data_fix_tables.sql"

PG_IMAGE = "postgres:17"   # live oci: server_version_num 170006
PG_PORT = 55432
PG_PASSWORD = "test"
CONTAINER_NAME = "filemaker_sync_test_pg"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True,
                              timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _connect(autocommit: bool = False):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="postgres",
                            password=PG_PASSWORD, dbname="postgres",
                            connect_timeout=3)
    conn.autocommit = autocommit
    return conn


def apply_sql_file(path: Path) -> None:
    """Apply one SQL file on a fresh autocommit connection (raises on error)."""
    conn = _connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(path.read_text(encoding="utf-8"))
    finally:
        conn.close()


@pytest.fixture(scope="session")
def pg():
    if not _docker_available():
        pytest.skip("Docker is unavailable; skipping Postgres integration tests")

    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
    subprocess.run(
        ["docker", "run", "--rm", "-d", "--name", CONTAINER_NAME,
         "-p", f"{PG_PORT}:5432", "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
         PG_IMAGE],
        check=True, capture_output=True,
    )
    conn = None
    try:
        deadline = time.monotonic() + 90
        while True:
            try:
                probe = _connect(autocommit=True)
                with probe.cursor() as cur:
                    cur.execute("select 1")
                probe.close()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)

        for path in (STUBS_SQL, BOOTSTRAP_SQL, DATA_FIX_SQL):
            apply_sql_file(path)

        conn = _connect()
        yield conn
    finally:
        if conn is not None:
            conn.close()
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


@pytest.fixture(autouse=True)
def _pg_rollback_after_test(request):
    """Roll back anything a pg-using test left open (incl. SET LOCAL ROLE)."""
    yield
    if "pg" in request.fixturenames:
        request.getfixturevalue("pg").rollback()


# --------------------------------------------------------------------------
# Helpers (shared with later tasks' tests). None of them commit: the caller
# decides when to commit or roll back.
# --------------------------------------------------------------------------

def as_anon(conn) -> None:
    """Act as PostgREST's `anon` role for the rest of the current transaction."""
    with conn.cursor() as cur:
        cur.execute("set local role anon")


def as_user(conn, uid: str) -> None:
    """Act as a signed-in user (`authenticated` + JWT sub claim) for the rest
    of the current transaction."""
    with conn.cursor() as cur:
        cur.execute("set local role authenticated")
        cur.execute("select set_config('request.jwt.claim.sub', %s, true)", (str(uid),))


def seed_catalog(conn, image_no: str, description: str | None) -> str:
    """Insert (or update the description of) a rat.catalog row with only the
    NOT NULL columns filled. Returns the catalog id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into rat.catalog (image_no, description, created_by)
            values (%s, %s, gen_random_uuid())
            on conflict (image_no) do update set description = excluded.description
            returning id
            """,
            (image_no, description),
        )
        return str(cur.fetchone()[0])


def make_user(conn, uid: str | None = None) -> str:
    """Insert an auth.users row (not an admin). Returns the user id."""
    uid = uid or str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "insert into auth.users (id, email) values (%s, %s) on conflict (id) do nothing",
            (uid, f"{uid}@example.test"),
        )
    return uid


def make_admin(conn, uid: str | None = None) -> str:
    """Insert an auth.users row and make it a Data Fixes admin. Returns the user id."""
    uid = make_user(conn, uid)
    with conn.cursor() as cur:
        cur.execute(
            "insert into rat.admin_user (user_id) values (%s) on conflict (user_id) do nothing",
            (uid,),
        )
    return uid


def seed_fix_value(conn, image_no: str, fix_id: str = "loco_number", *,
                   values=(), suggested_values=None, confidence: str = "high",
                   status: str = "extracted", needs_recheck: bool = False,
                   description_hash: str = "", extractor_version: str = "test"):
    """Insert a rat.data_fix_value row as the owner. Returns its updated_at."""
    if suggested_values is None:
        suggested_values = values
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into rat.data_fix_value
              (image_no, fix_id, "values", suggested_values, confidence, status,
               needs_recheck, description_hash, extractor_version)
            values (%s, %s, %s::text[], %s::text[], %s, %s, %s, %s, %s)
            returning updated_at
            """,
            (image_no, fix_id, list(values), list(suggested_values), confidence,
             status, needs_recheck, description_hash, extractor_version),
        )
        return cur.fetchone()[0]
