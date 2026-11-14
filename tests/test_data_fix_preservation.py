"""The Data Fixes promise: a reviewed value is never overwritten by a sync or
re-extraction. Runs against the Docker postgres (pg fixture), never live."""
from pathlib import Path

import pytest

from tests.conftest import make_admin, seed_catalog
from data_fix_preservation_check import reviewed_checksum
from extract_loco_fields import ApplyResult, apply_rows, build_rows, run
from loco_extract import description_hash

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _values(conn, image_no, fix_id):
    with conn.cursor() as cur:
        cur.execute('select "values", suggested_values, status, needs_recheck, updated_at '
                    "from rat.data_fix_value where image_no=%s and fix_id=%s",
                    (image_no, fix_id))
        return cur.fetchone()


def _review(conn, admin, image_no, fix_id, vals, status):
    ts = _values(conn, image_no, fix_id)[4]
    with conn.cursor() as cur:
        cur.execute("set local role authenticated")
        cur.execute("select set_config('request.jwt.claim.sub', %s, true)", (admin,))
        cur.execute("select rat.review_fix_value(%s,%s,%s::text[],%s,%s)",
                    (image_no, fix_id, vals, status, ts))
        cur.execute("reset role")


def test_reviewed_values_survive_sync_and_reextract(pg):
    conn = pg
    descs = {f"s_pres_{k}": f"Class 4 number {n} Flying Fox heads"
             for k, n in zip("ABCD", (11111, 22222, 33333, 44444))}
    for i, d in descs.items():
        seed_catalog(conn, i, d)
    records = list(descs.items())
    apply_rows(conn, build_rows(records, frozenset()))
    conn.commit()

    admin = make_admin(conn)
    _review(conn, admin, "s_pres_A", "loco_number", ["11111"], "verified")
    _review(conn, admin, "s_pres_B", "loco_number", ["9999"], "corrected")
    _review(conn, admin, "s_pres_C", "loco_number", [], "none_visible")
    conn.commit()
    before = reviewed_checksum(conn)

    # Stage 2 simulation (owner role): B and D change, A and C re-sent unchanged.
    new = dict(descs)
    new["s_pres_B"] = "Class 4 number 55555 Flying Fox heads"
    new["s_pres_D"] = "Class 4 number 66666 Flying Fox heads"
    with conn.cursor() as cur:
        for i, d in new.items():
            cur.execute("INSERT INTO rat.catalog (image_no, description, created_by) "
                        "VALUES (%s,%s,gen_random_uuid()) ON CONFLICT (image_no) "
                        "DO UPDATE SET description = EXCLUDED.description", (i, d))
    conn.commit()

    apply_rows(conn, build_rows(list(new.items()), frozenset()))
    conn.commit()

    assert _values(conn, "s_pres_A", "loco_number")[:3] == (["11111"], ["11111"], "verified")
    assert _values(conn, "s_pres_B", "loco_number")[0] == ["9999"]
    assert _values(conn, "s_pres_B", "loco_number")[2] == "corrected"
    assert _values(conn, "s_pres_B", "loco_number")[1] == ["55555"]
    assert _values(conn, "s_pres_C", "loco_number")[2] == "none_visible"
    assert _values(conn, "s_pres_C", "loco_number")[0] == []
    assert _values(conn, "s_pres_B", "loco_number")[3] is True
    assert _values(conn, "s_pres_A", "loco_number")[3] is False
    assert _values(conn, "s_pres_C", "loco_number")[3] is False
    assert _values(conn, "s_pres_D", "loco_number")[0] == ["66666"]
    assert reviewed_checksum(conn)["checksum"] == before["checksum"]
    assert reviewed_checksum(conn)["reviewed"] >= 3


def test_reapply_is_noop(pg):
    conn = pg
    seed_catalog(conn, "s_pres_noop", "Class 4 number 42616 Flying Fox heads")
    rows = build_rows([("s_pres_noop", "Class 4 number 42616 Flying Fox heads")], frozenset())
    first = apply_rows(conn, rows)
    conn.commit()
    assert first.inserted == 2
    ts = _values(conn, "s_pres_noop", "loco_number")[4]
    assert apply_rows(conn, rows) == ApplyResult(inserted=0, refreshed=0, rechecks=0)
    conn.commit()
    assert _values(conn, "s_pres_noop", "loco_number")[4] == ts


@pytest.mark.parametrize("text", ["café", "a\r\nb", "msmsa026\r5", None])
def test_hash_parity(pg, text):
    with pg.cursor() as cur:
        cur.execute("select encode(sha256(convert_to(coalesce(%s, ''), 'UTF8')), 'hex')", (text,))
        assert cur.fetchone()[0] == description_hash(text)


def test_loader_never_touches_data_fix():
    for name in ("db_dml_loader.py", "filemaker_extract.py", "run_incremental_sync.py"):
        src = (SCRIPTS / name).read_text(encoding="utf-8")
        if name == "run_incremental_sync.py":
            # The sync may only call the extractor (one import + one .run() call)
            # and report under the summary key "data_fix"; any other mention of
            # data_fix (rat.data_fix_*, SQL, etc.) would be direct table access.
            assert "data_fix" not in src.replace('"data_fix"', ""), name
            assert src.count("extract_loco_fields") == 2, name
            assert "import extract_loco_fields" in src and "extract_loco_fields.run(" in src
        else:
            assert "data_fix" not in src and "extract_loco_fields" not in src, name


def test_run_scopes_to_image_nos(pg):
    conn = pg
    seed_catalog(conn, "s_pres_scopeA", "Class 4 number 42616 Flying Fox heads")
    seed_catalog(conn, "s_pres_scopeB", "Class 4 number 42617 Flying Fox heads")
    conn.commit()
    out = run(conn, ["s_pres_scopeA"], dry_run=False)
    assert out["rows"] == 2 and out["apply"]["inserted"] == 2
    with conn.cursor() as cur:
        cur.execute("select image_no, count(*) from rat.data_fix_value "
                    "where image_no like 's_pres_scope%' group by 1")
        assert cur.fetchall() == [("s_pres_scopeA", 2)]
    dry = run(conn, ["s_pres_scopeB"], dry_run=True)
    assert dry["apply"] is None
