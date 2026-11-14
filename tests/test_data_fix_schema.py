"""Integration tests for supabase/schema/add_data_fix_tables.sql.

Run against a throwaway postgres container (see conftest.pg), never the live DB.
Every test uses its own image_no prefix because the database is shared.
"""
import datetime as dt

import psycopg2
import psycopg2.errors
import psycopg2.extras
import pytest

from tests.conftest import (DATA_FIX_SQL, apply_sql_file, as_anon, as_user,
                      make_admin, make_user, seed_catalog, seed_fix_value)
from loco_extract import description_hash

OLD_TS = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)


def _row(conn, image_no, fix_id="loco_number"):
    with conn.cursor() as cur:
        cur.execute(
            """select "values", status, reviewed_by::text, needs_recheck, updated_at,
                      description_hash, suggested_values
                 from rat.data_fix_value where image_no = %s and fix_id = %s""",
            (image_no, fix_id),
        )
        return cur.fetchone()


def test_anon_cannot_read_values(pg):
    seed_fix_value(pg, "s_anon_1", values=["123"])
    pg.commit()
    as_anon(pg)
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        with pg.cursor() as cur:
            cur.execute("select * from rat.data_fix_value where image_no = 's_anon_1'")


def test_non_admin_sees_nothing(pg):
    seed_fix_value(pg, "s_nonadm_1", values=["123"])
    uid = make_user(pg)
    pg.commit()

    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.data_fix_value where image_no = 's_nonadm_1'")
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from rat.data_fix_history where image_no = 's_nonadm_1'")
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from rat.data_fix")
        assert cur.fetchone()[0] == 0
        cur.execute("select rat.is_admin()")
        assert cur.fetchone()[0] is False
    pg.rollback()

    as_user(pg, uid)
    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("select * from rat.data_fix_progress()")
    assert exc.value.pgcode == "42501"


def test_admin_reads_and_reviews(pg):
    desc = "Class 4 number 42616 at Shed"
    seed_catalog(pg, "s_admin_1", desc)
    seed_fix_value(pg, "s_admin_1", values=["42616"], description_hash="stale")
    uid = make_admin(pg)
    pg.commit()

    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select rat.is_admin()")
        assert cur.fetchone()[0] is True
        cur.execute("select updated_at from rat.data_fix_value "
                    "where image_no = 's_admin_1' and fix_id = 'loco_number'")
        updated_at = cur.fetchone()[0]
        cur.execute(
            "select image_no, \"values\", status from rat.review_fix_value(%s, %s, %s, %s, %s)",
            ("s_admin_1", "loco_number", ["9999"], "corrected", updated_at),
        )
        assert cur.fetchone() == ("s_admin_1", ["9999"], "corrected")
    pg.commit()

    values, status, reviewed_by, needs_recheck, _, dhash, _ = _row(pg, "s_admin_1")
    assert values == ["9999"]
    assert status == "corrected"
    assert reviewed_by == uid
    assert needs_recheck is False
    assert dhash == description_hash(desc)   # same hash as the Python extractor
    with pg.cursor() as cur:
        cur.execute("select source, changed_by::text, new_values, new_status "
                    "from rat.data_fix_history where image_no = 's_admin_1' "
                    "and source = 'review'")
        assert cur.fetchall() == [("review", uid, ["9999"], "corrected")]


def test_review_stale_raises_40001(pg):
    seed_fix_value(pg, "s_stale_1", values=["1"])
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("select rat.review_fix_value(%s, %s, %s, %s, %s)",
                        ("s_stale_1", "loco_number", ["2"], "corrected", OLD_TS))
    assert exc.value.pgcode == "40001"
    pg.rollback()
    assert _row(pg, "s_stale_1")[:2] == (["1"], "extracted")


def test_review_bad_status_raises_22023(pg):
    updated_at = seed_fix_value(pg, "s_badst_1", values=["1"])
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("select rat.review_fix_value(%s, %s, %s, %s, %s)",
                        ("s_badst_1", "loco_number", ["2"], "extracted", updated_at))
    assert exc.value.pgcode == "22023"


def test_review_none_visible_stores_empty(pg):
    updated_at = seed_fix_value(pg, "s_nonevis_1", values=["77"])
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select rat.review_fix_value(%s, %s, %s, %s, %s)",
                    ("s_nonevis_1", "loco_number", ["77"], "none_visible", updated_at))
    pg.commit()
    assert _row(pg, "s_nonevis_1")[:2] == ([], "none_visible")


def test_guard_blocks_direct_update(pg):
    updated_at = seed_fix_value(pg, "s_guard_1", values=["5"], suggested_values=["5"])
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select rat.review_fix_value(%s, %s, %s, %s, %s)",
                    ("s_guard_1", "loco_number", ["5"], "verified", updated_at))
    pg.commit()

    # As the owner (the extractor's role), in a fresh transaction.
    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("update rat.data_fix_value set values = '{1}' "
                        "where image_no = 's_guard_1' and fix_id = 'loco_number'")
    assert "reviewed value is protected" in str(exc.value)
    pg.rollback()

    with pytest.raises(psycopg2.Error) as exc:
        with pg.cursor() as cur:
            cur.execute("update rat.data_fix_value set status = 'extracted' "
                        "where image_no = 's_guard_1' and fix_id = 'loco_number'")
    assert "reviewed value is protected" in str(exc.value)
    pg.rollback()

    # The extractor may still refresh the suggestion and flag a recheck.
    before = _row(pg, "s_guard_1")[4]
    with pg.cursor() as cur:
        cur.execute("update rat.data_fix_value set suggested_values = '{6}', needs_recheck = true "
                    "where image_no = 's_guard_1' and fix_id = 'loco_number'")
    pg.commit()
    values, status, _, needs_recheck, after, _, suggested = _row(pg, "s_guard_1")
    assert (values, status, needs_recheck, suggested) == (["5"], "verified", True, ["6"])
    assert after > before   # updated_at always refreshed


def test_confirm_skips_stale(pg):
    desc = "number 1 and number 2"
    seed_catalog(pg, "s_conf_1", desc)
    ts1 = seed_fix_value(pg, "s_conf_1", values=[], suggested_values=["111"])
    pg.commit()
    seed_fix_value(pg, "s_conf_2", values=[], suggested_values=["222"])
    uid = make_admin(pg)
    pg.commit()

    items = psycopg2.extras.Json([
        {"image_no": "s_conf_1", "updated_at": ts1.isoformat()},
        {"image_no": "s_conf_2", "updated_at": OLD_TS.isoformat()},
    ])
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select rat.confirm_fix_values(%s, %s)", ("loco_number", items))
        assert cur.fetchone()[0] == 1
    pg.commit()

    values, status, reviewed_by, needs_recheck, _, dhash, _ = _row(pg, "s_conf_1")
    assert (values, status, reviewed_by, needs_recheck) == (["111"], "verified", uid, False)
    assert dhash == description_hash(desc)
    assert _row(pg, "s_conf_2")[:2] == ([], "extracted")
    with pg.cursor() as cur:
        cur.execute("select count(*) from rat.data_fix_history "
                    "where image_no = 's_conf_1' and source = 'review'")
        assert cur.fetchone()[0] == 1


def test_confirm_never_overwrites_correction(pg):
    ts = seed_fix_value(pg, "s_confcorr_1", values=["CORR"], suggested_values=["NEW"],
                        status="corrected", needs_recheck=True)
    uid = make_admin(pg)
    pg.commit()
    items = psycopg2.extras.Json([{"image_no": "s_confcorr_1", "updated_at": ts.isoformat()}])
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select rat.confirm_fix_values(%s, %s)", ("loco_number", items))
        assert cur.fetchone()[0] == 0
    pg.commit()
    values, status, _, needs_recheck, updated_at, _, suggested = _row(pg, "s_confcorr_1")
    assert (values, status, needs_recheck, suggested, updated_at) == (
        ["CORR"], "corrected", True, ["NEW"], ts)


def test_view_text_semantics(pg):
    for n in range(1, 5):
        seed_catalog(pg, f"s_view_{n}", f"view photo {n}")
    # s_view_1: no data_fix_value row at all
    seed_fix_value(pg, "s_view_2", values=[], status="extracted")
    seed_fix_value(pg, "s_view_3", values=["9"], status="no_loco")
    seed_fix_value(pg, "s_view_4", values=["A1", "B2"], status="corrected")
    seed_fix_value(pg, "s_view_4", "loco_name", values=["Flying Fox"], status="extracted")
    pg.commit()

    as_anon(pg)
    with pg.cursor() as cur:
        cur.execute("""select image_no, loco_numbers, loco_numbers_text,
                              loco_names, loco_names_text
                         from rat.mobile_catalog_view
                        where image_no like 's_view_%' order by image_no""")
        rows = cur.fetchall()
    assert [r[2] for r in rows] == [None, None, "", "A1; B2"]
    assert [r[1] for r in rows] == [None, [], [], ["A1", "B2"]]
    assert rows[3][3:] == (["Flying Fox"], "Flying Fox")
    assert rows[0][3:] == (None, None)


def test_queue_filters_and_paging(pg):
    tok = "zzqueuetok"
    for suffix in "abcd":
        seed_catalog(pg, f"s_queue_{suffix}", f"{tok} photo {suffix}")
    seed_fix_value(pg, "s_queue_a", values=["1"], confidence="high")
    seed_fix_value(pg, "s_queue_b", values=["2"], confidence="high")
    seed_fix_value(pg, "s_queue_c", values=["3"], confidence="low")
    seed_fix_value(pg, "s_queue_d", values=["4"], confidence="high",
                   status="verified", needs_recheck=True)
    seed_fix_value(pg, "s_queue_e", values=["5"], confidence="high")   # not in catalog
    uid = make_admin(pg)
    pg.commit()

    as_user(pg, uid)

    def page(after=None, **kw):
        with pg.cursor() as cur:
            cur.execute(
                """select image_no, description, "values", confidence, status, needs_recheck
                     from rat.data_fix_queue('loco_number', p_text => %(tok)s,
                          p_confidence => %(conf)s, p_after => %(after)s,
                          p_limit => %(limit)s, p_include_recheck => %(rc)s)""",
                {"tok": tok, "conf": kw.get("conf", ["high"]), "after": after,
                 "limit": kw.get("limit", 1), "rc": kw.get("rc", True)},
            )
            return cur.fetchall()

    p1 = page()
    assert [r[0] for r in p1] == ["s_queue_a"]
    assert p1[0][1] == f"{tok} photo a"
    assert [r[0] for r in page("s_queue_a")] == ["s_queue_b"]
    p3 = page("s_queue_b")
    assert [(r[0], r[4], r[5]) for r in p3] == [("s_queue_d", "verified", True)]
    assert page("s_queue_d") == []
    assert [r[0] for r in page(conf=None, limit=25)] == [
        "s_queue_a", "s_queue_b", "s_queue_c", "s_queue_d"]
    assert [r[0] for r in page(limit=25, rc=False)] == ["s_queue_a", "s_queue_b"]


def test_progress_counts(pg):
    seed_fix_value(pg, "s_prog_1", "loco_name", values=["X"], confidence="med")
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select fix_id, status, confidence, needs_recheck, n "
                    "from rat.data_fix_progress() "
                    "where fix_id = 'loco_name' and confidence = 'med' and status = 'extracted'")
        rows = cur.fetchall()
    assert len(rows) == 1 and rows[0][4] >= 1


def test_orphans_listed(pg):
    seed_fix_value(pg, "s_orph_1", values=["1"], status="verified")
    seed_fix_value(pg, "s_orph_2", values=["2"], status="extracted")
    seed_catalog(pg, "s_orph_3", "in catalog")
    seed_fix_value(pg, "s_orph_3", values=["3"], status="verified")
    uid = make_admin(pg)
    pg.commit()
    as_user(pg, uid)
    with pg.cursor() as cur:
        cur.execute("select image_no, fix_id, status from rat.data_fix_orphans() "
                    "where image_no like 's_orph_%'")
        assert cur.fetchall() == [("s_orph_1", "loco_number", "verified")]


def test_seeded_fixes(pg):
    with pg.cursor() as cur:
        cur.execute("select id, title, instructions, sort_order from rat.data_fix order by sort_order")
        assert cur.fetchall()[:2] == [("loco_number", "Loco number", "", 1),
                                      ("loco_name", "Loco name", "", 2)]


RPCS = [
    "rat.is_admin()",
    "rat.review_fix_value(text, text, text[], text, timestamptz)",
    "rat.confirm_fix_values(text, jsonb)",
    "rat.data_fix_progress()",
    "rat.data_fix_queue(text, text[], text[], boolean, text, text, text, integer)",
    "rat.data_fix_orphans()",
]


def test_rpc_execute_grants(pg):
    with pg.cursor() as cur:
        for fn in RPCS:
            cur.execute("select has_function_privilege('anon', %s, 'EXECUTE'), "
                        "has_function_privilege('authenticated', %s, 'EXECUTE'), "
                        "p.prosecdef, p.proconfig "
                        "from pg_proc p where p.oid = %s::regprocedure", (fn, fn, fn))
            assert cur.fetchone() == (False, True, True, ["search_path=\"\""]), fn
    as_anon(pg)
    with pytest.raises(psycopg2.errors.InsufficientPrivilege) as exc:
        with pg.cursor() as cur:
            cur.execute("select * from rat.data_fix_progress()")
    assert "permission denied for function" in str(exc.value)


DATA_FIX_TABLES = ("rat.data_fix", "rat.data_fix_value", "rat.data_fix_history", "rat.admin_user")
PRIVS = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")


def test_table_grants(pg):
    """Only the briefed authenticated SELECT survives live's default ACL
    (anon=r, authenticated=r, picaloco_agent=arw on every new rat table)."""
    allowed = {("authenticated", t, "SELECT") for t in DATA_FIX_TABLES[:3]}
    with pg.cursor() as cur:
        for table in DATA_FIX_TABLES:
            cur.execute("select relrowsecurity from pg_class where oid = %s::regclass", (table,))
            assert cur.fetchone()[0] is True, table
            for role in ("anon", "authenticated", "picaloco_agent", "public"):
                for priv in PRIVS:
                    cur.execute("select has_table_privilege(%s, %s, %s)", (role, table, priv))
                    assert cur.fetchone()[0] is ((role, table, priv) in allowed), (role, table, priv)


PRIVATE_TABLES = ("rat.catalog", "rat.catalog_metadata", "rat.catalog_builder",
                  "rat.usage", "rat.picture_metadata")


def test_private_tables_closed_to_anon_and_authenticated(pg):
    with pg.cursor() as cur:
        for table in PRIVATE_TABLES:
            for role in ("anon", "authenticated"):
                cur.execute("select has_table_privilege(%s, %s, 'SELECT')", (role, table))
                assert cur.fetchone()[0] is False, (role, table)
        # The public surface stays readable by both, as on live.
        for table in ("rat.mobile_catalog_view", "rat.collection", "rat.builder"):
            for role in ("anon", "authenticated"):
                cur.execute("select has_table_privilege(%s, %s, 'SELECT')", (role, table))
                assert cur.fetchone()[0] is True, (role, table)


@pytest.mark.parametrize("who", ["anon", "user", "admin"])
def test_select_catalog_denied(pg, who):
    seed_catalog(pg, f"s_privcat_{who}", "private")
    uid = make_admin(pg) if who == "admin" else make_user(pg)
    pg.commit()
    if who == "anon":
        as_anon(pg)
    else:
        as_user(pg, uid)
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        with pg.cursor() as cur:
            cur.execute("select image_no, valuation from rat.catalog limit 1")


def test_ddl_idempotent(pg):
    ts = seed_fix_value(pg, "s_idem_1", values=["42"], suggested_values=["43"],
                        status="corrected", needs_recheck=True)
    with pg.cursor() as cur:
        cur.execute("update rat.data_fix set instructions = 'client rules v1' "
                    "where id = 'loco_number'")
    pg.commit()
    before = _row(pg, "s_idem_1")
    pg.rollback()   # leave no open transaction holding locks during the re-apply

    apply_sql_file(DATA_FIX_SQL)

    with pg.cursor() as cur:
        cur.execute("select id, instructions from rat.data_fix "
                    "where id in ('loco_number', 'loco_name') order by sort_order")
        assert cur.fetchall() == [("loco_number", "client rules v1"), ("loco_name", "")]
    after = _row(pg, "s_idem_1")
    assert after == before and after[4] == ts
