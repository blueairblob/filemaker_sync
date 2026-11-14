"""Every Stage 2 load (GUI Full Sync, "Load to Target", the CLI) refreshes the
loco number/name suggestions for the catalog rows it just loaded, so new photos
enter the review queue and an edited description on a reviewed photo sets
needs_recheck. Best-effort: it runs on its OWN connection and can never fail
or change the outcome of the load. No database: everything here is faked."""
import logging

import pandas as pd
import pytest

import db_dml_loader
import extract_loco_fields
import reject_log


class FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def fakes(monkeypatch):
    seen = {"config_path": None, "profile": None, "conns": [], "runs": [], "crashes": []}

    def load_config(path):
        seen["config_path"] = path
        return {"cfg": True}

    def open_connection(cfg, profile):
        seen["profile"] = profile
        conn = FakeConn()
        seen["conns"].append(conn)
        return conn

    def run(conn, image_nos, dry_run):
        seen["runs"].append((conn, image_nos, dry_run))
        return {"rows": 2 * len(image_nos), "summary": {},
                "apply": {"inserted": 2, "refreshed": 1, "rechecks": 1}}

    def log_crash(source_script, config_path="config.toml", target_profile=None, run_id=None):
        seen["crashes"].append((source_script, target_profile, run_id))

    monkeypatch.setattr(reject_log, "load_config", load_config)
    monkeypatch.setattr(reject_log, "open_connection", open_connection)
    monkeypatch.setattr(reject_log, "log_crash", log_crash)
    monkeypatch.setattr(extract_loco_fields, "run", run)
    return seen


@pytest.fixture
def log(caplog):
    caplog.set_level(logging.INFO, logger="test_loco_refresh")
    return logging.getLogger("test_loco_refresh")


def test_refresh_runs_on_its_own_connection_and_closes_it(fakes, log, caplog):
    out = db_dml_loader.refresh_loco_fields(["b2", "a1", "a1"], "oci", "run-1", log,
                                            config_path="/x/config.toml")
    assert fakes["config_path"] == "/x/config.toml"
    assert fakes["profile"] == "oci"
    assert len(fakes["conns"]) == 1 and fakes["conns"][0].closed
    conn, image_nos, dry_run = fakes["runs"][0]
    assert conn is fakes["conns"][0]
    assert image_nos == ["a1", "b2"] and dry_run is False
    assert out == {"rows": 4, "inserted": 2, "refreshed": 1, "rechecks": 1}
    assert fakes["crashes"] == []
    assert any("Loco fields refreshed" in r.getMessage() for r in caplog.records)


def test_refresh_swallows_failures_logs_crash_and_closes(fakes, log, caplog, monkeypatch):
    def boom(conn, image_nos, dry_run):
        raise RuntimeError("extractor broke")

    monkeypatch.setattr(extract_loco_fields, "run", boom)
    out = db_dml_loader.refresh_loco_fields(["a1"], "oci", "run-2", log)
    assert out is None
    assert fakes["conns"][0].closed
    assert fakes["crashes"] == [("db_dml_loader.py", "oci", "run-2")]
    assert any("extractor broke" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)


def test_refresh_survives_connection_and_crash_log_failures(fakes, log, monkeypatch):
    def no_db(cfg, profile):
        raise OSError("db down")

    def crash_log_broken(*a, **k):
        raise OSError("still down")

    monkeypatch.setattr(reject_log, "open_connection", no_db)
    monkeypatch.setattr(reject_log, "log_crash", crash_log_broken)
    assert db_dml_loader.refresh_loco_fields(["a1"], "oci", "run-3", log) is None


def test_refresh_skips_when_nothing_loaded(fakes, log):
    # An empty list must never reach the extractor (and never a whole-catalog run).
    assert db_dml_loader.refresh_loco_fields([], "oci", "run-4", log) is None
    assert fakes["runs"] == [] and fakes["conns"] == []


def test_loaded_image_nos_strips_and_drops_blanks():
    df = pd.DataFrame({"image_no": [" arc00001 ", None, float("nan"), "", "bpuk2237", "arc00001"]})
    assert db_dml_loader.loaded_image_nos(df) == ["arc00001", "bpuk2237"]
    assert db_dml_loader.loaded_image_nos(pd.DataFrame()) == []


def test_main_calls_the_refresh_after_the_catalog_migrations():
    """main() is too entangled to run without a database; check the wiring."""
    import inspect
    src = inspect.getsource(db_dml_loader.main)
    refresh = src.index("refresh_loco_fields(")
    assert src.index("migrate_picture_metadata(catalog_df)") < refresh
    assert "loaded_image_nos(catalog_df)" in src[refresh - 200:refresh + 200]
    # At function-body level, after the `with session()` block: the load's own
    # work is finished (and its session closed) before the refresh starts.
    line = next(ln for ln in src.splitlines() if "refresh_loco_fields(" in ln)
    assert line.startswith("    ") and not line.startswith("     "), line
