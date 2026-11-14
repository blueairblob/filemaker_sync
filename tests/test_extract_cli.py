from extract_loco_fields import FixRow, build_rows, summarise
from loco_extract import description_hash


def test_build_rows_two_per_record():
    desc = "Class 4 number 42616 Flying Fox heads"
    rows = build_rows([("a1", desc)], frozenset())
    h = description_hash(desc)
    assert rows == [
        FixRow("a1", "loco_number", ("42616",), "high", h),
        FixRow("a1", "loco_name", ("Flying Fox",), "high", h),
    ]


def test_summarise_counts():
    records = [
        ("a1", "Class 4 number 42616 Flying Fox heads"),              # number high, name high
        ("a2", "Class GEA Garratt 4041 heads the Port Elizabeth train"),  # number low, name none
        ("a3", None),                                                  # none, none
    ]
    assert summarise(build_rows(records, frozenset())) == {
        "loco_number": {"high": 1, "low": 1, "none": 1},
        "loco_name": {"high": 1, "none": 2},
    }


def test_dry_run_keeps_description_per_record(tmp_path, capsys):
    import csv

    from extract_loco_fields import print_report, row_descriptions, write_csv
    records = [("dup", "Class 4 number 42616 Flying Fox heads"), ("dup", "Car number 19 departs")]
    rows = build_rows(records, frozenset())
    descs = row_descriptions(records)
    out = tmp_path / "o.csv"
    write_csv(out, rows, descs)
    got = [(r["values"], r["description"]) for r in csv.DictReader(open(out, encoding="utf-8"))
           if r["fix_id"] == "loco_number"]
    assert got == [("42616", records[0][1]), ("19", records[1][1])]
    print_report(rows, descs, 5)
    shown = capsys.readouterr().out
    assert records[0][1] in shown and records[1][1] in shown


def _stub_cli(monkeypatch):
    """Point extract_loco_fields.main() at fakes: never a real config or DB."""
    import extract_loco_fields
    import reject_log
    calls = {"dry_run": 0, "connect": 0}
    monkeypatch.setattr(reject_log, "load_config", lambda path: {"cfg": path})

    def fake_connect(*a, **k):
        calls["connect"] += 1
        raise AssertionError("dry run must never open a write connection")

    monkeypatch.setattr(reject_log, "open_connection", fake_connect)
    monkeypatch.setattr(extract_loco_fields, "dry_run",
                        lambda *a, **k: calls.__setitem__("dry_run", calls["dry_run"] + 1))
    return extract_loco_fields, calls


def test_cli_defaults_to_dry_run(monkeypatch):
    mod, calls = _stub_cli(monkeypatch)
    assert mod.main([]) == 0
    assert calls == {"dry_run": 1, "connect": 0}


def test_cli_explicit_dry_run_and_apply_are_exclusive(monkeypatch):
    import pytest
    mod, calls = _stub_cli(monkeypatch)
    assert mod.main(["--dry-run"]) == 0
    assert calls == {"dry_run": 1, "connect": 0}
    with pytest.raises(SystemExit):
        mod.main(["--dry-run", "--apply"])
