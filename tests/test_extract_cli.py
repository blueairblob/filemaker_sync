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
