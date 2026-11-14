import pytest

from loco_extract import extract_loco_names

PLACES = frozenset({"fort william", "laxey", "corsencon"})

CASES = [
    ("LNER Class A3 4-6-2 No. 60103 Flying Scotsman built in 1923 at Doncaster", ("Flying Scotsman",), "high"),
    ("Ex LNER J36 class 0-6-0 number 673 Maude is seen near Corsencon", ("Maude",), "high"),
    ("GWR 4-4-0 number 3440 City of Truro heads a special", ("City of Truro",), "high"),
    ("Former Hunslet 0-6-0T Doll stands in the yard", ("Doll",), "med"),
    ("Class 37 Co-Co diesel electric number 37264 stands in Fort William shed yard", (), "none"),
    ("YP class 4-6-2 number 2295 heads a passenger train taking water at Lalkua Junction", (), "none"),
    ("0-4-4-0 mallet tank number IVK148, built by Hartmann", (), "none"),
    ("Tram number 7 Laxey waits at the terminus", (), "none"),
    ("The engine 'Prince' waits at Boston Lodge", ("Prince",), "low"),
    (None, (), "none"),
]


@pytest.mark.parametrize("desc,names,conf", CASES)
def test_extract_loco_names(desc, names, conf):
    r = extract_loco_names(desc, PLACES)
    assert (r.values, r.confidence) == (names, conf)
