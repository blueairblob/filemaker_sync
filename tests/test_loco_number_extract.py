import hashlib

import pytest

from loco_extract import description_hash, extract_loco_numbers

CASES = [
 ("YP class 4-6-2 number 2295 heads a passenger train", ("2295",), "high"),
 ("19D Class 4-8-2 No. 3323 built in 1948 by NBL, Works No. 26043 near 6th Reverse", ("3323",), "high"),
 ("Class 19B 4-8-2 numbers 1413 and 1412 head the Mossel Bay train", ("1413", "1412"), "med"),
 ("Former FC Central Buenos Aires 4-6-0 number 130, Urquiza no. 290 is being turned", ("130", "290"), "med"),
 ("DR  2-10-2 T No. 99 7238 heads a passenger working", ("99 7238",), "high"),
 ("DR class 50 2-10-0 number 051 832-4 heads a freight", ("051 832-4",), "high"),
 ("Class 19D 4-8-2 number. 2683 heads the Cape Province Explorer", ("2683",), "high"),
 ("C.P. 2-6-0T No, E113 built by Kessler at Frechas", ("E113",), "high"),
 ("2-8-0 number DT 601 shunts in the yard at Chiayi", ("DT 601",), "high"),
 ("Class 117 unit numbers 51339/51382 heads south", ("51339/51382",), "med"),
 ("CFR 0-6-0 No. 2/3621 built in 1892 by WrN, Works No. 3621 at Sibiu", ("2/3621",), "med"),
 ("Class GEA 4-8-2+2-8-4 Garratt 4041 heads the Port Elizabeth train", ("4041",), "low"),
 ("A Garratt seen in 1975 at Mossel Bay", (), "none"),
 ("An LNER designed K1 class 2-6-0 heads a northbound", (), "none"),
 ("A number of wagons stand in the yard", (), "none"),
 ("OSIJ 2-4-2T built by Motala, Works No. 568 at Mariefred", (), "none"),
 ("K1 class 2-6-0 number 2005 climbs near Eller Beck", ("2005",), "high"),
 ("Car number 19 departs Laxey bound for Douglas", ("19",), "high"),
 ("LMS 4-6-0 number 45212\r\nat Lostock Hall", ("45212",), "high"),
 (None, (), "none"), ("", (), "none"), ("   ", (), "none"),
]


@pytest.mark.parametrize("desc,values,confidence", CASES)
def test_extract_loco_numbers(desc, values, confidence):
    result = extract_loco_numbers(desc)
    assert result.values == values
    assert result.confidence == confidence


def test_description_hash():
    empty = hashlib.sha256(b"").hexdigest()
    assert description_hash(None) == description_hash("") == empty
    assert description_hash("café") == hashlib.sha256("café".encode()).hexdigest()


# Task 3 dry-run tuning: a plural marker with one number usually means a 2nd loco went unmarked.
TUNING_CASES = [
    ("Ex LNER class A4 4-6-2 numbers 60009 Union of South Africa & 60026 Miles Beevor stand in"
     " Perth shed yard", ("60009",), "med"),
    ("Saxon Meyer 0-4-4-0T numbers 99.1568-7 leading and 99.1606-5 head the passenger from"
     " Wolkenstein to Johstadt", ("99.1568-7",), "med"),
]


@pytest.mark.parametrize("desc,values,confidence", TUNING_CASES)
def test_extract_loco_numbers_tuning(desc, values, confidence):
    result = extract_loco_numbers(desc)
    assert (result.values, result.confidence) == (values, confidence)
