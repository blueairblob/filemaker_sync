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


# Task 3 dry-run tuning: cases taken from live descriptions that the first rules got wrong.
TUNING_PLACES = frozenset({"lapford", "whitwell", "stafford", "port elizabeth"})

TUNING_CASES = [
    # A marked name dropped as a place means the record names 2+ locos: never high.
    ("West Country class 4-6-2 number 34102 Lapford  heads a down Salisbury line train near Woking"
     " being passed by Warship class type 4 B-B diesel hydraulic no. D826 Jupiter on an Exeter"
     " bound express", ("Jupiter",), "med"),
    ("Ex LSWR O2 class 0-4-4T number W17 Seaview stands in Sandown station with the 13.55 Ryde to"
     " Shanklin as sister engine number W26 Whitwell approches with the 14.10 Shanklin to Ryde",
     ("Seaview",), "med"),
    # A plural marker with one number names 2+ locos: never high.
    ("Ex LMS Jubilee class 4-6-0 numbers 45573 Newfoundland (left) and 45616 Malta G.C. (right)"
     " with class 4 Compound no. 41199 in between them", ("Newfoundland",), "med"),
    ("Ex LNER class A4 4-6-2 numbers 60009 Union of South Africa & 60026 Miles Beevor stand in"
     " Perth shed yard", ("Union of South Africa",), "med"),
    # A number that ends its sentence is not followed by a name.
    ("Ex Bombay Baroda and Central India Railway F1 Class (rebuilt as F Class by 1914)  0-6-0"
     " No. 734. This was the first locomotive constructed at the Ajmer Works in 1895", (), "none"),
    # Type words (Colliery, Garratt, Mallet ...) are never names.
    ("Hudswell Clarke 0-6-0T Stafford built in 1889, Works No. 319 at West Cannock No. 5 Colliery"
     " on 5/8/1962.", (), "none"),
    ("Class GEA 4-8-2+2-8-4 Garratt 4041 heads the Port Elizabeth train", (), "none"),
    ("Class 70 2-6-6-0 Mallet number 7012 heads a freight", (), "none"),
    # Dotted initials and short abbreviations stay inside a name.
    ("Ex USATC class S160 2-8-0 number 3257, WD no. 700 Maj-Gen Carl R. Gray Jr. stands in the"
     " yard at Longmoor", ("Maj-Gen Carl R. Gray Jr.",), "high"),
    ("2-4-0T No. 10 G.H. Wood built in 1905 by Beyer Peacock, Works No. 4662 at Douglas on"
     " 10/4/1997.", ("G.H. Wood",), "high"),
    ("4wDM number 39 T.W.Lewis heads a train of tipper wagons", ("T.W.Lewis",), "high"),
    ("Ex LMS Patriot class 4-6-0 number 45536 Private W. Wood V.C. heads a down express near"
     " Elstree", ("Private W. Wood V.C.",), "high"),
    # 'del' joins a name like 'of' does.
    ("0-6-0T number 1 Almodovar del Campo shunts in the yard at Penarroya          (krm 21419)",
     ("Almodovar del Campo",), "high"),
]


@pytest.mark.parametrize("desc,names,conf", TUNING_CASES)
def test_extract_loco_names_tuning(desc, names, conf):
    r = extract_loco_names(desc, TUNING_PLACES)
    assert (r.values, r.confidence) == (names, conf)
