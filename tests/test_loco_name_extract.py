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
    # Controller ruling (Task 3 fix round 1): no place filter straight after a marked number,
    # since most such "places" are real loco names; the wheel/quote paths keep the filter.
    ("Tram number 7 Laxey waits at the terminus", ("Laxey",), "high"),
    ("Former Hunslet 0-6-0T Laxey stands in the yard", (), "none"),
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
    # Marked names that are also places are kept (controller ruling); two names grade med.
    ("West Country class 4-6-2 number 34102 Lapford  heads a down Salisbury line train near Woking"
     " being passed by Warship class type 4 B-B diesel hydraulic no. D826 Jupiter on an Exeter"
     " bound express", ("Lapford", "Jupiter"), "med"),
    ("Ex LSWR O2 class 0-4-4T number W17 Seaview stands in Sandown station with the 13.55 Ryde to"
     " Shanklin as sister engine number W26 Whitwell approches with the 14.10 Shanklin to Ryde",
     ("Seaview", "Whitwell"), "med"),
    # '&' joins a name only when the name then ends cleanly (stop word, punctuation or end).
    ("Ex Southern Railway Merchant Navy class 4-6-2 number 35006 Peninsular & Orient S. N. Co"
     " stands in the shed at Toddington", ("Peninsular & Orient S. N. Co",), "high"),
    ("Ex LMSR Coronation class 4-6-2 number 46225 Duchess of Gloucester & English Electric type 4"
     " diesel number D216 stand in the arrival platforms at Euston", ("Duchess of Gloucester",), "high"),
    ("2-4-0T numbers 8 Fenella & 4 Loch head a train to Port Erin climbing away from Douglas near"
     " Ellenbrook Farm", ("Fenella",), "med"),
    # A trailing regnal numeral (1-9 or II-IX) ends a name.
    ("Ex GWR King class 4-6-0 number 6024 King Edward 1 passes Bearley Junction with a return"
     " railtour The William Shakespeare", ("King Edward 1",), "high"),
    ("Ex GWR King class 4-6-0 number 6009 King Charles II  heads an up express near Lapworth",
     ("King Charles II",), "high"),
    ("Ex GWR King class 4-6-0 number 6015 King Richard III departs Birmingham Snow Hill station"
     " with the down Inter City", ("King Richard III",), "high"),
    ("2-6-0 number 377 King Haakon 7 stands in Loughborough shed yard", ("King Haakon 7",), "high"),
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
