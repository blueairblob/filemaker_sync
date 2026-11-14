"""Pure extraction of locomotive running numbers from free-text photo descriptions.

Reads text and returns values with a confidence grade; never alters source data.
"""
import hashlib
import re
from dataclasses import dataclass

EXTRACTOR_VERSION = "loco-1"

# Marker words that introduce a running number (shared with the name extractor).
MARKER_WORDS = r"(?:numbers?|No|no|Nos|nos|Nr)[.,]?"
MARKER_RE = re.compile(r"(?<![Ww]orks )\b" + MARKER_WORDS + r"\s+")

# Token alternatives, most specific first.
_SPLIT = r"\d{2,3} \d{3,4}(?:-\d)?(?![A-Za-z0-9])"   # "99 7238", "051 832-4"
_PREFIXED = r"[A-Z]{1,4} \d[\w\.\-/]*"                 # "DT 601"
_PLAIN = r"[A-Z]{0,4}[\-\.]?\d[\w\.\-/]*"              # "E113", "2/3621"
TOKEN = rf"({_SPLIT}|{_PREFIXED}|{_PLAIN})"
_SEP = r"(?:\s*(?:,|&|and)\s*)"

_LIST_RE = re.compile(MARKER_RE.pattern + TOKEN + rf"((?:{_SEP}{TOKEN})*)")
_TOKEN_RE = re.compile(TOKEN)
_WORKS_RE = re.compile(r"\b[Ww]orks\s+[Nn]o\.?\s*\d[\w\.\-/]*|\(\d{2,5}/\d{4}\)")
_NUMBER_OF_RE = re.compile(r"\bnumber of\b", re.I)
_BARE_RE = re.compile(r"(?<![\w/\-\.])((?:[A-Z]{1,3})?\d{3,6})(?![\w/\-])")
_YEAR_RE = re.compile(r"^(18[3-9]\d|19\d\d|20[0-2]\d)$")
_ODD_DIGITS_RE = re.compile(r"\d{7,}")
_PLURAL_RE = re.compile(r"(?:numbers|Nos|nos)\b")


@dataclass(frozen=True)
class Extraction:
    values: tuple[str, ...]
    confidence: str  # 'none' | 'low' | 'med' | 'high'


def _clean(value: str) -> str:
    return value.rstrip(".,;:)-/")


def _normalise(description: str) -> str:
    """Blank out works numbers and the phrase 'number of' (not running numbers)."""
    return _NUMBER_OF_RE.sub(" ", _WORKS_RE.sub(" ", description))


def _marked_spans(text: str) -> list[tuple[int, int]]:
    """Spans of marker-introduced number lists in already-normalised text."""
    return [m.span() for m in _LIST_RE.finditer(text)]


def _plural_single(text: str, start: int, end: int) -> bool:
    """A plural marker with one number ("numbers 60009 Union of South Africa & 60026 ...")
    usually means a second loco went unmarked."""
    if not _PLURAL_RE.match(text, start):
        return False
    m = _LIST_RE.match(text, start, end)
    return not _TOKEN_RE.findall(m.group(2) or "")


def extract_loco_numbers(description: str | None) -> Extraction:
    if not description or not description.strip():
        return Extraction((), "none")
    text = _normalise(description)

    values: list[str] = []
    plural = False
    for start, end in _marked_spans(text):
        plural = plural or _plural_single(text, start, end)
        m = _LIST_RE.match(text, start, end)
        for tok in [m.group(1)] + _TOKEN_RE.findall(m.group(2) or ""):
            tok = _clean(tok)
            if tok and tok not in values:
                values.append(tok)

    if len(values) == 1:
        v = values[0]
        odd = "/" in v or len(v) > 10 or _ODD_DIGITS_RE.search(v)
        return Extraction((v,), "med" if odd or plural else "high")
    if values:
        return Extraction(tuple(values), "med")

    for cand in _BARE_RE.findall(text):
        if not _YEAR_RE.match(cand):
            return Extraction((cand,), "low")
    return Extraction((), "none")


# --- names -----------------------------------------------------------------
STOP_WORDS = frozenset("""stands standing heads heading built is was at on with in near pilots
piloting departs leaves arrives climbs passes shunts runs takes waits approaches crosses hauls
seen pulls enters and being after from to for by""".split())
NOT_NAME = frozenset({"Class", "Railway", "Railways", "Diesel", "Electric", "Tank", "Works",
                      "Shed", "Station", "Junction", "Yard"})
# Marker words are never name words ("No. 60103 ..." must not read "No" as a name).
MARKER_NOT_NAME = frozenset({"No", "Nos", "Nr", "Number", "Numbers"})
CONNECTORS = frozenset({"of", "the", "de", "del", "du", "la"})
# Type and place words: a candidate containing one is a description, not a name
# ("Garratt", "No. 5 Colliery").
TYPE_WORDS = frozenset({"Colliery", "Garratt", "Garratts", "Mallet", "Mallets", "Fireless",
                        "Meyer"})

_WHEEL_RE = re.compile(r"\d-\d+-\d+(?:-\d+)*[A-Z]{0,3}")
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'\u2019\-]*(?![\w])")
# Dotted initials ("G.H.", "T.W.Lewis", "V.C.") or a short abbreviation ("Jr.").
_INITIALS_RE = re.compile(r"(?:[A-Z]\.)+(?:[A-Z][a-z][A-Za-z'\-]*(?![\w.]))?|[A-Z][a-z]\.(?=\s|$)")
_QUOTED_RE = re.compile(r"\"([^\"\n]{1,60})\"|\u2018([^\u2019\n]{1,60})\u2019|'([^'\n]{1,60})'")
_LEVEL = {"low": 1, "med": 2, "high": 3}


def _is_name_word(w: str) -> bool:
    if not w[0].isupper() or w in NOT_NAME or w in MARKER_NOT_NAME:
        return False
    return not (len(w) >= 2 and w.isupper())  # LMS, BR


def _name_at(text: str, pos: int) -> str | None:
    """Parse a run of Title-case words (with connectors) starting at/after pos."""
    words: list[str] = []
    i = pos
    while True:
        j = i
        while j < len(text) and text[j] in " \t":
            j += 1
        if words and j == i:  # no whitespace: punctuation or end follows
            break
        m = _INITIALS_RE.match(text, j)
        if m and m.group().rstrip(".") not in MARKER_NOT_NAME:
            words.append(m.group())
            i = m.end()
            continue
        m = _WORD_RE.match(text, j)
        if not m:
            break
        w = m.group()
        if w.lower() in STOP_WORDS:
            break
        if w in CONNECTORS:
            if not words:
                break
        elif not _is_name_word(w):
            break
        words.append(w)
        i = m.end()
    while words and words[-1] in CONNECTORS:
        words.pop()
    return " ".join(words) if words else None


def extract_loco_names(description: str | None, place_names: frozenset[str]) -> Extraction:
    if not description or not description.strip():
        return Extraction((), "none")
    text = _normalise(description)

    found: dict[str, int] = {}  # name -> best level

    def add(name: str | None, level: str) -> bool:
        """Record a candidate; True when it was dropped as a place name."""
        if not name:
            return False
        if name.lower() in place_names:
            return True
        if not any(w in TYPE_WORDS for w in name.split()):
            found[name] = max(found.get(name, 0), _LEVEL[level])
        return False

    # A marked name dropped as a place is usually a 2nd loco named after a place
    # ("34102 Lapford ... D826 Jupiter"), so what is left cannot be high.
    capped = False
    for start, end in _marked_spans(text):
        if text[start:end].endswith("."):  # "No. 734. This was ..." -- sentence ended
            continue
        if add(_name_at(text, end), "high") or _plural_single(text, start, end):
            capped = True
    for m in _WHEEL_RE.finditer(text):
        hit = _name_at(text, m.end())
        add(hit, "med")
    for m in _QUOTED_RE.finditer(text):
        inner = next(g for g in m.groups() if g is not None).strip()
        hit = _name_at(inner, 0)
        if hit == inner and len(inner.split()) <= 4:
            add(inner, "low")

    if not found:
        return Extraction((), "none")
    if len(found) > 1:
        return Extraction(tuple(found), "med")
    (name, level), = found.items()
    if capped:
        level = min(level, _LEVEL["med"])
    return Extraction((name,), {v: k for k, v in _LEVEL.items()}[level])


def description_hash(description: str | None) -> str:
    return hashlib.sha256((description or "").encode("utf-8")).hexdigest()
