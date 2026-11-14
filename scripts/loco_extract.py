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


@dataclass(frozen=True)
class Extraction:
    values: tuple[str, ...]
    confidence: str  # 'none' | 'low' | 'med' | 'high'


def _clean(value: str) -> str:
    return value.rstrip(".,;:)-/")


def extract_loco_numbers(description: str | None) -> Extraction:
    if not description or not description.strip():
        return Extraction((), "none")
    text = _NUMBER_OF_RE.sub(" ", _WORKS_RE.sub(" ", description))

    values: list[str] = []
    for m in _LIST_RE.finditer(text):
        for tok in [m.group(1)] + _TOKEN_RE.findall(m.group(2) or ""):
            tok = _clean(tok)
            if tok and tok not in values:
                values.append(tok)

    if len(values) == 1:
        v = values[0]
        odd = "/" in v or len(v) > 10 or _ODD_DIGITS_RE.search(v)
        return Extraction((v,), "med" if odd else "high")
    if values:
        return Extraction(tuple(values), "med")

    for cand in _BARE_RE.findall(text):
        if not _YEAR_RE.match(cand):
            return Extraction((cand,), "low")
    return Extraction((), "none")


def description_hash(description: str | None) -> str:
    return hashlib.sha256((description or "").encode("utf-8")).hexdigest()
