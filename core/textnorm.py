"""
Text normalisation applied to every record before it is written anywhere.

1688 serves UTF-8 (pages and mtop JSON alike; only the s.1688.com query
string is GBK, and that is a URL, not text). What still needs fixing before
the corpus is usable:

- HTML left in strings: search highlight tags (<font color=red>), entities
  (&amp;, &#39;).
- Unicode form: sellers paste titles from many editors, so the same string
  can arrive composed or decomposed -> NFC.
- Full-width ASCII letters/digits (Ｔ恤, Ｌ码, ２０２６) -> ASCII. Full-width
  CJK punctuation (，。：) is native Chinese typography and is kept.
- Invisible characters (BOM, zero-width space/joiner, control chars) and
  runs of whitespace, including the ideographic space U+3000.
"""
from __future__ import annotations

import html
import re
import unicodedata
from typing import Any

_TAG_RE = re.compile(r"<[^>]+>")
_INVISIBLE_RE = re.compile("[" + "".join(chr(c) for c in (*range(0x00, 0x09), 0x0B, 0x0C, *range(0x0E, 0x20), 0x7F, *range(0x200B, 0x2010), 0x2028, 0x2029, 0xFEFF)) + "]")
_WS_RE = re.compile("[\\s" + chr(0x3000) + "]+")


def _fold_fullwidth_alnum(text: str) -> str:
    out = []
    for ch in text:
        code = ord(ch)
        # Ａ-Ｚ, ａ-ｚ, ０-９ live at U+FF21.., U+FF41.., U+FF10.. (offset 0xFEE0).
        if 0xFF10 <= code <= 0xFF19 or 0xFF21 <= code <= 0xFF3A or 0xFF41 <= code <= 0xFF5A:
            out.append(chr(code - 0xFEE0))
        else:
            out.append(ch)
    return "".join(out)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(_TAG_RE.sub(" ", str(value)))
    text = unicodedata.normalize("NFC", text)
    text = _fold_fullwidth_alnum(text)
    text = _INVISIBLE_RE.sub("", text)
    return _WS_RE.sub(" ", text).strip()


def normalize_value(value: Any) -> Any:
    """Recursively normalise every string inside dicts/lists; leave numbers,
    booleans and None alone. Dict keys are normalised too (attribute names)."""
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {normalize_text(k) if isinstance(k, str) else k: normalize_value(v) for k, v in value.items()}
    return value


# Fields that are identifiers, not text: never touched.
_VERBATIM_META = {"search_url", "detail_url"}


def normalize_record(record: Any) -> Any:
    """In-place normalisation of a ProductRecord (titles, descriptions, meta)."""
    for name in ("title_zh", "title_vi", "description_zh", "description_vi"):
        if hasattr(record, name):
            setattr(record, name, normalize_text(getattr(record, name)))
    record.meta = {
        k: (v if k in _VERBATIM_META else normalize_value(v)) for k, v in (record.meta or {}).items()
    }
    return record
