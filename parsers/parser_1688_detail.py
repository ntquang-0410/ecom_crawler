"""
Parser for a 1688.com product detail page (detail.1688.com/offer/<id>.html).

What a detail page offers as *text*:
- `subject`: the seller's title (same as in search results).
- Product attributes (商品属性): 20-50 `name -> values` pairs such as
  主面料成分: 棉, 版型: 宽松型, 颜色: 黑色,白色. They live in the server-rendered
  HTML as JSON (`"CpvEnhance":{"decisionCpv":[...],"normalCpv":[...]}`), so no
  JavaScript has to run.
- `categoryName` / `categoryId`: 1688's own leaf category, e.g. 男式T恤.
- `detailUrl`: CDN address of the long description (详情描述). That block is
  almost always a stack of images with next to no text, so the engine only
  keeps whatever text it finds there.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from core.models import ProductRecord, QueueItem

logger = logging.getLogger(__name__)

SOURCE_SITE = "1688"

_OFFER_ID_RE = re.compile(r"/offer/(\d+)\.html")
_SUBJECT_RE = re.compile(r'"subject"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CATEGORY_NAME_RE = re.compile(r'"leafCategoryName"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CATEGORY_ID_RE = re.compile(r'"leafCategoryId"\s*:\s*"?(\d+)')
_DETAIL_URL_RE = re.compile(r'"detailUrl"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CPV_RE = re.compile(r'"CpvEnhance"\s*:\s*')
_FID_LIST_RE = re.compile(r'\[\s*\{\s*"fid"\s*:')
_FID_OBJ_RE = re.compile(r'\{\s*"fid"\s*:\s*\d+\s*,')
_WS_RE = re.compile(r"[ \t\r\n　]+")


def _unescape(s: str) -> str:
    try:
        return json.loads(f'"{s}"')
    except ValueError:
        return s


def _json_after(html: str, match: re.Match) -> Optional[Dict[str, Any]]:
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, match.end())
        return obj if isinstance(obj, dict) else None
    except ValueError:
        return None


def _collect(entries: Any, attrs: Dict[str, List[str]]) -> None:
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        name = _WS_RE.sub(" ", str(entry.get("name") or "")).strip()
        raw_values = entry.get("values") or ([entry["value"]] if entry.get("value") else [])
        values = [_WS_RE.sub(" ", str(v)).strip() for v in raw_values if str(v).strip()]
        if name and values and name not in attrs:
            attrs[name] = values


def extract_attributes(html: str) -> Dict[str, List[str]]:
    """Ordered attribute name -> list of values.

    The domestic page template carries them as
    `"CpvEnhance":{"decisionCpv":[...],"normalCpv":[...]}`; the overseas
    (translated) template only has the flat list
    `[{"fid":287,"name":"Chất liệu","values":["da thật"],...}, ...]`.
    """
    attrs: Dict[str, List[str]] = {}
    for m in _CPV_RE.finditer(html):
        cpv = _json_after(html, m)
        if cpv:
            _collect(cpv.get("decisionCpv"), attrs)
            _collect(cpv.get("normalCpv"), attrs)
        if attrs:
            return attrs

    for m in _FID_LIST_RE.finditer(html):
        try:
            entries, _ = json.JSONDecoder().raw_decode(html, m.start())
        except ValueError:
            continue
        if isinstance(entries, list) and any(isinstance(e, dict) and "fid" in e for e in entries):
            _collect(entries, attrs)
        if len(attrs) >= 5:
            break
    return attrs


def extract_attributes_by_fid(html: str) -> Dict[str, Dict[str, Any]]:
    """fid -> {"name": ..., "values": [...]} from the flat attribute list.

    `fid` is 1688's attribute-type id and is identical in the Chinese and
    translated templates, so it is the only reliable key for aligning a
    product's attributes across languages (their order and subset differ).
    """
    by_fid: Dict[str, Dict[str, Any]] = {}
    decoder = json.JSONDecoder()
    # Objects like {"fid":287,...,"name":"Chất liệu","values":["da thật"]}; the
    # page also has fid objects for SKU props, which lack name/values.
    for m in _FID_OBJ_RE.finditer(html):
        try:
            entry, _ = decoder.raw_decode(html, m.start())
        except ValueError:
            continue
        if not isinstance(entry, dict):
            continue
        name = _WS_RE.sub(" ", str(entry.get("name") or "")).strip()
        raw_values = entry.get("values") or ([entry["value"]] if entry.get("value") else [])
        values = [_WS_RE.sub(" ", str(v)).strip() for v in raw_values if str(v).strip()]
        fid = str(entry.get("fid"))
        if name and values and fid not in by_fid:
            by_fid[fid] = {"name": name, "values": values}
    return by_fid


def attributes_to_text(attrs: Dict[str, List[str]]) -> str:
    """`主面料成分: 棉; 颜色: 黑色, 白色` -- the form the MT stage will consume."""
    return "; ".join(f"{k}: {', '.join(v)}" for k, v in attrs.items())


def first(pattern: re.Pattern, html: str) -> Optional[str]:
    m = pattern.search(html)
    return _unescape(m.group(1)) if m else None


class Detail1688Parser:
    def __init__(self, lang: str = "zh") -> None:
        self.lang = lang

    def parse(self, html: str, item: QueueItem, worker_id: str) -> Optional[ProductRecord]:
        offer_match = _OFFER_ID_RE.search(item.url)
        product_id = offer_match.group(1) if offer_match else ""
        title = first(_SUBJECT_RE, html)
        attrs = extract_attributes(html)

        if not product_id or not title:
            logger.warning("Detail page for %s has no subject (stub/blocked page?)", item.url)
            return None

        title = _WS_RE.sub(" ", title).strip()
        return ProductRecord(
            product_id=product_id,
            title_zh=title if self.lang == "zh" else "",
            title_vi=title if self.lang == "vi" else "",
            category=item.category,
            source_site=SOURCE_SITE,
            url=f"https://detail.1688.com/offer/{product_id}.html",
            worker=worker_id,
            meta={
                "attributes": attrs,
                "attributes_by_fid": extract_attributes_by_fid(html),
                "attributes_text": attributes_to_text(attrs),
                "category_1688": first(_CATEGORY_NAME_RE, html),
                "category_id_1688": first(_CATEGORY_ID_RE, html),
                "detail_url": first(_DETAIL_URL_RE, html),
                "description_text": "",
                "description_images": 0,
            },
            queue_key=item.key,
        )


def description_from_cdn(body: str) -> Tuple[str, int]:
    """Text and image count of a `var offer_details={"content":"<html>"}`
    CDN payload. Requires bs4 only here so the parser stays importable."""
    from bs4 import BeautifulSoup

    m = re.search(r'"content"\s*:\s*"(.*)"\s*}\s*;?\s*$', body, re.S)
    content = _unescape(m.group(1)) if m else body
    soup = BeautifulSoup(content, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = _WS_RE.sub(" ", soup.get_text(" ", strip=True)).strip()
    # The template embeds its own JSON config; that is not seller text.
    text = re.sub(r"\{\s*\"[^}]{0,300}\}", "", text).strip()
    return text, len(soup.find_all("img"))
