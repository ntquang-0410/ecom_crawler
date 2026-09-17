"""
Parser for a 1688.com product detail page (detail.1688.com/offer/<id>.html).

What a detail page offers as *text* (the rest -- the 详情描述 block -- is
images, and it is not translated, so it is not part of the corpus):

- `subject`: the seller's title (same as in search results).
- Product attributes (商品属性): 20-50 `name -> values` pairs such as
  主面料成分: 棉, 版型: 宽松型. They live in the server-rendered HTML as JSON
  objects `{"fid": 287, "name": ..., "values": [...]}`; `fid` is 1688's
  attribute-type id and is identical in the Chinese and translated
  templates, so it is the key for aligning the two languages.
- SKU options (`skuProps`): 颜色 / 尺码 with their option names, also keyed
  by `fid`.
- `leafCategoryName` / `leafCategoryId` (always Chinese), `location`
  (河北省保定市), `companyName`, tiered prices.

The same page fetched with the `oversealanguage=vi` cookie carries the same
structure with names/values machine-translated by Alibaba, or -- for
products outside the cross-border pool -- the untouched Chinese page.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from core.models import ProductRecord, QueueItem

logger = logging.getLogger(__name__)

SOURCE_SITE = "1688"

_OFFER_ID_RE = re.compile(r"/offer/(\d+)\.html")
_SUBJECT_RE = re.compile(r'"subject"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CATEGORY_NAME_RE = re.compile(r'"leafCategoryName"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CATEGORY_ID_RE = re.compile(r'"leafCategoryId"\s*:\s*"?(\d+)')
_DETAIL_URL_RE = re.compile(r'"detailUrl"\s*:\s*"((?:[^"\\]|\\.)+)"')
_LOCATION_RE = re.compile(r'"location"\s*:\s*"((?:[^"\\]|\\.)+)"')
_COMPANY_RE = re.compile(r'"companyName"\s*:\s*"((?:[^"\\]|\\.)+)"')
_CPV_RE = re.compile(r'"CpvEnhance"\s*:\s*')
_FID_OBJ_RE = re.compile(r'\{\s*"fid"\s*:\s*\d+\s*,')
_FEATURE_ATTRS_RE = re.compile(r'"featureAttributes"\s*:\s*')
_SKU_PROPS_RE = re.compile(r'"skuProps"\s*:\s*')
_CURRENT_PRICES_RE = re.compile(r'"currentPrices"\s*:\s*')
_WS_RE = re.compile(r"[ \t\r\n　]+")
# 河北省保定市 / 广东省广州市 / 上海市 / 新疆维吾尔自治区乌鲁木齐市
_LOCATION_SPLIT_RE = re.compile(r"^(.+?(?:省|自治区|特别行政区|市))(.*)$")


def _unescape(s: str) -> str:
    try:
        return json.loads(f'"{s}"')
    except ValueError:
        return s


def _clean(s: Any) -> str:
    return _WS_RE.sub(" ", str(s or "")).strip()


def first(pattern: re.Pattern, html: str) -> Optional[str]:
    m = pattern.search(html)
    return _clean(_unescape(m.group(1))) if m else None


def _decode_after(html: str, pos: int) -> Any:
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, pos)
        return obj
    except ValueError:
        return None


def _collect_fid_entry(entry: Any, by_fid: Dict[str, Dict[str, Any]]) -> None:
    if not isinstance(entry, dict) or entry.get("fid") is None:
        return
    name = _clean(entry.get("name"))
    raw_values = entry.get("values") or ([entry["value"]] if entry.get("value") else [])
    values = [_clean(v) for v in raw_values if _clean(v)]
    fid = str(entry["fid"])
    if name and values and fid not in by_fid:
        by_fid[fid] = {"name": name, "values": values}


def extract_attributes_by_fid(html: str) -> Dict[str, Dict[str, Any]]:
    """Ordered `fid -> {"name": ..., "values": [...]}` of the attribute table.

    The domestic template has the complete list under `"featureAttributes"`
    (`{"decisionValues":[..],"fid":287,...,"name":"材质","values":["PU"]}`);
    the translated template has flat `{"fid":..,"name":..,"values":..}`
    objects. Both are read; entries with a fid but no name/values (SKU
    props, column definitions) are skipped.
    """
    by_fid: Dict[str, Dict[str, Any]] = {}
    for m in _FEATURE_ATTRS_RE.finditer(html):
        entries = _decode_after(html, m.end())
        if isinstance(entries, list):
            for entry in entries:
                _collect_fid_entry(entry, by_fid)
    for m in _FID_OBJ_RE.finditer(html):
        _collect_fid_entry(_decode_after(html, m.start()), by_fid)
    return by_fid


def extract_sku_props(html: str) -> Dict[str, Dict[str, Any]]:
    """`fid -> {"name": 颜色, "values": [咖啡色, 棕色, ...]}` from `skuProps`."""
    out: Dict[str, Dict[str, Any]] = {}
    for m in _SKU_PROPS_RE.finditer(html):
        props = _decode_after(html, m.end())
        if not isinstance(props, list):
            continue
        for prop in props:
            if not isinstance(prop, dict):
                continue
            name = _clean(prop.get("prop"))
            values = [_clean(v.get("name")) for v in prop.get("value") or [] if isinstance(v, dict)]
            values = [v for v in values if v]
            fid = str(prop.get("fid") or f"sku:{name}")
            if name and values and fid not in out:
                out[fid] = {"name": name, "values": values}
        if out:
            break
    return out


def extract_price(html: str) -> Optional[float]:
    """Lowest tier of `currentPrices` (`[{"beginAmount":1,"price":"18.50"}, ...]`)."""
    for m in _CURRENT_PRICES_RE.finditer(html):
        tiers = _decode_after(html, m.end())
        if not isinstance(tiers, list):
            continue
        prices = []
        for tier in tiers:
            if isinstance(tier, dict) and tier.get("price") not in (None, ""):
                try:
                    prices.append(float(str(tier["price"]).replace(",", "")))
                except ValueError:
                    pass
        if prices:
            return min(prices)
    return None


def split_location(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not location:
        return None, None
    m = _LOCATION_SPLIT_RE.match(location)
    if not m:
        return location, None
    province, city = m.group(1), m.group(2).strip()
    return province, (city or None)


def attributes_to_text(entries: List[Dict[str, Any]]) -> str:
    """`主面料成分: 棉; 颜色: 黑色, 白色` -- the form the MT stage consumes."""
    return "; ".join(f"{e['name']}: {', '.join(e['values'])}" for e in entries)


@dataclass
class DetailPage:
    """Everything text-like on one detail page, in whatever language the
    page was served in."""

    product_id: str
    title: str
    attributes: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # fid -> {name, values}
    sku: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # fid -> {name, values}
    category_1688: Optional[str] = None
    category_id_1688: Optional[str] = None
    location: Optional[str] = None
    company: Optional[str] = None
    price: Optional[float] = None
    detail_url: Optional[str] = None

    def fields(self) -> Dict[str, Dict[str, Any]]:
        """Attributes followed by SKU options, one ordered map keyed by fid."""
        merged = dict(self.attributes)
        for fid, entry in self.sku.items():
            merged.setdefault(fid, entry)
        return merged

    def description(self, fids: Optional[List[str]] = None) -> str:
        fields = self.fields()
        order = fids if fids is not None else list(fields)
        return attributes_to_text([fields[f] for f in order if f in fields])


def parse_detail_page(html: str, url: str) -> Optional[DetailPage]:
    offer_match = _OFFER_ID_RE.search(url)
    product_id = offer_match.group(1) if offer_match else ""
    title = first(_SUBJECT_RE, html)
    if not product_id or not title:
        logger.warning("Detail page for %s has no subject (stub/blocked page?)", url)
        return None
    return DetailPage(
        product_id=product_id,
        title=title,
        attributes=extract_attributes_by_fid(html),
        sku=extract_sku_props(html),
        category_1688=first(_CATEGORY_NAME_RE, html),
        category_id_1688=first(_CATEGORY_ID_RE, html),
        location=first(_LOCATION_RE, html),
        company=first(_COMPANY_RE, html),
        price=extract_price(html),
        detail_url=first(_DETAIL_URL_RE, html),
    )


def build_record(
    zh: DetailPage,
    vi: Optional[DetailPage],
    item: QueueItem,
    worker_id: str,
) -> ProductRecord:
    """One row from the Chinese page and, when the product is in 1688's
    cross-border pool, its Vietnamese rendering.

    `description_zh`/`description_vi` are built over the *same* attribute
    ids in the *same* order (the Chinese page's), so the two strings are
    sentence-aligned by construction. Attributes present on only one side
    are still kept in `meta.attributes_zh` / `meta.attributes_vi`.
    """
    search_meta = dict(item.meta or {})
    province, city = split_location(zh.location)

    if vi is not None:
        shared = [fid for fid in zh.fields() if fid in vi.fields()]
        description_zh = zh.description(shared)
        description_vi = vi.description(shared)
    else:
        description_zh = zh.description()
        description_vi = ""

    meta: Dict[str, Any] = {
        "keyword": search_meta.get("keyword"),
        "page": search_meta.get("page"),
        "search_url": search_meta.get("search_url"),
        "price": search_meta.get("price") if search_meta.get("price") is not None else zh.price,
        "sales": search_meta.get("sales"),
        "province": search_meta.get("province") or province,
        "city": search_meta.get("city") or city,
        "biz_type": search_meta.get("biz_type"),
        "shop": search_meta.get("shop") or zh.company,
        "is_ad": search_meta.get("is_ad"),
        "category_1688": zh.category_1688,
        "category_id_1688": zh.category_id_1688,
        "has_vi": vi is not None,
        "attributes_zh": list(zh.fields().values()),
        "attributes_vi": list(vi.fields().values()) if vi is not None else [],
        "n_attributes_zh": len(zh.fields()),
        "n_attributes_vi": len(vi.fields()) if vi is not None else 0,
        "n_attributes_aligned": len(shared) if vi is not None else 0,
        "detail_url": zh.detail_url,
    }
    return ProductRecord(
        product_id=zh.product_id,
        title_zh=zh.title,
        title_vi=vi.title if vi is not None else "",
        description_zh=description_zh,
        description_vi=description_vi,
        category=item.category,
        source_site=SOURCE_SITE,
        url=f"https://detail.1688.com/offer/{zh.product_id}.html",
        worker=worker_id,
        meta=meta,
        queue_key=item.key,
    )


class Detail1688Parser:
    """Thin object so main.py can keep constructing `Detail1688Parser(lang=...)`."""

    def __init__(self, lang: str = "zh") -> None:
        self.lang = lang

    @staticmethod
    def parse_page(html: str, url: str) -> Optional[DetailPage]:
        return parse_detail_page(html, url)

    @staticmethod
    def build(zh: DetailPage, vi: Optional[DetailPage], item: QueueItem, worker_id: str) -> ProductRecord:
        return build_record(zh, vi, item, worker_id)


def description_from_cdn(body: str) -> Tuple[str, int]:
    """Text and image count of the 详情描述 CDN payload
    (`var desc='<html>'` or `{"content":"<html>"}`). Only the Chinese page
    has one; it is mostly images and sometimes seller boilerplate, so the
    engine stores whatever text it finds as `meta.description_extra_zh`."""
    from bs4 import BeautifulSoup

    m = re.search(r'"content"\s*:\s*"(.*)"\s*}\s*;?\s*$', body, re.S)
    if m:
        content = _unescape(m.group(1))
    else:
        m = re.search(r"^\s*var\s+\w+\s*=\s*'(.*)'\s*;?\s*$", body, re.S)
        content = m.group(1).replace("\\'", "'").replace("\\\\", "\\") if m else body
    soup = BeautifulSoup(content, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = _WS_RE.sub(" ", soup.get_text(" ", strip=True)).strip()
    # The template embeds its own JSON config; that is not seller text.
    text = re.sub(r"\{\s*\"[^}]{0,300}\}", "", text)
    text = re.sub(r"(?:\\\s*)+", " ", text)  # escaped line continuations
    text = _WS_RE.sub(" ", text).strip()
    return text, len(soup.find_all("img"))
