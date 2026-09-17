"""
Parser for the 1688.com search-results API.

The search page (s.1688.com/selloffer/offer_search.htm) renders its product
list from an XHR to `mtop.relationrecommend.wirelessrecommend.recommend`
whose JSONP body looks like:

    mtopjsonpreqTppId_32517_getOfferList2({"api": ..., "data": {"data": {
        "OFFER": {"found": 2000, "hasMore": true, "items": [
            {"cellType": "smart_ui_offer", "data": {"offerId": "...",
             "title": "中德BYB22000短袖<font color=red>T恤</font>...", ...}}, ...]}}}})

Each page carries 60 offers with the seller's original Chinese title, which
is exactly the text we want (no machine translation layer, unlike the
detail page served to foreign IPs).
"""
from __future__ import annotations

import html
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from core.models import ProductRecord, QueueItem

logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\n　]+")

SOURCE_SITE = "1688"


def clean_title(raw: str) -> str:
    """Strip the search highlight markup (<font color=red>), unescape HTML
    entities and collapse whitespace. Emoji / decorative characters are kept
    on purpose (CLAUDE.md section 6)."""
    text = html.unescape(_TAG_RE.sub("", raw or ""))
    return _WS_RE.sub(" ", text).strip()


def decode_jsonp(body: str) -> Dict[str, Any]:
    """Return the JSON payload wrapped by a JSONP callback (or plain JSON)."""
    start = body.find("(")
    raw = body[start + 1 :] if start != -1 and not body.lstrip().startswith("{") else body
    obj, _ = json.JSONDecoder().raw_decode(raw.lstrip())
    return obj


def query_param(url: str, name: str, encoding: str = "utf-8") -> Optional[str]:
    values = parse_qs(urlparse(url).query, encoding=encoding, errors="replace").get(name)
    return values[0] if values else None


def search_keyword(url: str) -> str:
    # s.1688.com query strings are GBK-encoded (see scripts/seed_1688_keywords.py).
    return query_param(url, "keywords", encoding="gbk") or ""


@dataclass
class SearchPage:
    records: List[ProductRecord]
    keyword: str
    page: int
    found: int
    has_more: bool


class Search1688Parser:
    def __init__(self, lang: str = "zh") -> None:
        self.lang = lang

    def parse(self, body: str, item: QueueItem, worker_id: str) -> SearchPage:
        return self.parse_payload(decode_jsonp(body), item, worker_id)

    def parse_payload(self, payload: Dict[str, Any], item: QueueItem, worker_id: str) -> SearchPage:
        ret = payload.get("ret") or []
        if ret and not str(ret[0]).startswith("SUCCESS"):
            raise ValueError(f"mtop returned {ret}")

        offer = ((payload.get("data") or {}).get("data") or {}).get("OFFER") or {}
        items = offer.get("items") or []

        keyword = search_keyword(item.url)
        page = query_param(item.url, "beginPage") or "1"

        records: List[ProductRecord] = []
        for cell in items:
            data = cell.get("data") or {}
            offer_id = str(data.get("offerId") or "").strip()
            title = clean_title(data.get("title") or "")
            if not offer_id or not title:
                continue
            records.append(
                ProductRecord(
                    product_id=offer_id,
                    title_zh=title if self.lang == "zh" else "",
                    title_vi=title if self.lang == "vi" else "",
                    category=item.category,
                    source_site=SOURCE_SITE,
                    url=f"https://detail.1688.com/offer/{offer_id}.html",
                    worker=worker_id,
                    meta={
                        "keyword": keyword,
                        "page": int(page),
                        "price": (data.get("priceInfo") or {}).get("price"),
                        "sales": (data.get("afterPrice") or {}).get("text"),
                        "province": data.get("province"),
                        "city": data.get("city"),
                        "biz_type": data.get("bizType"),
                        "shop": data.get("loginId"),
                        "is_ad": data.get("isP4P") == "true" or data.get("type") == "bid",
                        "search_url": item.url,
                    },
                    queue_key=item.key,
                )
            )

        found = int(offer.get("found") or 0)
        has_more = bool(offer.get("hasMore")) and bool(records)
        logger.info(
            "1688 search '%s' page %s: %s offers (found=%s, hasMore=%s)",
            keyword,
            page,
            len(records),
            found,
            has_more,
        )
        return SearchPage(
            records=records, keyword=keyword, page=int(page), found=found, has_more=has_more
        )
