"""Tiki listing/detail JSON -> ProductRecord, in the same row format as the 1688 corpus (monolingual Vietnamese)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from core.models import ProductRecord
from core.textnorm import normalize_text
from parsers.parser_1688_detail import attributes_to_text

# Bucket -> Tiki top-level category ids. fashion/bags/shoes come from the Kaggle Tiki dataset instead.
BUCKETS: Dict[str, List[int]] = {
    "electronics": [1815, 1846, 1789],
    "auto": [8594],
    "food": [4384],
    "home": [1883],
    "beauty": [1520],
    "mother_baby": [2549],
}
# Tiki caps every (category, sort) listing at 50 pages x 40 = 2000; these three return disjoint windows.
SORTS = ("default", "newest", "price,asc")
PAGE_SIZE = 40
MAX_PAGE = 50

_LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings?limit={limit}&category={cat}&page={page}"
_DETAIL_URL = "https://tiki.vn/api/v2/products/{pid}"
_PRODUCT_URL = "https://tiki.vn/{path}"


def listing_url(category_id: int, page: int, sort: str = "default") -> str:
    url = _LISTING_URL.format(limit=PAGE_SIZE, cat=category_id, page=page)
    return url if sort == "default" else f"{url}&sort={sort}"


def detail_url(product_id: str, spid: Optional[str] = None) -> str:
    url = _DETAIL_URL.format(pid=product_id)
    return f"{url}?spid={spid}" if spid else url


def _str_or_none(value: Any) -> Optional[str]:
    # Spec values can carry HTML (`100% gạo trắng<br>`); normalise before joining so no stray space lands before `;`.
    return normalize_text(value) or None


def parse_listing(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One entry per product on a listing page: id, the listed seller offer (spid) and the ad flag."""
    items = []
    for x in payload.get("data") or []:
        if x.get("id") is None:
            continue
        items.append({
            "id": str(x["id"]),
            "spid": _str_or_none(x.get("seller_product_id")),
            "is_ad": bool(x.get("advertisement")),
        })
    return items


def _fields(detail: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Specification attributes followed by SKU options, as [{name, values}] like 1688's attributes_vi."""
    fields: List[Dict[str, Any]] = []
    seen = set()
    for group in detail.get("specifications") or []:
        for attr in group.get("attributes") or []:
            name, value = _str_or_none(attr.get("name")), _str_or_none(attr.get("value"))
            if name and value and name not in seen:
                seen.add(name)
                fields.append({"name": name, "values": [value]})
    for opt in detail.get("configurable_options") or []:
        name = _str_or_none(opt.get("name"))
        values = [label for label in (_str_or_none(v.get("label")) for v in opt.get("values") or []) if label]
        if name and values and name not in seen:
            seen.add(name)
            fields.append({"name": name, "values": values})
    return fields


def build_record(
    detail: Dict[str, Any], category: str, worker_id: str, listing_meta: Dict[str, Any]
) -> Optional[ProductRecord]:
    name = _str_or_none(detail.get("name"))
    if detail.get("id") is None or not name:
        return None
    pid = str(detail["id"])
    fields = _fields(detail)
    # `categories` is often "Root" or the top-level node; the breadcrumb (minus the product itself, id 0) holds the real leaf.
    crumbs = [b for b in detail.get("breadcrumbs") or [] if b.get("category_id")]
    leaf = crumbs[-1] if crumbs else {}
    seller = detail.get("current_seller") or {}
    sold = (detail.get("quantity_sold") or {}).get("value")
    price, list_price = detail.get("price"), detail.get("list_price")
    path = (detail.get("url_path") or f"p{pid}.html").split("?")[0]
    url = _PRODUCT_URL.format(path=path)

    meta = {
        "keyword": None,
        "page": listing_meta["page"],
        "sort": listing_meta["sort"],
        "search_url": listing_meta["search_url"],
        "category_id_tiki_root": str(listing_meta["root_category"]),
        "price": float(price) if price is not None else None,
        "list_price": float(list_price) if list_price is not None else None,
        "currency": "VND",
        "sales": int(sold) if sold is not None else None,
        "province": None,
        "city": None,
        "biz_type": None,
        "shop": _str_or_none(seller.get("name")),
        "is_ad": listing_meta["is_ad"],
        "brand": _str_or_none((detail.get("brand") or {}).get("name")),
        "category_tiki": _str_or_none(leaf.get("name")),
        "category_id_tiki": str(leaf["category_id"]) if leaf.get("category_id") else None,
        "breadcrumb_tiki": [b["name"] for b in crumbs],
        "has_vi": True,
        "attributes_vi": fields,
        "n_attributes_vi": len(fields),
        "description_prose_vi": BeautifulSoup(detail.get("description") or "", "html.parser").get_text(" "),
        # Images inside the prose description itself, matching 1688's
        # description_images (its 详情 block's image count) -- not the
        # product's photo gallery, which is a different field (`images`).
        "description_images": len(BeautifulSoup(detail.get("description") or "", "html.parser").find_all("img")),
        "detail_url": url,
    }
    return ProductRecord(
        product_id=pid,
        title_zh="",
        title_vi=name,
        description_zh="",
        description_vi=attributes_to_text(fields),
        category=category,
        source_site="tiki",
        url=url,
        worker=worker_id,
        meta=meta,
    )
