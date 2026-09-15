"""
Generic, best-effort HTML parser used as a working default/template.

Real deployments should subclass `BaseProductParser` per target site (product
pages differ a lot across e-commerce platforms). This implementation extracts
reasonable fields from common HTML conventions (OpenGraph tags, <h1>, meta
description, and any <table> as key/value specs) so the pipeline is runnable
end-to-end out of the box.
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Dict, Optional

from bs4 import BeautifulSoup

from core.models import ProductRecord, QueueItem
from parsers.base_parser import BaseProductParser

logger = logging.getLogger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")


def clean_text(raw: Optional[str]) -> str:
    """Collapse whitespace/newlines and strip leading/trailing spaces."""
    if not raw:
        return ""
    return _WHITESPACE_RE.sub(" ", raw).strip()


class GenericProductParser(BaseProductParser):
    def parse(self, html: str, item: QueueItem, worker_id: str) -> Optional[ProductRecord]:
        soup = BeautifulSoup(html, "lxml")

        title = self._extract_title(soup)
        description = self._extract_description(soup)
        specs = self._extract_specs(soup)

        if not title:
            logger.warning("No title found for %s, skipping", item.url)
            return None

        product_id = self._extract_product_id(soup, item.url)

        return ProductRecord(
            product_id=product_id,
            title=clean_text(title),
            description=clean_text(description),
            specs=specs,
            category=item.category,
            source_url=item.url,
            worker_id=worker_id,
            queue_key=item.key,
        )

    @staticmethod
    def _extract_title(soup: BeautifulSoup) -> Optional[str]:
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"]
        h1 = soup.find("h1")
        if h1:
            return h1.get_text()
        if soup.title:
            return soup.title.get_text()
        return None

    @staticmethod
    def _extract_description(soup: BeautifulSoup) -> Optional[str]:
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            return og_desc["content"]
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return meta_desc["content"]
        first_p = soup.find("p")
        return first_p.get_text() if first_p else None

    @staticmethod
    def _extract_specs(soup: BeautifulSoup) -> Dict[str, str]:
        specs: Dict[str, str] = {}
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) >= 2:
                    key = clean_text(cells[0].get_text())
                    value = clean_text(cells[1].get_text())
                    if key and value:
                        specs[key] = value
        return specs

    @staticmethod
    def _extract_product_id(soup: BeautifulSoup, url: str) -> str:
        og_id = soup.find("meta", property="product:retailer_item_id")
        if og_id and og_id.get("content"):
            return og_id["content"]
        # Fallback: deterministic hash of the URL so ids are stable/reproducible.
        return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
