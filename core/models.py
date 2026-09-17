"""Shared data models used across the crawler pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class QueueItem:
    """A single unit of work claimed from the Central Queue."""

    key: str  # Firebase node key (unique id in the queue)
    url: str
    category: str = "unknown"
    attempts: int = 0
    # Anything the seeder wants the crawler to carry into the record, e.g.
    # the search-result fields (price, sales, shop...) of a product whose
    # detail page is queued.
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProductRecord:
    """One product, ready for the raw JSONL log and the parquet batch.

    Core columns follow CLAUDE.md section 4; `description_*` is the product's
    attribute table + SKU options rendered as text (1688 has no prose
    description: its 详情 block is images). `meta` holds site-specific extras
    so the core columns stay stable across sites while nothing useful from
    the source is thrown away.
    """

    product_id: str
    title_zh: str
    category: str
    source_site: str
    url: str
    worker: str
    title_vi: str = ""
    description_zh: str = ""
    description_vi: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    crawl_time: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    queue_key: Optional[str] = None  # links back to the QueueItem that produced it

    @property
    def has_vi(self) -> bool:
        return bool(self.title_vi)

    def to_row(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "title_zh": self.title_zh,
            "title_vi": self.title_vi,
            "description_zh": self.description_zh,
            "description_vi": self.description_vi,
            "category": self.category,
            "source_site": self.source_site,
            "url": self.url,
            "crawl_time": self.crawl_time,
            "worker": self.worker,
            "meta": self.meta,
        }
