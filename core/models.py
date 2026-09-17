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


@dataclass
class ProductRecord:
    """One product title, ready for the raw JSONL log and the parquet batch.

    Schema follows CLAUDE.md section 4. `meta` holds site-specific extras
    (price, sales, shop, keyword...) so the core columns stay stable across
    sites while nothing useful from the source is thrown away.
    """

    product_id: str
    title_zh: str
    category: str
    source_site: str
    url: str
    worker: str
    # Filled instead of `title_zh` when the site is crawled in Vietnamese
    # (1688's own machine translation; see README "Ngôn ngữ").
    title_vi: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    crawl_time: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    queue_key: Optional[str] = None  # links back to the QueueItem that produced it

    def to_row(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "title_zh": self.title_zh,
            "title_vi": self.title_vi,
            "category": self.category,
            "source_site": self.source_site,
            "url": self.url,
            "crawl_time": self.crawl_time,
            "worker": self.worker,
            "meta": self.meta,
        }
