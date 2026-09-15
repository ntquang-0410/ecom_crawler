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
    """A cleaned product ready to be packaged into a parquet batch."""

    product_id: str
    title: str
    description: str
    specs: Dict[str, Any] = field(default_factory=dict)
    category: str = "unknown"
    source_url: str = ""
    worker_id: str = ""
    crawled_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    queue_key: Optional[str] = None  # links back to the QueueItem that produced it

    def to_row(self) -> Dict[str, Any]:
        """Flat dict for DataFrame construction. `specs` is JSON-serialized to
        keep the parquet schema stable across products with heterogeneous
        spec keys (arbitrary/variable dict shapes break columnar inference)."""
        import json

        return {
            "product_id": self.product_id,
            "title": self.title,
            "description": self.description,
            "specs": json.dumps(self.specs, ensure_ascii=False),
            "category": self.category,
            "source_url": self.source_url,
            "worker_id": self.worker_id,
            "crawled_at": self.crawled_at,
        }
