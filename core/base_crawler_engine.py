"""
BaseCrawlerEngine: common interface for page-fetching engines.

Allows `Worker` to swap between fetch strategies (plain aiohttp vs. a real
browser via Playwright) without any other code changes -- both engines
expose the same `crawl_batch()` contract and an optional `close()` for
releasing long-lived resources (e.g. a browser process).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Tuple

from core.models import ProductRecord, QueueItem


class BaseCrawlerEngine(ABC):
    @abstractmethod
    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        """Fetch + parse a list of QueueItems.

        Returns (successful_products, failed_items) so the caller can decide
        what to do with items that could not be crawled (release back to the
        queue).
        """
        raise NotImplementedError

    async def close(self) -> None:
        """Release any held resources (browser processes, sessions, etc.).

        Default no-op; override in engines that hold long-lived resources
        across multiple `crawl_batch()` calls (e.g. a shared browser).
        """
        return None
