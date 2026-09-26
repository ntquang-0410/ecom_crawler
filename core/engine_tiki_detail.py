"""
DetailTikiEngine: one row per product from Tiki's public product-detail JSON
API (`tiki.vn/api/v2/products/{id}`) -- no login, no browser, no anti-bot
handling needed (see core/tiki_client.py). Every row is monolingual
Vietnamese (title_zh/description_zh stay empty, same shape as the 1688
mono_zh sink) since Tiki has no Chinese side to align against.
"""
from __future__ import annotations

import logging
from typing import List, Tuple

from core.base_crawler_engine import BaseCrawlerEngine
from core.models import ProductRecord, QueueItem
from core.tiki_client import TikiClient
from parsers.parser_tiki import build_record, detail_url

logger = logging.getLogger(__name__)


class DetailTikiEngine(BaseCrawlerEngine):
    def __init__(
        self,
        worker_id: str,
        request_timeout_seconds: int = 60,
        max_fetch_attempts: int = 3,
        min_delay_seconds: float = 1.0,
        max_delay_seconds: float = 2.0,
    ) -> None:
        self.worker_id = worker_id
        self.client = TikiClient(
            timeout_seconds=request_timeout_seconds,
            max_attempts=max_fetch_attempts,
            min_delay_seconds=min_delay_seconds,
            max_delay_seconds=max_delay_seconds,
        )

    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        import asyncio

        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        for item in items:
            spid = item.meta.get("spid")
            url = detail_url(item.meta.get("product_id", ""), spid)
            payload = await asyncio.to_thread(self.client.get_json, url)
            if payload is None:
                failures.append(item)
                continue
            try:
                record = build_record(payload, item.category, self.worker_id, item.meta)
            except Exception:
                logger.exception("Parser raised for %s", url)
                record = None
            if record is None:
                failures.append(item)
                continue
            record.queue_key = item.key
            successes.append(record)

        return successes, failures
