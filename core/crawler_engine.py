"""
CrawlerEngine: asyncio + aiohttp based concurrent page fetcher/parser.

Runs entirely inside a single worker process, bounding concurrency with a
semaphore so one machine doesn't open unlimited sockets or hammer a target
site. Network-level failures are retried a few times independently of the
Hugging Face upload backoff (which is handled separately in
HuggingFaceUploader).
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional, Tuple

import aiohttp

from core.base_crawler_engine import BaseCrawlerEngine
from core.models import ProductRecord, QueueItem
from parsers.base_parser import BaseProductParser

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; EcomCrawlerBot/1.0; "
        "+https://example.com/bot-info)"
    )
}


class CrawlerEngine(BaseCrawlerEngine):
    def __init__(
        self,
        parser: BaseProductParser,
        worker_id: str,
        max_concurrent_requests: int = 20,
        request_timeout_seconds: int = 30,
        max_fetch_attempts: int = 3,
    ) -> None:
        self.parser = parser
        self.worker_id = worker_id
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
        self.max_fetch_attempts = max_fetch_attempts

    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        """Fetch + parse a list of QueueItems concurrently.

        Returns (successful_products, failed_items) so the caller can decide
        what to do with items that could not be crawled (release back to the
        queue).
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        async with aiohttp.ClientSession(
            headers=DEFAULT_HEADERS, timeout=self.timeout
        ) as session:
            tasks = [self._process_item(session, semaphore, item) for item in items]
            # return_exceptions=True is required here: with the default False,
            # a single unexpected exception in ANY item (e.g. odd page encoding,
            # a bug in a custom parser) would propagate out of gather() and
            # crash the whole batch/worker process, discarding every other
            # item's work-in-progress and leaving the whole claimed chunk
            # stuck as `processing` until the lock timeout. Treat per-item
            # exceptions as ordinary failures instead.
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for item, result in zip(items, results):
            if isinstance(result, BaseException):
                # BaseException (not just Exception) so this also narrows
                # out asyncio.CancelledError, which subclasses BaseException
                # directly and would otherwise slip through and be treated
                # as a successfully-parsed product below.
                logger.error("Unhandled error crawling %s: %s", item.url, result)
                failures.append(item)
            elif result is not None:
                successes.append(result)
            else:
                failures.append(item)

        return successes, failures

    async def _process_item(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        item: QueueItem,
    ) -> Optional[ProductRecord]:
        async with semaphore:
            html = await self._fetch_with_retry(session, item.url)
            if html is None:
                return None
            try:
                return self.parser.parse(html, item, self.worker_id)
            except Exception:
                logger.exception("Parser raised while processing %s", item.url)
                return None

    async def _fetch_with_retry(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[str]:
        delay = 1.0
        for attempt in range(1, self.max_fetch_attempts + 1):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    logger.warning(
                        "Non-200 status %s for %s (attempt %s/%s)",
                        response.status,
                        url,
                        attempt,
                        self.max_fetch_attempts,
                    )
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "Fetch error for %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    self.max_fetch_attempts,
                    exc,
                )

            if attempt < self.max_fetch_attempts:
                await asyncio.sleep(delay)
                delay *= 2

        logger.error("Giving up on %s after %s attempts", url, self.max_fetch_attempts)
        return None
