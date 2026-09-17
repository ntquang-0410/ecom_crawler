"""
Detail1688Engine: collects product attributes (商品属性) and any description
text from 1688 product detail pages.

Each queue item is one detail URL (`https://detail.1688.com/offer/<id>.html`).
The engine opens the first one by navigation, then fetches the rest with
`fetch()` from inside that page: same-origin, same cookies, same TLS and
browser fingerprint, but no JavaScript execution per product -- the
server-rendered HTML already carries the attribute JSON the parser reads.
If a fetch comes back as the anti-bot stub, it falls back to a real
navigation for that item.

The long description (详情描述) lives on a CDN with no anti-bot in front, so
it is fetched from Python directly; it is nearly always images only.
"""
from __future__ import annotations

import asyncio
import logging
import random
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from core.base_crawler_engine import BaseCrawlerEngine
from core.browser_1688 import (
    AccountLanguageError,
    Browser1688Session,
    WallNotClearedError,
    is_wall,
    texts_match_language,
)
from core.models import ProductRecord, QueueItem
from parsers.parser_1688_detail import Detail1688Parser, description_from_cdn

logger = logging.getLogger(__name__)

# A real detail page is ~100 KB+; the anti-bot stub is ~2.5 KB.
_MIN_REAL_HTML = 20_000

_FETCH_JS = """
async (url) => {
  const r = await fetch(url, {credentials: 'include', redirect: 'follow'});
  return {status: r.status, url: r.url, html: await r.text()};
}
"""


class Detail1688Engine(BaseCrawlerEngine, Browser1688Session):
    def __init__(
        self,
        parser: Detail1688Parser,
        worker_id: str,
        profile_dir: Path,
        request_timeout_seconds: int = 30,
        max_fetch_attempts: int = 3,
        min_delay_seconds: float = 1.0,
        max_delay_seconds: float = 3.0,
        human_wait_seconds: int = 300,
        fetch_description: bool = True,
        site_language: str = "zh",
    ) -> None:
        Browser1688Session.__init__(
            self, profile_dir, request_timeout_seconds, human_wait_seconds, site_language
        )
        self.parser = parser
        self.worker_id = worker_id
        self.max_fetch_attempts = max_fetch_attempts
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.fetch_description = fetch_description
        self._http: Optional[aiohttp.ClientSession] = None
        self._consecutive_wrong_language = 0
        self._untranslated = 0

    async def _ensure_session(self, first_url: str) -> Page:
        if self._page is not None and not self._page.is_closed():
            return self._page
        page = await self._launch()
        await self.goto_through_walls(page, first_url)
        return page

    async def close(self) -> None:
        if self._http is not None:
            await self._http.close()
            self._http = None
        await Browser1688Session.close(self)

    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        for item in items:
            await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
            html = await self._fetch_with_retry(item.url)
            if html is None:
                failures.append(item)
                continue
            try:
                record = self.parser.parse(html, item, self.worker_id)
            except Exception:
                logger.exception("Parser raised for %s", item.url)
                failures.append(item)
                continue
            if record is None:
                failures.append(item)
                continue
            title = record.title_zh or record.title_vi
            if not texts_match_language([title], self.site_language):
                if self.site_language != "zh" and texts_match_language([title], "zh"):
                    # 1688 only translates its cross-border pool; a domestic
                    # product simply has no Vietnamese version. Not an error,
                    # nothing to retry: the item is acknowledged without a record.
                    self._untranslated += 1
                    logger.info(
                        "%s has no '%s' version (untranslated so far: %s)",
                        item.url, self.site_language, self._untranslated,
                    )
                    # Keep a stub so the coverage gap is recorded and the
                    # product is not re-seeded on the next run.
                    record.title_vi = ""
                    record.title_zh = ""
                    record.meta = {"untranslated": True, "title_seen": title}
                    successes.append(record)
                else:
                    self._consecutive_wrong_language += 1
                    logger.warning(
                        "%s came back in an unexpected language (%s in a row); releasing it",
                        item.url, self._consecutive_wrong_language,
                    )
                    failures.append(item)
                    if self._consecutive_wrong_language >= 5:
                        raise AccountLanguageError(
                            f"5 products in a row came back neither in '{self.site_language}' nor Chinese "
                            "despite the pinned oversealanguage cookie. Check the account and restart."
                        )
                continue
            self._consecutive_wrong_language = 0
            if self.fetch_description:
                await self._add_description(record)
            successes.append(record)

        return successes, failures

    async def _fetch_with_retry(self, url: str) -> Optional[str]:
        delay = 2.0
        for attempt in range(1, self.max_fetch_attempts + 1):
            page = await self._ensure_session(url)
            html = await self._fetch_in_page(page, url)
            if html is None:
                html = await self._fetch_by_navigation(page, url)
            if html is not None:
                return html
            logger.warning("Detail fetch failed for %s (attempt %s/%s)", url, attempt, self.max_fetch_attempts)
            if attempt < self.max_fetch_attempts:
                await asyncio.sleep(delay)
                delay *= 2
        logger.error("Giving up on %s after %s attempts", url, self.max_fetch_attempts)
        return None

    async def _fetch_in_page(self, page: Page, url: str) -> Optional[str]:
        await self.force_language()
        try:
            res = await asyncio.wait_for(page.evaluate(_FETCH_JS, url), timeout=self.timeout_ms / 1000)
        except (PlaywrightTimeoutError, asyncio.TimeoutError):
            logger.warning("In-page fetch timed out for %s", url)
            return None
        except Exception as exc:
            logger.warning("In-page fetch failed for %s: %s", url, exc)
            return None
        html = res.get("html") or ""
        if res.get("status") == 200 and len(html) >= _MIN_REAL_HTML and not is_wall(res.get("url", ""), html[:20000]):
            return html
        logger.info(
            "In-page fetch returned status=%s len=%s for %s; falling back to navigation",
            res.get("status"), len(html), url,
        )
        return None

    async def _fetch_by_navigation(self, page: Page, url: str) -> Optional[str]:
        try:
            await self.goto_through_walls(page, url)
        except WallNotClearedError:
            raise
        except Exception as exc:
            logger.warning("Navigation failed for %s: %s", url, exc)
            return None
        html = await page.content()
        return html if len(html) >= _MIN_REAL_HTML else None

    async def _add_description(self, record: ProductRecord) -> None:
        detail_url = record.meta.get("detail_url")
        if not detail_url:
            return
        if self._http is None:
            self._http = aiohttp.ClientSession(
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://detail.1688.com/"},
                timeout=aiohttp.ClientTimeout(total=self.timeout_ms / 1000),
            )
        try:
            async with self._http.get(detail_url) as resp:
                if resp.status != 200:
                    return
                body = await resp.text(errors="replace")
        except Exception as exc:
            logger.info("Description CDN fetch failed for %s: %s", record.product_id, exc)
            return
        text, n_images = description_from_cdn(body)
        record.meta["description_text"] = text
        record.meta["description_images"] = n_images
