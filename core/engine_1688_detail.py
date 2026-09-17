"""
Detail1688Engine: one row per product from 1688 detail pages, in Chinese and
-- when 1688 has a translation -- Vietnamese, fetched in the same pass.

Each queue item is one detail URL (`https://detail.1688.com/offer/<id>.html`)
plus the search-result fields the seeder attached (`QueueItem.meta`). The
engine opens the first URL by navigation, then fetches the rest with
`fetch()` from inside that page: same-origin, same cookies, same TLS and
browser fingerprint, but no JavaScript execution per product -- the
server-rendered HTML already carries the attribute JSON the parser reads.
If a fetch comes back as the anti-bot stub, it falls back to a real
navigation for that item.

Bilingual mode (`site_language="zh+vi"`), per product:
  1. pin `oversealanguage=vi`, fetch. If the page is Chinese, the product is
     outside 1688's cross-border pool: that page *is* the Chinese page and
     the row is monolingual (`meta.has_vi=False`). Done in one request.
  2. otherwise pin `oversealanguage=zh`, fetch again, and build the row from
     both pages (attributes aligned by fid).

The long description (详情描述) lives on a CDN with no anti-bot in front; it
is images with, sometimes, seller boilerplate text, and only exists in
Chinese. Whatever text it has is kept as `meta.description_extra_zh`.
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
from parsers.parser_1688_detail import Detail1688Parser, DetailPage, description_from_cdn

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
        site_language: str = "zh+vi",
    ) -> None:
        self.bilingual = site_language == "zh+vi"
        Browser1688Session.__init__(
            self, profile_dir, request_timeout_seconds, human_wait_seconds,
            "vi" if self.bilingual else site_language,
        )
        self.parser = parser
        self.worker_id = worker_id
        self.max_fetch_attempts = max_fetch_attempts
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.fetch_description = fetch_description
        self._http: Optional[aiohttp.ClientSession] = None
        self._consecutive_wrong_language = 0
        self.n_bilingual = 0
        self.n_monolingual = 0

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

    # ------------------------------------------------------------------ #
    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        for item in items:
            await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
            try:
                record = await self._crawl_bilingual(item) if self.bilingual else await self._crawl_mono(item)
            except (WallNotClearedError, AccountLanguageError):
                raise
            except Exception:
                logger.exception("Unexpected error on %s", item.url)
                record = None
            if record is None:
                failures.append(item)
                continue
            if self.fetch_description:
                await self._add_description(record)
            successes.append(record)

        return successes, failures

    async def _crawl_bilingual(self, item: QueueItem) -> Optional[ProductRecord]:
        vi_page = await self._fetch_page(item.url, "vi")
        if vi_page is None:
            return None

        if texts_match_language([vi_page.title], "vi"):
            # Same pacing between the two fetches of one product as between
            # products: two requests a second apart is what trips the slider.
            await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
            zh_page = await self._fetch_page(item.url, "zh")
            if zh_page is None:
                return None
            if not texts_match_language([zh_page.title], "zh"):
                return self._wrong_language(item, "zh", zh_page.title)
            self._consecutive_wrong_language = 0
            self.n_bilingual += 1
            return self.parser.build(zh_page, vi_page, item, self.worker_id)

        if texts_match_language([vi_page.title], "zh"):
            # Not in the cross-border pool: 1688 served the Chinese page.
            self._consecutive_wrong_language = 0
            self.n_monolingual += 1
            logger.info(
                "%s has no Vietnamese version (bilingual so far: %s, monolingual: %s)",
                item.url, self.n_bilingual, self.n_monolingual,
            )
            return self.parser.build(vi_page, None, item, self.worker_id)

        return self._wrong_language(item, "vi/zh", vi_page.title)

    async def _crawl_mono(self, item: QueueItem) -> Optional[ProductRecord]:
        page = await self._fetch_page(item.url, self.site_language)
        if page is None:
            return None
        if not texts_match_language([page.title], "zh"):
            return self._wrong_language(item, "zh", page.title)
        self._consecutive_wrong_language = 0
        self.n_monolingual += 1
        return self.parser.build(page, None, item, self.worker_id)

    def _wrong_language(self, item: QueueItem, expected: str, title: str) -> None:
        self._consecutive_wrong_language += 1
        logger.warning(
            "%s came back in an unexpected language (expected %s, got %r; %s in a row); releasing it",
            item.url, expected, title[:60], self._consecutive_wrong_language,
        )
        if self._consecutive_wrong_language >= 5:
            raise AccountLanguageError(
                "5 products in a row came back in an unexpected language despite the pinned "
                "oversealanguage cookie. Check the account and restart."
            )
        return None

    # ------------------------------------------------------------------ #
    async def _fetch_page(self, url: str, lang: str) -> Optional[DetailPage]:
        html = await self._fetch_with_retry(url, lang)
        if html is None:
            return None
        try:
            return self.parser.parse_page(html, url)
        except Exception:
            logger.exception("Parser raised for %s (%s)", url, lang)
            return None

    async def _fetch_with_retry(self, url: str, lang: str) -> Optional[str]:
        delay = 2.0
        for attempt in range(1, self.max_fetch_attempts + 1):
            page = await self._ensure_session(url)
            html = await self._fetch_in_page(page, url, lang)
            if html is None:
                html = await self._fetch_by_navigation(page, url, lang)
            if html is not None:
                return html
            logger.warning("Detail fetch failed for %s [%s] (attempt %s/%s)", url, lang, attempt, self.max_fetch_attempts)
            if attempt < self.max_fetch_attempts:
                await asyncio.sleep(delay)
                delay *= 2
        logger.error("Giving up on %s [%s] after %s attempts", url, lang, self.max_fetch_attempts)
        return None

    async def _fetch_in_page(self, page: Page, url: str, lang: str) -> Optional[str]:
        await self.force_language(lang)
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

    async def _fetch_by_navigation(self, page: Page, url: str, lang: str) -> Optional[str]:
        self.site_language = lang
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
        detail_url = record.meta.pop("detail_url", None)
        record.meta["description_extra_zh"] = ""
        record.meta["description_images"] = 0
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
        record.meta["description_extra_zh"] = text
        record.meta["description_images"] = n_images
