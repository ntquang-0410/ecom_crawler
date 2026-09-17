"""
Search1688Engine: pulls 1688.com search results through the site's own
`lib.mtop` JavaScript client, from inside a real, logged-in Chrome.

- The 1688 home page is opened ONCE per worker run. It ships
  `window.lib.mtop`, the client 1688's own frontend uses; it signs requests
  and manages tokens.
- Every queue item (one keyword + page number, encoded in a search URL) is
  served by calling the `getOfferList` API through that client via
  `page.evaluate` -- about 1-2 seconds per 60 products, no navigation. The
  search *page* is what trips Alibaba's punish layer fastest, so avoiding it
  matters as much as the speed.
- An API-level validation challenge carries the CAPTCHA URL; it is opened
  so a person can solve it in the window.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from core.base_crawler_engine import BaseCrawlerEngine
from core.browser_1688 import (
    AccountLanguageError,
    Browser1688Session,
    WallNotClearedError,
    texts_match_language,
)
from core.models import ProductRecord, QueueItem
from parsers.parser_1688_search import Search1688Parser, query_param, search_keyword

logger = logging.getLogger(__name__)

HOME_URL = "https://www.1688.com/"

# Runs inside the page. Mirrors the request the search page itself makes
# (captured from the network tab), minus the page-specific `pageId`.
_SEARCH_JS = """
async ({keywords, page}) => {
  if (!window.lib || !window.lib.mtop) return {error: 'lib.mtop missing'};
  const params = {beginPage: String(page), pageSize: 60, method: 'getOfferList',
                  searchScene: 'pcOfferSearch', verticalProductFlag: 'pcmarket',
                  charset: 'GBK', spm: 'a26352.13672862', keywords};
  try {
    return await window.lib.mtop.request({
      api: 'mtop.relationrecommend.WirelessRecommend.recommend', v: '2.0',
      type: 'GET', dataType: 'jsonp', timeout: 20000,
      data: {appId: 32517, params: JSON.stringify(params)}});
  } catch (e) {
    return {error: String(e && e.message || e), ret: e && e.ret, data: e && e.data};
  }
}
"""


class Search1688Engine(BaseCrawlerEngine, Browser1688Session):
    def __init__(
        self,
        parser: Search1688Parser,
        worker_id: str,
        profile_dir: Path,
        request_timeout_seconds: int = 30,
        max_fetch_attempts: int = 3,
        min_delay_seconds: float = 1.0,
        max_delay_seconds: float = 3.0,
        human_wait_seconds: int = 300,
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
        # keyword -> last page that still had results; later pages are
        # skipped without a request (1688 caps a keyword at ~2000 hits).
        self._last_page: Dict[str, int] = {}
        self._consecutive_blocks = 0
        self._consecutive_timeouts = 0
        self._consecutive_wrong_language = 0

    async def _ensure_session(self) -> Page:
        if self._page is not None and not self._page.is_closed():
            return self._page
        page = await self._launch()
        await self._open_home(page)
        return page

    async def close(self) -> None:
        # BaseCrawlerEngine.close() (a no-op) comes first in the MRO and would
        # otherwise shadow the browser shutdown.
        await Browser1688Session.close(self)

    async def _open_home(self, page: Page) -> None:
        """Load the home page and wait until `lib.mtop` is usable."""
        await self.goto_through_walls(page, HOME_URL)
        deadline = time.monotonic() + self.timeout_ms / 1000
        while time.monotonic() < deadline:
            try:
                if await page.evaluate("!!(window.lib && window.lib.mtop && window.lib.mtop.request)"):
                    return
            except Exception:
                pass  # navigation in progress; try again shortly
            await asyncio.sleep(1.0)
        raise RuntimeError("1688 home page loaded but lib.mtop never became available")

    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        for item in items:
            keyword = search_keyword(item.url)
            page_no = int(query_param(item.url, "beginPage") or 1)
            last = self._last_page.get(keyword)
            if last is not None and page_no > last:
                logger.info("Skipping '%s' page %s: keyword exhausted at page %s", keyword, page_no, last)
                continue

            await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
            payload = await self._search_with_retry(keyword, page_no)
            if payload is None:
                failures.append(item)
                continue
            try:
                result = self.parser.parse_payload(payload, item, self.worker_id)
            except Exception:
                logger.exception("Parser raised for '%s' page %s", keyword, page_no)
                failures.append(item)
                continue

            titles = [r.title_zh or r.title_vi for r in result.records]
            if not texts_match_language(titles, self.site_language):
                # The site ignored the pinned cookie this once; release the
                # page for a retry instead of storing translated titles.
                self._consecutive_wrong_language += 1
                logger.warning(
                    "'%s' page %s came back in the wrong language (%s in a row); releasing it",
                    keyword, page_no, self._consecutive_wrong_language,
                )
                failures.append(item)
                if self._consecutive_wrong_language >= 3:
                    raise AccountLanguageError(
                        f"1688 keeps returning content not in '{self.site_language}' despite the "
                        "pinned oversealanguage cookie. Check the account's language setting "
                        "(scripts/set_1688_language.py) and restart."
                    )
                continue
            self._consecutive_wrong_language = 0

            successes.extend(result.records)
            if not result.has_more:
                self._last_page[keyword] = result.page

        return successes, failures

    async def _search_with_retry(self, keyword: str, page_no: int) -> Optional[Dict[str, Any]]:
        delay = 2.0
        for attempt in range(1, self.max_fetch_attempts + 1):
            page = await self._ensure_session()
            await self.force_language()
            try:
                # lib.mtop's own timeout is not always honoured on a stalled
                # connection, so bound the call from outside as well.
                res = await asyncio.wait_for(
                    page.evaluate(_SEARCH_JS, {"keywords": keyword, "page": page_no}),
                    timeout=self.timeout_ms / 1000,
                )
            except (PlaywrightTimeoutError, asyncio.TimeoutError):
                # A hanging response is how 1688 throttles quietly; three in
                # a row means back off, not hammer.
                self._consecutive_timeouts += 1
                logger.warning(
                    "Timeout calling search API for '%s' p%s (%s in a row)",
                    keyword, page_no, self._consecutive_timeouts,
                )
                if self._consecutive_timeouts >= 3:
                    self._consecutive_timeouts = 0
                    await self._cool_down("API responses hanging")
                # A JSONP call whose reply is a punish page never fires its
                # callback; reloading the home page surfaces that wall for a
                # human and refreshes the API tokens.
                await self._recover_page(page)
                res = None
            except Exception as exc:
                # Typically "execution context destroyed" after a navigation
                # (e.g. the site bounced us to a wall): reload and retry.
                logger.warning("Search API call failed for '%s' p%s: %s", keyword, page_no, exc)
                await self._recover_page(page)
                res = None

            if res is not None:
                ret = str((res.get("ret") or [""])[0])
                if ret.startswith("SUCCESS") and "error" not in res:
                    self._consecutive_blocks = 0
                    self._consecutive_timeouts = 0
                    return res
                logger.warning(
                    "Search API '%s' p%s returned %s (attempt %s/%s)",
                    keyword, page_no, ret or res.get("error"), attempt, self.max_fetch_attempts,
                )
                if "USER_VALIDATE" in ret or "punish" in str(res.get("data") or ""):
                    await self._handle_api_punish(page, res)
                    continue
                if "TOKEN" in ret or "SESSION_EXPIRED" in ret or "NEED_LOGIN" in ret:
                    await self._recover_page(page)

            if attempt < self.max_fetch_attempts:
                await asyncio.sleep(delay)
                delay *= 2

        logger.error("Giving up on '%s' page %s after %s attempts", keyword, page_no, self.max_fetch_attempts)
        return None

    async def _recover_page(self, page: Page) -> None:
        try:
            await self._open_home(page)
        except WallNotClearedError:
            raise
        except Exception as exc:
            logger.warning("Home page reload failed: %s", exc)

    async def _handle_api_punish(self, page: Page, res: Dict[str, Any]) -> None:
        """The API refused us with a validation challenge. Open the CAPTCHA it
        points to so a person can solve it; if none is given, cool down."""
        data = res.get("data") or {}
        punish_url = data.get("url") if isinstance(data, dict) else None
        if punish_url:
            logger.warning("API asks for validation; opening %s", punish_url[:120])
            try:
                await page.goto(punish_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            except Exception as exc:
                logger.warning("Could not open punish page: %s", exc)
            await self.wait_for_human(page)
            await self._recover_page(page)
            return
        await self._cool_down("API blocked without a CAPTCHA URL")
        await self._recover_page(page)

    async def _cool_down(self, reason: str) -> None:
        self._consecutive_blocks += 1
        minutes = min(5 * self._consecutive_blocks, 30)
        logger.warning("%s (%s in a row). Cooling down %s minutes.", reason, self._consecutive_blocks, minutes)
        await asyncio.sleep(minutes * 60)
