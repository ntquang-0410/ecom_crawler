"""
PlaywrightCrawlerEngine: fetches pages using a real headless Chromium
browser (via Playwright) instead of a plain HTTP client.

Use this when the target site requires JavaScript rendering or blocks
simple HTTP clients through bot-detection/fingerprinting (e.g. returns a
CAPTCHA/"verify you're human" page instead of real content -- this is
exactly what Alibaba does to `aiohttp`-based requests).

IMPORTANT -- read before relying on this for a blocked site:
- This engine only applies common, publicly documented stealth tweaks
  (hiding `navigator.webdriver`, a realistic user-agent/viewport, randomized
  per-request delays). It does NOT guarantee bypassing sophisticated
  anti-bot systems (Akamai/PerimeterX/Cloudflare-class), which can still
  fingerprint headless browsers via canvas/WebGL/TLS signatures, or may
  present a real CAPTCHA that requires manual solving or a paid solver
  service -- neither of which this engine attempts.
- Each concurrent request is a full browser tab: far more CPU/RAM per
  request than `CrawlerEngine` (aiohttp), so `max_concurrent_requests`
  should be much lower (a handful, not dozens).
- Requires the Playwright package AND its browser binary to be installed
  once per machine: `playwright install chromium` (see README).
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import List, Optional, Tuple

from playwright.async_api import (
    Browser,
    BrowserContext,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from core.base_crawler_engine import BaseCrawlerEngine
from core.models import ProductRecord, QueueItem
from parsers.base_parser import BaseProductParser

logger = logging.getLogger(__name__)

# A realistic, current desktop Chrome UA. Playwright's default UA includes
# "HeadlessChrome", which is itself a trivially-checked bot signal, so this
# must be overridden.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Patches the most commonly checked automation signals before any page
# script runs. This is a well-known, publicly documented baseline (similar
# to what the `playwright-stealth` package does) -- not a guarantee against
# sites with dedicated bot-detection vendors.
_STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.chrome = { runtime: {} };
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
"""


class PlaywrightCrawlerEngine(BaseCrawlerEngine):
    def __init__(
        self,
        parser: BaseProductParser,
        worker_id: str,
        max_concurrent_requests: int = 4,
        request_timeout_seconds: int = 30,
        max_fetch_attempts: int = 3,
        headless: bool = True,
        min_delay_seconds: float = 1.0,
        max_delay_seconds: float = 3.0,
    ) -> None:
        self.parser = parser
        self.worker_id = worker_id
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout_ms = request_timeout_seconds * 1000
        self.max_fetch_attempts = max_fetch_attempts
        self.headless = headless
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._start_lock = asyncio.Lock()

    async def _ensure_started(self) -> BrowserContext:
        """Lazily launch a single shared browser/context, reused across
        every `crawl_batch()` call for this engine's lifetime (launching a
        fresh browser per batch would cost ~1-2s of pure overhead each
        time). Guarded by a lock since multiple concurrent callers could
        otherwise race to launch duplicate browsers.
        """
        async with self._start_lock:
            if self._context is not None:
                return self._context

            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            self._context = await self._browser.new_context(
                user_agent=DEFAULT_USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            await self._context.add_init_script(_STEALTH_INIT_SCRIPT)
            return self._context

    async def crawl_batch(
        self, items: List[QueueItem]
    ) -> Tuple[List[ProductRecord], List[QueueItem]]:
        context = await self._ensure_started()
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        successes: List[ProductRecord] = []
        failures: List[QueueItem] = []

        tasks = [self._process_item(context, semaphore, item) for item in items]
        # return_exceptions=True: one item raising must not take down every
        # other in-flight page/result in the batch (same rationale as
        # CrawlerEngine's aiohttp implementation).
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for item, result in zip(items, results):
            if isinstance(result, BaseException):
                logger.error("Unhandled error crawling %s: %s", item.url, result)
                failures.append(item)
            elif result is not None:
                successes.append(result)
            else:
                failures.append(item)

        return successes, failures

    async def _process_item(
        self,
        context: BrowserContext,
        semaphore: asyncio.Semaphore,
        item: QueueItem,
    ) -> Optional[ProductRecord]:
        async with semaphore:
            # Randomized delay before each request so traffic looks less
            # like a tight automated loop, reducing (not eliminating) the
            # chance of a rate-limit/IP ban.
            await asyncio.sleep(
                random.uniform(self.min_delay_seconds, self.max_delay_seconds)
            )
            html = await self._fetch_with_retry(context, item.url)
            if html is None:
                return None
            try:
                return self.parser.parse(html, item, self.worker_id)
            except Exception:
                logger.exception("Parser raised while processing %s", item.url)
                return None

    async def _fetch_with_retry(
        self, context: BrowserContext, url: str
    ) -> Optional[str]:
        delay = 1.0
        for attempt in range(1, self.max_fetch_attempts + 1):
            page = await context.new_page()
            try:
                response = await page.goto(
                    url, timeout=self.timeout_ms, wait_until="domcontentloaded"
                )
                if response is not None and response.status == 200:
                    # Give lazy-loaded/XHR-driven content a brief moment to
                    # render before reading the final DOM.
                    await page.wait_for_timeout(1500)
                    return await page.content()
                status = response.status if response is not None else "no-response"
                logger.warning(
                    "Non-200 status %s for %s (attempt %s/%s)",
                    status,
                    url,
                    attempt,
                    self.max_fetch_attempts,
                )
            except PlaywrightTimeoutError as exc:
                logger.warning(
                    "Timeout fetching %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    self.max_fetch_attempts,
                    exc,
                )
            except Exception as exc:
                logger.warning(
                    "Fetch error for %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    self.max_fetch_attempts,
                    exc,
                )
            finally:
                await page.close()

            if attempt < self.max_fetch_attempts:
                await asyncio.sleep(delay)
                delay *= 2

        logger.error("Giving up on %s after %s attempts", url, self.max_fetch_attempts)
        return None

    async def close(self) -> None:
        """Release browser resources. `Worker.run()` calls this once the
        crawl loop ends (drained, or after a fatal error), regardless of how
        it exits, so the Chromium process doesn't linger."""
        if self._context is not None:
            await self._context.close()
            self._context = None
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
