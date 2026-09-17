"""
Shared browser session for the 1688 engines: a real Chrome with a persistent,
logged-in profile, plus the anti-bot wall handling both engines need.

Why a real Chrome with a persistent profile: the bundled headless Chromium
gets a slider CAPTCHA in every configuration we tried, while a real profile
passes once a person has solved the slider a single time and logged in.
Walls (slider CAPTCHA, login page) are never "solved" by code: the engine
logs loudly and waits for someone to deal with them in the visible window.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Optional, Sequence

from playwright.async_api import BrowserContext, Page, Playwright, async_playwright

logger = logging.getLogger(__name__)

_WALL_URL_MARKERS = ("login.taobao.com", "login.1688.com", "/punish", "_____tmd_____")
_WALL_HTML_MARKERS = ("sd/punish", "nocaptcha", 'id="nocaptcha"')


class WallNotClearedError(RuntimeError):
    """Nobody cleared the CAPTCHA/login wall in time; the whole worker should
    stop rather than burn through the queue failing every item."""


class AccountLanguageError(RuntimeError):
    """1688 keeps serving content in a language other than the one requested."""


_CJK_RE = re.compile(r"[一-鿿]")
_VI_RE = re.compile(r"[ăâđêôơưàáảãạèéẻẽẹìíỉĩịòóỏõọùúủũụỳýỷỹỵ]", re.I)


def texts_match_language(texts: Sequence[str], lang: str) -> bool:
    """True when a batch of titles is (mostly) in `lang`: 'zh' or 'vi'."""
    texts = [t for t in texts if t]
    if not texts:
        return True
    if lang == "vi":
        return sum(bool(_VI_RE.search(t)) for t in texts) >= 0.5 * len(texts)
    return sum(bool(_CJK_RE.search(t)) for t in texts) >= 0.8 * len(texts)


def is_wall(url: str, html_snippet: str) -> bool:
    return any(m in url for m in _WALL_URL_MARKERS) or any(
        m in html_snippet for m in _WALL_HTML_MARKERS
    )


class Browser1688Session:
    def __init__(
        self,
        profile_dir: Path,
        request_timeout_seconds: int,
        human_wait_seconds: int,
        site_language: str = "zh",
    ) -> None:
        self.profile_dir = Path(profile_dir)
        self.timeout_ms = request_timeout_seconds * 1000
        self.human_wait_seconds = human_wait_seconds
        self.site_language = site_language
        self._playwright: Optional[Playwright] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    async def _launch(self) -> Page:
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = await async_playwright().start()
        launch_kwargs = dict(
            headless=False,
            viewport={"width": 1366, "height": 800},
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5"},
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = None
        for attempt in range(1, 6):
            try:
                self._context = await self._playwright.chromium.launch_persistent_context(
                    str(self.profile_dir), channel="chrome", **launch_kwargs
                )
                break
            except Exception as exc:
                if "existing browser session" in str(exc):
                    # A previous Chrome on this profile (e.g. the search
                    # engine that just closed) has not released its lock yet.
                    logger.warning("Profile still locked by another Chrome (attempt %s/5); retrying in 5s", attempt)
                    await asyncio.sleep(5)
                    continue
                logger.warning(
                    "Real Chrome not available (%s); falling back to bundled Chromium, "
                    "which Alibaba's anti-bot is very likely to block.",
                    exc,
                )
                self._context = await self._playwright.chromium.launch_persistent_context(
                    str(self.profile_dir), **launch_kwargs
                )
                break
        if self._context is None:
            raise RuntimeError(
                f"Chrome profile {self.profile_dir} is in use by another Chrome. Close it and retry."
            )
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        return self._page

    async def close(self) -> None:
        if self._context is not None:
            await self._context.close()
            self._context = None
            self._page = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def force_language(self, lang: Optional[str] = None) -> None:
        """1688 picks the content language (seller's Chinese, or its own
        machine translation) from the `oversealanguage` cookie, and resets
        that cookie from the account's overseas profile on every page load.
        The APIs honour whatever the cookie says at call time, so it is
        simply pinned before each call/navigation."""
        assert self._context is not None
        await self._context.add_cookies(
            [{"name": "oversealanguage", "value": lang or self.site_language, "domain": ".1688.com", "path": "/"}]
        )

    # ------------------------------------------------------------------ #
    # Walls
    # ------------------------------------------------------------------ #
    @staticmethod
    async def html_head(page: Page) -> str:
        try:
            return await page.evaluate("document.documentElement.outerHTML.slice(0, 20000)")
        except Exception:
            return ""

    async def wall_present(self, page: Page) -> bool:
        snippet = await self.html_head(page)
        if "补充联系信息" in snippet:
            await self._dismiss_interstitial(page)
            return False
        return is_wall(page.url, snippet)

    @staticmethod
    async def _dismiss_interstitial(page: Page) -> None:
        """New accounts get bounced to a 'complete your contact info' form
        (补充联系信息). It is optional: click 'don't remind me again'."""
        logger.info("Dismissing 1688 contact-info interstitial")
        try:
            await page.click("text=不再提醒", timeout=5000)
            await page.wait_for_timeout(1000)
        except Exception as exc:
            logger.warning("Could not dismiss interstitial automatically: %s", exc)

    async def wait_for_human(self, page: Page) -> None:
        logger.warning(
            "=== CAPTCHA / LOGIN WALL on %s. Please solve it (or log in) in the Chrome "
            "window. Waiting up to %ss ===",
            page.url[:100],
            self.human_wait_seconds,
        )
        t0 = time.monotonic()
        last_log = t0
        while time.monotonic() - t0 < self.human_wait_seconds:
            await asyncio.sleep(3)
            if not await self.wall_present(page):
                logger.warning("Wall cleared after %.0fs, resuming.", time.monotonic() - t0)
                return
            if time.monotonic() - last_log > 30:
                last_log = time.monotonic()
                logger.warning("Still waiting for human (%.0fs elapsed)...", time.monotonic() - t0)
        raise WallNotClearedError(
            f"CAPTCHA/login wall not cleared within {self.human_wait_seconds}s"
        )

    async def goto_through_walls(self, page: Page, url: str, wait_until: str = "domcontentloaded") -> None:
        """Navigate with the language pinned; if a wall appears, wait for a
        human then navigate again."""
        await self.force_language()
        await page.goto(url, wait_until=wait_until, timeout=self.timeout_ms)
        await asyncio.sleep(1.0)
        while await self.wall_present(page):
            await self.wait_for_human(page)
            await self.force_language()
            await page.goto(url, wait_until=wait_until, timeout=self.timeout_ms)
            await asyncio.sleep(1.0)
        await self.force_language()
