"""
Set the 1688 account language (简体中文 / Tiếng Việt / English) through the
"Target Market, Language & Currency" panel, and verify by calling the search
API until results actually come back in the requested language.

Why verify: the panel sets the cookie client-side at once, but the server-
side preference (and the translation cache behind the search API) can lag
or silently fail, and the worker refuses to run on translated titles.

Usage:
    python scripts/set_1688_language.py zh
    python scripts/set_1688_language.py vi
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402
from playwright.async_api import Page, async_playwright  # noqa: E402

load_dotenv()

PROFILE = Path(os.getenv("BROWSER_PROFILE_DIR", ".pw_profile"))
LABELS = {"zh": "简体中文", "vi": "Tiếng Việt", "en": "English"}
CJK = re.compile(r"[一-鿿]")
VI = re.compile(r"[ăâđêôơưàáảãạèéẻẽẹìíỉĩịòóỏõọùúủũụỳýỷỹỵ]", re.I)

SEARCH_JS = """
async () => {
  const params = {beginPage: '1', pageSize: 60, method: 'getOfferList', searchScene: 'pcOfferSearch',
                  verticalProductFlag: 'pcmarket', charset: 'GBK', spm: 'a26352.13672862', keywords: '收纳盒'};
  const res = await window.lib.mtop.request({api: 'mtop.relationrecommend.WirelessRecommend.recommend', v: '2.0',
      type: 'GET', dataType: 'jsonp', timeout: 20000, data: {appId: 32517, params: JSON.stringify(params)}});
  return (((((res || {}).data || {}).data || {}).OFFER || {}).items || []).map(x => x.data.title);
}
"""


def language_of(titles: list[str]) -> str:
    if not titles:
        return "?"
    zh = sum(bool(CJK.search(t)) for t in titles)
    vi = sum(bool(VI.search(t)) for t in titles)
    if zh >= 0.8 * len(titles):
        return "zh"
    if vi >= 0.5 * len(titles):
        return "vi"
    return "en/other"


async def wait_walls(page: Page) -> None:
    for i in range(300):
        if any(m in page.url for m in ("login", "punish", "_____tmd_____")):
            if i % 10 == 0:
                print("  >> CAPTCHA/login wall - please solve it in the Chrome window")
            await page.wait_for_timeout(3000)
            continue
        return


SAVE_RE = re.compile(r"^(Save|保存|Lưu)$")


async def open_panel(page: Page) -> None:
    button = page.get_by_text(re.compile(r"^[A-Z]{2}-[^-]+-[A-Z]{3}$")).first
    for _ in range(4):
        await button.hover(timeout=15000)
        await page.wait_for_timeout(800)
        await button.click(timeout=15000)
        await page.wait_for_timeout(3000)
        if await page.get_by_text(SAVE_RE).count():
            return
    raise RuntimeError("language panel did not open")


async def switch(page: Page, label: str, current_label: str) -> None:
    await page.goto("https://www.1688.com/", wait_until="domcontentloaded", timeout=60000)
    await wait_walls(page)
    await page.wait_for_timeout(10000)
    await open_panel(page)
    # Inside the panel the language select shows the current language; its
    # dropdown only opens from the arrow at the right edge of the box.
    cur = page.get_by_text(current_label, exact=True).last
    bb = await cur.bounding_box()
    if not bb:
        raise RuntimeError(f"current language label {current_label!r} not found in panel")
    await page.mouse.click(bb["x"] + 250, bb["y"] + bb["height"] / 2)
    await page.wait_for_timeout(2000)
    await page.screenshot(path="lang_dropdown.png")
    option = page.get_by_text(label, exact=True).last
    await option.click(timeout=10000)
    await page.wait_for_timeout(1000)
    await page.get_by_text(SAVE_RE).first.click(timeout=10000)
    await page.wait_for_timeout(6000)


async def verify(page: Page) -> tuple[str, list[str]]:
    await page.goto("https://www.1688.com/", wait_until="domcontentloaded", timeout=60000)
    await wait_walls(page)
    for _ in range(30):
        await page.wait_for_timeout(1000)
        try:
            if await page.evaluate("!!(window.lib && window.lib.mtop)"):
                break
        except Exception:
            pass
    titles = await asyncio.wait_for(page.evaluate(SEARCH_JS), timeout=90)
    return language_of(titles), titles


async def main() -> None:
    target = (sys.argv[1] if len(sys.argv) > 1 else "zh").lower()
    label = LABELS[target]
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(PROFILE), channel="chrome", headless=False, viewport={"width": 1366, "height": 800},
            locale="en-US", args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        for attempt in range(1, 4):
            lang, titles = await verify(page)
            print(f"[{attempt}] search API currently returns: {lang}  e.g. {titles[0][:50] if titles else ''!r}")
            if lang == target:
                print(f"OK: account language is {label}.")
                break
            print(f"    switching to {label} ...")
            try:
                await switch(page, label, LABELS.get(lang, "English"))
            except Exception as exc:
                print("    switch failed:", str(exc)[:150])
                await page.screenshot(path="lang_switch_failed.png")
            await page.wait_for_timeout(5000)
        else:
            print("FAILED to set the language after 3 attempts; do it by hand in the Chrome window.")
        await ctx.close()


if __name__ == "__main__":
    asyncio.run(main())
