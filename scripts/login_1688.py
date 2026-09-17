"""
One-time login helper: opens the crawler's Chrome profile on the 1688 login
page and waits until you are logged in, so `main.py` never has to stop for
a login wall. Cookies stay in BROWSER_PROFILE_DIR (default `.pw_profile`).

Usage:
    python scripts/login_1688.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402
from playwright.async_api import async_playwright  # noqa: E402

load_dotenv()

PROFILE = Path(os.getenv("BROWSER_PROFILE_DIR", ".pw_profile"))
LOGIN_URL = "https://login.1688.com/member/signin.htm"
CHECK_URL = "https://www.1688.com/"
WAIT_SECONDS = 3600


async def main() -> None:
    PROFILE.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(PROFILE),
            channel="chrome",
            headless=False,
            viewport={"width": 1366, "height": 800},
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5"},
            args=["--disable-blink-features=AutomationControlled"],
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)

        print("Log in inside the Chrome window (password or QR code).")
        print(f"Waiting up to {WAIT_SECONDS // 60} minutes...")
        t0 = time.time()
        while time.time() - t0 < WAIT_SECONDS:
            await page.wait_for_timeout(3000)
            cookies = {c["name"] for c in await ctx.cookies("https://www.1688.com")}
            logged_in = "login" not in page.url and bool({"cookie2", "_tb_token_", "sgcookie"} & cookies)
            if logged_in:
                print(f"Logged in after {time.time() - t0:.0f}s. Session saved to {PROFILE.resolve()}")
                break
        else:
            print("Timed out without a login. Run the script again.")
        await ctx.close()


if __name__ == "__main__":
    asyncio.run(main())
