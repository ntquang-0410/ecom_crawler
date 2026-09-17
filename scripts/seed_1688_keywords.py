"""
Seed the Central Queue with 1688.com search-page URLs.

Input: a text file with one `category<TAB>keyword` per line (blank lines and
lines starting with `#` are ignored). Every keyword expands to
`--pages` queue items, one per result page:

    https://s.1688.com/selloffer/offer_search.htm?keywords=<kw>&beginPage=<n>

1688 caps a keyword at ~2000 hits (60 per page), so more than ~34 pages
per keyword only yields empty pages. Crawling by keyword quota per category
is what keeps the corpus balanced (CLAUDE.md section 6).

Usage:
    python scripts/seed_1688_keywords.py keywords_1688.txt --pages 34
    python scripts/seed_1688_keywords.py keywords_1688.txt --pages 3 --dry-run
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

SEARCH_URL = "https://s.1688.com/selloffer/offer_search.htm?keywords={kw}&beginPage={page}"


def read_keywords(path: Path) -> Dict[str, List[str]]:
    by_category: Dict[str, List[str]] = defaultdict(list)
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "\t" not in line:
            raise ValueError(f"Expected 'category<TAB>keyword', got: {line!r}")
        category, keyword = (part.strip() for part in line.split("\t", 1))
        if keyword and keyword not in by_category[category]:
            by_category[category].append(keyword)
    return by_category


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed the queue with 1688 search pages.")
    ap.add_argument("file", help="category<TAB>keyword per line")
    ap.add_argument("--pages", type=int, default=34, help="result pages per keyword")
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true", help="print URLs, do not enqueue")
    args = ap.parse_args()

    by_category = read_keywords(Path(args.file))
    pages = range(args.start_page, args.start_page + args.pages)

    total = 0
    queue_manager = None
    for category, keywords in by_category.items():
        # 1688 decodes the query string as GBK, not UTF-8: a UTF-8-encoded
        # keyword turns into mojibake and returns unrelated products.
        urls = [
            SEARCH_URL.format(kw=quote(kw.encode("gbk")), page=page)
            for kw in keywords
            for page in pages
        ]
        if args.dry_run:
            for url in urls:
                print(f"{category}\t{url}")
        else:
            if queue_manager is None:
                settings = load_settings()
                queue_manager = QueueManager(
                    cred_path=settings.firebase_cred_path,
                    db_url=settings.firebase_db_url,
                    queue_path=settings.firebase_queue_path,
                    worker_id="seeder",
                )
            queue_manager.enqueue_urls(urls, category=category)
        total += len(urls)
        print(f"{category}: {len(keywords)} keywords x {len(pages)} pages = {len(urls)} items")

    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} search pages in total.")


if __name__ == "__main__":
    main()
