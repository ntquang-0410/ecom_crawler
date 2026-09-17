"""
Rebuild the search queue from the keyword file, skipping every
(keyword, page) that already exists in data/raw/1688search_zh_*.jsonl.

Use it when the Firebase queue was lost or cleared: the raw JSONL is the
source of truth for what has been crawled, so nothing is fetched twice.

    python scripts/reseed_1688_search.py keywords_1688.txt --pages 34 [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Set, Tuple
from urllib.parse import quote

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402
from scripts.seed_1688_keywords import SEARCH_URL, read_keywords  # noqa: E402


def crawled_pages(raw_dir: Path) -> Set[Tuple[str, int]]:
    done: Set[Tuple[str, int]] = set()
    for path in sorted(raw_dir.glob("1688search_zh_*.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    meta = json.loads(line).get("meta") or {}
                except ValueError:
                    continue
                if meta.get("keyword") and meta.get("page"):
                    done.add((meta["keyword"], int(meta["page"])))
    return done


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--pages", type=int, default=34)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    done = crawled_pages(Path(settings.raw_data_dir))
    print(f"pages already in raw: {len(done)}")

    queue_manager = None
    total = 0
    for category, keywords in read_keywords(Path(args.file)).items():
        urls = [
            SEARCH_URL.format(kw=quote(kw.encode("gbk")), page=page)
            for kw in keywords
            for page in range(1, args.pages + 1)
            if (kw, page) not in done
        ]
        if urls and not args.dry_run:
            if queue_manager is None:
                queue_manager = QueueManager(
                    cred_path=settings.firebase_cred_path,
                    db_url=settings.firebase_db_url,
                    queue_path=settings.firebase_queue_path,
                    worker_id="seeder",
                )
            queue_manager.enqueue_urls(urls, category=category)
        total += len(urls)
        print(f"{category}: {len(urls)} pages to (re)seed")
    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} pages into '{settings.firebase_queue_path}'.")


if __name__ == "__main__":
    main()
