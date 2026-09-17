"""
Seed the Central Queue with 1688 product detail URLs, taken from the
product ids already collected by the search crawl (data/raw/1688_zh_*.jsonl).

Products whose attributes were already crawled (data/raw/1688detail_*.jsonl)
are skipped, so the script can be re-run after every search pass.

Usage:
    python scripts/seed_1688_details.py                       # everything new
    python scripts/seed_1688_details.py --per-category 2000   # balanced subset
    python scripts/seed_1688_details.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

DETAIL_URL = "https://detail.1688.com/offer/{pid}.html"


def load_ids(raw_dir: Path, prefix: str) -> Dict[str, str]:
    """product_id -> category, first occurrence wins."""
    out: Dict[str, str] = {}
    for path in sorted(raw_dir.glob(f"{prefix}*.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                pid = str(row.get("product_id") or "")
                if pid and pid not in out:
                    out[pid] = row.get("category") or "unknown"
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed the queue with 1688 detail pages.")
    ap.add_argument("--per-category", type=int, default=0, help="cap per category (0 = all)")
    ap.add_argument("--seed", type=int, default=42, help="random seed for the per-category sample")
    ap.add_argument(
        "--ids-from",
        help="only products present in this JSONL (e.g. a 1688detail_vi file, to crawl the same ids in zh)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    raw_dir = Path(settings.raw_data_dir)
    titles = load_ids(raw_dir, "1688_zh_")
    if args.ids_from:
        wanted = set(load_ids(Path(args.ids_from).parent, Path(args.ids_from).stem))
        titles = {pid: cat for pid, cat in titles.items() if pid in wanted}
        print(f"restricted to {len(titles)} ids from {args.ids_from}")
    done: Set[str] = set(load_ids(raw_dir, f"1688detail_{settings.site_language}_"))
    pending = {pid: cat for pid, cat in titles.items() if pid not in done}
    print(
        f"products with titles: {len(titles)} | already detailed in '{settings.site_language}': "
        f"{len(done)} | new: {len(pending)} | queue path: {settings.firebase_queue_path}"
    )

    by_category: Dict[str, List[str]] = defaultdict(list)
    for pid, cat in pending.items():
        by_category[cat].append(pid)

    rng = random.Random(args.seed)
    queue_manager = None
    total = 0
    for category, pids in sorted(by_category.items()):
        if args.per_category and len(pids) > args.per_category:
            pids = rng.sample(pids, args.per_category)
        urls = [DETAIL_URL.format(pid=pid) for pid in pids]
        if args.dry_run:
            for url in urls[:3]:
                print(f"{category}\t{url}")
        else:
            if queue_manager is None:
                queue_manager = QueueManager(
                    cred_path=settings.firebase_cred_path,
                    db_url=settings.firebase_db_url,
                    queue_path=settings.firebase_queue_path,
                    worker_id="seeder",
                )
            queue_manager.enqueue_urls(urls, category=category)
        total += len(urls)
        print(f"{category}: {len(urls)} detail pages")

    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} detail pages in total.")


if __name__ == "__main__":
    main()
