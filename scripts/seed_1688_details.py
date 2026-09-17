"""
Seed the detail queue with 1688 product pages, taken from the product ids
collected by the search stage (data/raw/1688search_zh_*.jsonl).

Each queue item carries the product's search-result fields (price, sales,
province, city, biz_type, shop, keyword, search_url...) so the detail row is
self-contained. Products already present in a detail shard
(1688_bilingual_*.jsonl / 1688_mono_zh_*.jsonl) are skipped, so the script
can be re-run after every search pass.

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
from typing import Any, Dict, List, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

DETAIL_URL = "https://detail.1688.com/offer/{pid}.html"
SEARCH_PREFIX = "1688search_zh_"
DETAIL_PREFIXES = ("1688_bilingual_", "1688_mono_zh_")
CARRIED_META = ("keyword", "page", "search_url", "price", "sales", "province", "city", "biz_type", "shop", "is_ad")


def load_rows(raw_dir: Path, prefix: str) -> Dict[str, Dict[str, Any]]:
    """product_id -> row, first occurrence wins."""
    out: Dict[str, Dict[str, Any]] = {}
    for path in sorted(raw_dir.glob(f"{prefix}*.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                pid = str(row.get("product_id") or "")
                if pid and pid not in out:
                    out[pid] = row
    return out


def carried_meta(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("meta") or {}
    return {k: meta.get(k) for k in CARRIED_META if meta.get(k) is not None}


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed the queue with 1688 detail pages.")
    ap.add_argument("--per-category", type=int, default=0, help="cap per category (0 = all)")
    ap.add_argument("--seed", type=int, default=42, help="random seed for the per-category sample")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    raw_dir = Path(settings.raw_data_dir)
    found = load_rows(raw_dir, SEARCH_PREFIX)
    done: Set[str] = set()
    for prefix in DETAIL_PREFIXES:
        done |= set(load_rows(raw_dir, prefix))
    pending = {pid: row for pid, row in found.items() if pid not in done}
    print(
        f"products from search: {len(found)} | already detailed: {len(done)} | "
        f"new: {len(pending)} | queue path: {settings.firebase_queue_path}"
    )

    by_category: Dict[str, List[str]] = defaultdict(list)
    for pid, row in pending.items():
        by_category[row.get("category") or "unknown"].append(pid)

    rng = random.Random(args.seed)
    queue_manager = None
    total = 0
    for category, pids in sorted(by_category.items()):
        if args.per_category and len(pids) > args.per_category:
            pids = rng.sample(pids, args.per_category)
        urls = [DETAIL_URL.format(pid=pid) for pid in pids]
        metas = [carried_meta(pending[pid]) for pid in pids]
        if args.dry_run:
            for url, meta in list(zip(urls, metas))[:2]:
                print(f"{category}\t{url}\t{meta}")
        else:
            if queue_manager is None:
                queue_manager = QueueManager(
                    cred_path=settings.firebase_cred_path,
                    db_url=settings.firebase_db_url,
                    queue_path=settings.firebase_queue_path,
                    worker_id="seeder",
                )
            queue_manager.enqueue_urls(urls, category=category, metas=metas)
        total += len(urls)
        print(f"{category}: {len(urls)} detail pages")

    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} detail pages in total.")


if __name__ == "__main__":
    main()
