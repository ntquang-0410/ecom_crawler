"""
Seed the detail queue with 1688 product pages, taken from the product ids
collected by the search stage (data/raw/1688search_zh_*.jsonl).

Each queue item carries the product's search-result fields (price, sales,
province, city, biz_type, shop, keyword, search_url...) so the detail row is
self-contained.

A product is skipped if it is already present in a LOCAL detail shard
(1688_bilingual_*.jsonl / 1688_mono_zh_*.jsonl) -- OR already has a node in
`--queue` (any status: pending/processing/done/failed), fetched fresh from
Firebase every run. The Firebase check is what matters on a multi-machine
team: local files only see what THIS machine crawled, so checking local
files alone re-enqueues everything another machine already has (this
happened on 19/09 -- two machines each seeded from their own raw/, producing
~11k duplicate queue items). Re-running this script is safe now regardless
of which machine already seeded what.

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
    print(f"already detailed (local raw on this machine only): {len(done)}")

    queue_manager = QueueManager(
        cred_path=settings.firebase_cred_path,
        db_url=settings.firebase_db_url,
        queue_path=settings.firebase_queue_path,
        worker_id="seeder",
    )
    existing_items = queue_manager.ref.get() or {}
    queued_urls = {v.get("url") for v in existing_items.values() if isinstance(v, dict)}
    print(f"already queued in '{settings.firebase_queue_path}' (any status, any machine): {len(queued_urls)}")

    pending = {
        pid: row
        for pid, row in found.items()
        if pid not in done and DETAIL_URL.format(pid=pid) not in queued_urls
    }
    print(
        f"products from search: {len(found)} | already detailed or queued: "
        f"{len(found) - len(pending)} | new: {len(pending)}"
    )

    by_category: Dict[str, List[str]] = defaultdict(list)
    for pid, row in pending.items():
        by_category[row.get("category") or "unknown"].append(pid)

    rng = random.Random(args.seed)
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
            queue_manager.enqueue_urls(urls, category=category, metas=metas)
        total += len(urls)
        print(f"{category}: {len(urls)} detail pages")

    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} detail pages in total.")


if __name__ == "__main__":
    main()
