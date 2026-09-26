"""
Discover Tiki product ids per bucket and seed `queue_tiki_detail`.

Run this ONCE (any one machine) before the team starts `main.py` workers with
CRAWLER_ENGINE=tiki_detail -- it only calls the public listing API (no
detail fetches), so it is quick compared to the detail crawl it seeds.

Combines 3 sort orders per bucket (default/newest/price,asc) to get past
Tiki's ~2000-result cap on a single sort, same idea as 1688's multi-keyword
search. Re-running is safe: it fetches the current queue first (any status,
any machine) and never re-enqueues a product already there.

    python scripts/seed_tiki_details.py --per-category 2200
    python scripts/seed_tiki_details.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402
from core.tiki_client import TikiClient  # noqa: E402
from parsers.parser_tiki import BUCKETS, SORTS, detail_url, listing_url, parse_listing  # noqa: E402

MAX_PAGE = 50
TIKI_QUEUE_PATH = "queue_tiki_detail"


def discover_bucket(client: TikiClient, roots: List[int], target: int, already: Set[str]) -> List[Dict]:
    """Walk (root, sort, page) until `target` NEW unique ids are found or
    every window is exhausted. Order: finish one (root, sort) before moving
    to the next, so a partial run still yields a clean, deduped batch."""
    found: List[Dict] = []
    seen_this_run: Set[str] = set()

    for root in roots:
        for sort in SORTS:
            for page in range(1, MAX_PAGE + 1):
                if len(found) >= target:
                    return found
                payload = client.get_json(listing_url(root, page, sort))
                if not payload:
                    break
                items = parse_listing(payload)
                if not items:
                    break
                for it in items:
                    if it["id"] in already or it["id"] in seen_this_run:
                        continue
                    seen_this_run.add(it["id"])
                    found.append({
                        "id": it["id"], "spid": it["spid"], "is_ad": it["is_ad"],
                        "page": page, "sort": sort, "root_category": root,
                        "search_url": listing_url(root, page, sort),
                    })
                    if len(found) >= target:
                        return found
    return found


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-category", type=int, default=2200)
    ap.add_argument("--min-delay", type=float, default=1.0)
    ap.add_argument("--max-delay", type=float, default=2.0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    client = TikiClient(
        timeout_seconds=settings.request_timeout_seconds, max_attempts=settings.max_crawl_attempts,
        min_delay_seconds=args.min_delay, max_delay_seconds=args.max_delay,
    )

    queue_manager = QueueManager(
        cred_path=settings.firebase_cred_path, db_url=settings.firebase_db_url,
        queue_path=TIKI_QUEUE_PATH, worker_id="seeder",
        max_attempts=settings.max_crawl_attempts, lock_timeout_seconds=settings.lock_timeout_seconds,
    )
    existing_items = queue_manager.ref.get() or {}
    already_by_bucket: Dict[str, Set[str]] = {b: set() for b in BUCKETS}
    for v in existing_items.values():
        if isinstance(v, dict) and v.get("category") in already_by_bucket:
            pid = (v.get("meta") or {}).get("product_id")
            if pid:
                already_by_bucket[v["category"]].add(pid)
    print(f"already queued (any status, any machine): { {b: len(s) for b, s in already_by_bucket.items()} }")

    total = 0
    for bucket, roots in BUCKETS.items():
        already = already_by_bucket[bucket]
        target = max(0, args.per_category - len(already))
        if target == 0:
            print(f"{bucket}: already has {len(already)} >= target, skipping")
            continue
        found = discover_bucket(client, roots, target, already)
        print(f"{bucket}: found {len(found)} new products (target {target})")

        if args.dry_run:
            for it in found[:2]:
                print(f"  {it}")
            total += len(found)
            continue

        urls = [detail_url(it["id"], it["spid"]) for it in found]
        metas = [
            {
                "product_id": it["id"], "spid": it["spid"], "is_ad": it["is_ad"],
                "page": it["page"], "sort": it["sort"], "root_category": it["root_category"],
                "search_url": it["search_url"],
            }
            for it in found
        ]
        queue_manager.enqueue_urls(urls, category=bucket, metas=metas)
        total += len(found)

    print(f"{'Would enqueue' if args.dry_run else 'Enqueued'} {total} products into '{TIKI_QUEUE_PATH}'.")


if __name__ == "__main__":
    main()
