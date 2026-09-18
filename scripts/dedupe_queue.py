"""
Remove duplicate items (same URL) from a queue, and optionally delete a
stale queue node.

    python scripts/dedupe_queue.py queue_detail_bi --dry-run
    python scripts/dedupe_queue.py queue_detail_bi
    python scripts/dedupe_queue.py queue_detail_bi --delete-node queue

Why duplicates happen: `seed_1688_details.py` only knows the raw files on
the machine it runs on, so seeding from two machines enqueues the same
product twice under different keys, and every worker would crawl it twice.

Which copy survives, per URL: a `done` one if any (so the product is not
re-crawled), else a `processing` one (someone is on it right now), else the
oldest `pending`/`failed` one. Only the other copies are deleted; nothing
that is `done` or `processing` is ever removed. Safe to run while workers
are crawling: a worker that has already claimed a copy keeps it.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

RANK = {"done": 0, "processing": 1, "pending": 2, "failed": 3}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("queue")
    ap.add_argument("--delete-node", help="also delete this top-level node entirely (e.g. a stale 'queue')")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    qm = QueueManager(settings.firebase_cred_path, settings.firebase_db_url, args.queue, worker_id="dedupe")
    items = qm.ref.get() or {}
    print(f"{args.queue}: {len(items)} items, {dict(Counter(v.get('status') for v in items.values()))}")

    by_url = defaultdict(list)
    for key, v in items.items():
        if isinstance(v, dict) and v.get("url"):
            by_url[v["url"]].append((key, v))

    to_delete = []
    kept_status = Counter()
    for url, copies in by_url.items():
        if len(copies) < 2:
            continue
        copies.sort(key=lambda kv: (RANK.get(kv[1].get("status"), 9), kv[1].get("updated_at", "")))
        keep, rest = copies[0], copies[1:]
        kept_status[keep[1].get("status")] += 1
        to_delete.extend(k for k, v in rest if v.get("status") not in ("done", "processing"))

    print(f"distinct urls: {len(by_url)} | urls with copies: {sum(1 for c in by_url.values() if len(c) > 1)}")
    print(f"copies to delete: {len(to_delete)} | surviving copy is {dict(kept_status)}")
    print(f"after: {len(items) - len(to_delete)} items")

    if args.dry_run:
        if args.delete_node:
            n = len(qm.ref.parent.child(args.delete_node).get(shallow=True) or {})
            print(f"would delete node '{args.delete_node}' ({n} items)")
        return

    for i in range(0, len(to_delete), 500):
        chunk = to_delete[i : i + 500]
        qm.ref.update({k: None for k in chunk})
        print(f"deleted {min(i + 500, len(to_delete))}/{len(to_delete)}", flush=True)

    if args.delete_node:
        qm.ref.parent.child(args.delete_node).delete()
        print(f"deleted node '{args.delete_node}'")

    after = qm.ref.get() or {}
    print(f"{args.queue} now: {len(after)} items, {dict(Counter(v.get('status') for v in after.values()))}")


if __name__ == "__main__":
    main()
