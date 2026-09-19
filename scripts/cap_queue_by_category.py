"""
Cap `queue_detail_bi` per category: keep at most `--cap` items total (done +
pending combined) for each kept category, and drop every pending item of a
category that isn't in the keep-list at all.

    python scripts/cap_queue_by_category.py --dry-run
    python scripts/cap_queue_by_category.py

Only `pending` items are ever removed -- `done`/`processing` items (already
crawled or being crawled right now) are never touched, so this is safe to
run while a worker is active. A category already past its cap (its `done`
count alone exceeds `--cap`) has all its remaining pending items removed:
nothing more is needed from it.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

KEEP_CATEGORIES = {"bags", "beauty", "electronics", "fashion", "food", "home", "mother_baby", "shoes"}
DROP_CATEGORIES = {"auto", "office", "sports"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default="queue_detail_bi")
    ap.add_argument("--cap", type=int, default=2500, help="target done+pending per kept category")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    qm = QueueManager(settings.firebase_cred_path, settings.firebase_db_url, args.queue, worker_id="cap")
    items = qm.ref.get() or {}
    print(f"{args.queue}: {len(items)} items total")

    by_cat = defaultdict(list)
    for key, v in items.items():
        by_cat[v.get("category")].append((key, v))

    unknown = set(by_cat) - KEEP_CATEGORIES - DROP_CATEGORIES
    if unknown:
        print(f"WARNING: categories not classified as keep or drop, left untouched: {unknown}")

    to_delete = []
    print(f"\n{'category':14s} {'done':>6s} {'pending':>7s} {'target cap':>10s} {'pending kept':>12s} {'pending removed':>16s}")
    for cat in sorted(by_cat):
        entries = by_cat[cat]
        done_keys = [k for k, v in entries if v.get("status") in ("done", "processing")]
        pending_entries = [(k, v) for k, v in entries if v.get("status") == "pending"]

        cap = args.cap if cat in KEEP_CATEGORIES else 0
        keep_n = max(0, cap - len(done_keys))
        remove = pending_entries[keep_n:]
        to_delete.extend(k for k, v in remove)

        print(f"{cat:14s} {len(done_keys):>6d} {len(pending_entries):>7d} {cap:>10d} {len(pending_entries) - len(remove):>12d} {len(remove):>16d}")

    print(f"\ntotal pending to remove: {len(to_delete)}")
    print(f"queue would go from {len(items)} to {len(items) - len(to_delete)} items")

    if args.dry_run:
        return

    for i in range(0, len(to_delete), 500):
        chunk = to_delete[i : i + 500]
        qm.ref.update({k: None for k in chunk})
        print(f"deleted {min(i + 500, len(to_delete))}/{len(to_delete)}", flush=True)

    after = qm.ref.get() or {}
    print(f"{args.queue} now: {len(after)} items, {dict(Counter(v.get('status') for v in after.values()))}")


if __name__ == "__main__":
    main()
