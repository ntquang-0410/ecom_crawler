"""
Tiki review sample, end to end on this machine only (no Firebase, no Hugging Face), same as sample_crawl.py for 1688.

    python scripts/sample_crawl_tiki.py --rows 100
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from config import load_settings  # noqa: E402
from core.models import ProductRecord  # noqa: E402
from core.textnorm import normalize_record  # noqa: E402
from core.tiki_client import TikiClient  # noqa: E402
from parsers.parser_tiki import BUCKETS, build_record, detail_url, listing_url, parse_listing  # noqa: E402

CSV_COLUMNS = [
    "product_id", "category", "title_vi", "description_vi", "description_prose_vi", "n_attributes_vi",
    "price", "list_price", "currency", "sales", "shop", "brand", "is_ad",
    "category_tiki", "category_id_tiki", "category_id_tiki_root", "breadcrumb_tiki",
    "url", "search_url", "crawl_time", "source_site",
]


def to_csv_row(row: Dict) -> Dict:
    meta = row["meta"]
    out = {c: row.get(c, meta.get(c)) for c in CSV_COLUMNS}
    out["breadcrumb_tiki"] = " > ".join(meta.get("breadcrumb_tiki") or [])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100)
    ap.add_argument("--out", default="data/sample_tiki")
    ap.add_argument("--min-delay", type=float, default=1.0)
    ap.add_argument("--max-delay", type=float, default=2.0)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    settings = load_settings()
    out_dir = ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    client = TikiClient(
        timeout_seconds=settings.request_timeout_seconds, max_attempts=settings.max_crawl_attempts,
        min_delay_seconds=args.min_delay, max_delay_seconds=args.max_delay,
    )

    # 1. listing page 1 of every root category, interleaving roots inside a bucket
    per_bucket: Dict[str, List[Dict]] = defaultdict(list)
    for bucket, roots in BUCKETS.items():
        pages = []
        for root in roots:
            url = listing_url(root, page=1)
            payload = client.get_json(url)
            items = parse_listing(payload) if payload else []
            pages.append([dict(it, page=1, sort="default", search_url=url, root_category=root) for it in items])
            print(f"listing {bucket}/{root}: {len(items)} products", flush=True)
        while any(pages):
            for p in pages:
                if p:
                    per_bucket[bucket].append(p.pop(0))

    # 2. round-robin across buckets, first bucket wins on a product listed in two roots
    picked: List[tuple] = []
    seen = set()
    cross_bucket_dupes = 0
    while len(picked) < args.rows and any(per_bucket.values()):
        for bucket in list(per_bucket):
            while per_bucket[bucket] and len(picked) < args.rows:
                it = per_bucket[bucket].pop(0)
                if it["id"] in seen:
                    cross_bucket_dupes += 1
                    continue
                seen.add(it["id"])
                picked.append((bucket, it))
                break
    print(f"detail: fetching {len(picked)} products", flush=True)

    # 3. detail
    records: List[ProductRecord] = []
    failed = []
    for bucket, it in picked:
        payload = client.get_json(detail_url(it["id"], it["spid"]))
        record = build_record(payload, bucket, settings.worker_id, it) if payload else None
        if record is None:
            failed.append(it["id"])
            continue
        records.append(normalize_record(record))

    # 4. write
    rows = [r.to_row() for r in records]
    jsonl_path = out_dir / f"sample_{args.rows}.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    csv_path = out_dir / f"sample_{args.rows}.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow(to_csv_row(row))

    empties = Counter(
        k for r in records for k in ("price", "sales", "shop", "brand", "category_tiki", "description_prose_vi")
        if not r.meta.get(k)
    )
    print(f"\nwrote {len(rows)} rows -> {jsonl_path} and {csv_path}")
    print(f"failed: {len(failed)} {failed[:10]} | cross-bucket duplicates skipped: {cross_bucket_dupes}")
    print(f"empty fields: {dict(empties)} | empty description_vi: {sum(1 for r in records if not r.description_vi)}")
    print(f"is_ad true: {sum(1 for r in records if r.meta.get('is_ad'))}")
    print("per category:", dict(Counter(r.category for r in records)))


if __name__ == "__main__":
    main()
