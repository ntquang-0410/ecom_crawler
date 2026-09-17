"""
Crawl a small review sample end to end on this machine only: no Firebase
queue, no Hugging Face upload. Meant for checking the row format before a
full run.

    python scripts/sample_crawl.py --rows 100
    python scripts/sample_crawl.py --rows 100 --out data/sample

Steps:
  1. search: page 1 of the first keyword of every category in
     keywords_1688.txt (60 products each), through the normal search engine;
  2. pick products round-robin across categories until --rows;
  3. detail in zh+vi mode through the normal detail engine (same code path
     as main.py), normalised like the worker does;
  4. write <out>/sample_<rows>.jsonl (exact row format) and
     sample_<rows>.csv (UTF-8 with BOM, opens cleanly in Excel).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from config import load_settings  # noqa: E402
from core.browser_1688 import WallNotClearedError  # noqa: E402
from core.engine_1688_detail import Detail1688Engine  # noqa: E402
from core.engine_1688_search import Search1688Engine  # noqa: E402
from core.models import ProductRecord, QueueItem  # noqa: E402
from core.textnorm import normalize_record  # noqa: E402
from parsers.parser_1688_detail import Detail1688Parser  # noqa: E402
from parsers.parser_1688_search import Search1688Parser  # noqa: E402
from scripts.seed_1688_details import CARRIED_META  # noqa: E402
from scripts.seed_1688_keywords import SEARCH_URL, read_keywords  # noqa: E402

CSV_COLUMNS = [
    "product_id", "category", "title_zh", "title_vi", "description_zh", "description_vi",
    "has_vi", "n_attributes_aligned", "price", "sales", "province", "city", "biz_type", "shop",
    "category_1688", "keyword", "url", "search_url", "crawl_time", "source_site",
]


def to_csv_row(row: Dict) -> Dict:
    meta = row["meta"]
    out = {c: row.get(c, meta.get(c)) for c in CSV_COLUMNS}
    out["has_vi"] = meta.get("has_vi")
    out["n_attributes_aligned"] = meta.get("n_attributes_aligned")
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100)
    ap.add_argument("--keywords", default="keywords_1688.txt")
    ap.add_argument("--out", default="data/sample")
    ap.add_argument("--min-delay", type=float, default=4.0)
    ap.add_argument("--max-delay", type=float, default=8.0)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    settings = load_settings()
    out_dir = ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. search --------------------------------------------------------------
    by_category = read_keywords(ROOT / args.keywords)
    search_items = [
        QueueItem(key=f"s{i}", url=SEARCH_URL.format(kw=quote(kws[0].encode("gbk")), page=1), category=cat)
        for i, (cat, kws) in enumerate(by_category.items())
    ]
    search = Search1688Engine(
        parser=Search1688Parser(lang="zh"), worker_id=settings.worker_id,
        profile_dir=Path(settings.browser_profile_dir),
        request_timeout_seconds=settings.request_timeout_seconds,
        min_delay_seconds=args.min_delay, max_delay_seconds=args.max_delay,
        human_wait_seconds=settings.human_wait_seconds, site_language="zh",
    )
    try:
        found, failed = await search.crawl_batch(search_items)
    finally:
        await search.close()
    print(f"search: {len(found)} products from {len(search_items) - len(failed)}/{len(search_items)} pages", flush=True)
    await asyncio.sleep(3)  # let Chrome release the profile lock before the detail engine opens it

    # 2. pick round-robin ----------------------------------------------------
    per_cat: Dict[str, List[ProductRecord]] = defaultdict(list)
    seen = set()
    for r in found:
        if r.product_id not in seen:
            seen.add(r.product_id)
            per_cat[r.category].append(r)
    picked: List[ProductRecord] = []
    while len(picked) < args.rows and any(per_cat.values()):
        for cat in list(per_cat):
            if per_cat[cat] and len(picked) < args.rows:
                picked.append(per_cat[cat].pop(0))
    detail_items = [
        QueueItem(
            key=f"d{i}", url=r.url, category=r.category,
            meta={k: r.meta.get(k) for k in CARRIED_META if r.meta.get(k) is not None},
        )
        for i, r in enumerate(picked)
    ]
    print(f"detail: crawling {len(detail_items)} products in zh+vi", flush=True)

    # 3. detail --------------------------------------------------------------
    detail = Detail1688Engine(
        parser=Detail1688Parser(), worker_id=settings.worker_id,
        profile_dir=Path(settings.browser_profile_dir),
        request_timeout_seconds=settings.request_timeout_seconds,
        min_delay_seconds=args.min_delay, max_delay_seconds=args.max_delay,
        human_wait_seconds=settings.human_wait_seconds,
        fetch_description=settings.detail_fetch_description, site_language="zh+vi",
    )
    records: List[ProductRecord] = []
    failures: List[QueueItem] = []
    try:
        # One item per call so a CAPTCHA nobody solves still leaves us with
        # every product crawled before it.
        for item in detail_items:
            ok, bad = await detail.crawl_batch([item])
            records.extend(ok)
            failures.extend(bad)
    except WallNotClearedError as exc:
        print(f"stopped early: {exc}", flush=True)
    finally:
        await detail.close()
    for r in records:
        normalize_record(r)

    # 4. write ---------------------------------------------------------------
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

    n_bi = sum(1 for r in records if r.has_vi)
    empties = Counter(k for r in records for k in ("price", "sales", "province", "city", "biz_type", "shop") if r.meta.get(k) is None)
    print(f"\nwrote {len(rows)} rows -> {jsonl_path} and {csv_path}")
    print(f"bilingual: {n_bi} | monolingual zh: {len(records) - n_bi} | failed: {len(failures)}")
    print(f"null fields: {dict(empties)}")
    print("per category:", dict(Counter(r.category for r in records)))


if __name__ == "__main__":
    asyncio.run(main())
