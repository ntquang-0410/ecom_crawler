"""
Build a clean, flat snapshot of the finished data (bilingual + mono-zh detail
records only -- no search-stage rows) and push it to the Hub at a path
outside data/raw and data/bronze, so it doesn't get mixed into the
auto-converted viewer table with the ~73k title-only search rows.

    python scripts/snapshot_to_hf.py

Safe to run anytime, including while a crawl is in progress: it only reads
the local data/raw/*.jsonl files and overwrites two parquet files on the Hub.

`meta` is un-nested into real typed columns (price, sales, province,
category_1688...) instead of staying a JSON string: data/raw/ keeps meta as
JSON because search-stage and detail-stage rows have different meta keys and
Parquet needs one fixed schema per column, but this snapshot only ever holds
detail-stage rows, whose meta keys are identical across every row (verified:
all 20 keys present in 100% of rows), so there is no reason to keep it
opaque here. `attributes_zh`/`attributes_vi` stay nested (list of
{name, values}) since their length varies per product; pandas/pyarrow read
that natively as a column of Python lists, no json.loads() needed either.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402

# Scalar meta fields promoted to top-level columns; the rest (attributes_zh/vi,
# nested lists) are merged in as-is by pd.json_normalize.
_META_COLUMNS = [
    "price", "sales", "province", "city", "biz_type", "shop", "is_ad",
    "category_1688", "category_id_1688", "has_vi",
    "n_attributes_zh", "n_attributes_vi", "n_attributes_aligned",
    "attributes_zh", "attributes_vi",
    "description_extra_zh", "description_images",
    "keyword", "search_url",
]


def load_rows(raw_dir: Path, prefix: str) -> pd.DataFrame:
    rows = []
    for path in sorted(raw_dir.glob(f"{prefix}*.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                rows.append(json.loads(line))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    meta_df = pd.json_normalize(df.pop("meta")).reindex(columns=_META_COLUMNS)
    return pd.concat([df, meta_df], axis=1)


def main() -> None:
    settings = load_settings()
    raw_dir = Path(settings.raw_data_dir)

    bilingual = load_rows(raw_dir, "1688_bilingual_")
    mono = load_rows(raw_dir, "1688_mono_zh_")
    print(f"bilingual: {len(bilingual)} rows | mono_zh: {len(mono)} rows")

    tmp = raw_dir.parent / "snapshot_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    bi_path = tmp / "bilingual_zh_vi.parquet"
    mono_path = tmp / "mono_zh.parquet"
    bilingual.to_parquet(bi_path, engine="pyarrow", index=False)
    mono.to_parquet(mono_path, engine="pyarrow", index=False)

    api = HfApi(token=settings.hf_token)
    api.upload_file(
        path_or_fileobj=str(bi_path), path_in_repo="data/snapshot/bilingual_zh_vi.parquet",
        repo_id=settings.hf_repo_id, repo_type=settings.hf_repo_type,
        commit_message=f"Snapshot: {len(bilingual)} bilingual zh-vi products",
    )
    api.upload_file(
        path_or_fileobj=str(mono_path), path_in_repo="data/snapshot/mono_zh.parquet",
        repo_id=settings.hf_repo_id, repo_type=settings.hf_repo_type,
        commit_message=f"Snapshot: {len(mono)} monolingual zh products",
    )
    print(f"uploaded to {settings.hf_repo_id}/data/snapshot/")


if __name__ == "__main__":
    main()
