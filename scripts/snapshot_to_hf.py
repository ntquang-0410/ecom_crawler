"""
Build a clean, flat snapshot of the finished data (bilingual + mono-zh detail
records only -- no search-stage rows) and push it to the Hub at a path
outside data/raw and data/bronze, so it doesn't get mixed into the
auto-converted viewer table with the ~73k title-only search rows.

    python scripts/snapshot_to_hf.py

`meta` is un-nested into real typed columns (price, sales, province,
category_1688...) instead of staying a JSON string: data/raw/ keeps meta as
JSON because search-stage and detail-stage rows have different meta keys and
Parquet needs one fixed schema per column, but this snapshot only ever holds
detail-stage rows, whose meta keys are identical across every row, so there
is no reason to keep it opaque here. `attributes_zh`/`attributes_vi` stay
nested (list of {name, values}) since their length varies per product;
pandas/pyarrow read that natively as a column of Python lists.

IMPORTANT for a multi-machine team: this reads every worker's raw shards
from the Hub (data/raw/1688_bilingual_*_worker_*.jsonl,
1688_mono_zh_*_worker_*.jsonl -- each worker's file name includes its own
worker id, so they never collide), not just this machine's local files.
Building the snapshot from a single machine's local data/raw/ only and then
`upload_file`-ing it would silently OVERWRITE whatever the other workers had
already contributed, since all three machines write to the same
data/snapshot/*.parquet path (this happened in practice: worker_huy's
snapshot run replaced worker_quang's earlier one). Run
`scripts/mirror_raw_to_hf.py` first (sync_loop.py already does, in order) so
this machine's own latest shards are on the Hub before this step reads them
back.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

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


def load_rows_from_hub(api: HfApi, repo_id: str, repo_type: str, token: str, prefix: str) -> pd.DataFrame:
    """Download every worker's raw shard matching `prefix` (e.g. all
    data/raw/1688_bilingual_*.jsonl, whichever worker crawled them) and
    concatenate. Deduped by product_id (first occurrence wins) as a safety
    net -- the Firebase queue transaction already prevents two workers from
    crawling the same product, so real duplicates should be rare."""
    files = [
        f for f in api.list_repo_files(repo_id, repo_type=repo_type)
        if f.startswith(f"data/raw/{prefix}") and f.endswith(".jsonl")
    ]
    rows = []
    for f in sorted(files):
        path = hf_hub_download(repo_id, f, repo_type=repo_type, token=token)
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                rows.append(json.loads(line))
    print(f"  {prefix}*: {len(files)} shard(s) from the Hub, {len(rows)} rows total")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset="product_id", keep="first")
    if len(df) != before:
        print(f"  {prefix}*: dropped {before - len(df)} duplicate product_id rows")
    meta_df = pd.json_normalize(df.pop("meta")).reindex(columns=_META_COLUMNS)
    return pd.concat([df.reset_index(drop=True), meta_df.reset_index(drop=True)], axis=1)


def main() -> None:
    settings = load_settings()
    api = HfApi(token=settings.hf_token)

    print("Downloading every worker's raw shards from the Hub...")
    bilingual = load_rows_from_hub(api, settings.hf_repo_id, settings.hf_repo_type, settings.hf_token, "1688_bilingual_")
    mono = load_rows_from_hub(api, settings.hf_repo_id, settings.hf_repo_type, settings.hf_token, "1688_mono_zh_")
    print(f"merged across all workers: {len(bilingual)} bilingual + {len(mono)} mono_zh rows")

    tmp = Path(settings.raw_data_dir).parent / "snapshot_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    bi_path = tmp / "bilingual_zh_vi.parquet"
    mono_path = tmp / "mono_zh.parquet"
    bilingual.to_parquet(bi_path, engine="pyarrow", index=False)
    mono.to_parquet(mono_path, engine="pyarrow", index=False)

    api.upload_file(
        path_or_fileobj=str(bi_path), path_in_repo="data/snapshot/bilingual_zh_vi.parquet",
        repo_id=settings.hf_repo_id, repo_type=settings.hf_repo_type,
        commit_message=f"Snapshot: {len(bilingual)} bilingual zh-vi products (all workers)",
    )
    api.upload_file(
        path_or_fileobj=str(mono_path), path_in_repo="data/snapshot/mono_zh.parquet",
        repo_id=settings.hf_repo_id, repo_type=settings.hf_repo_type,
        commit_message=f"Snapshot: {len(mono)} monolingual zh products (all workers)",
    )
    print(f"uploaded to {settings.hf_repo_id}/data/snapshot/")


if __name__ == "__main__":
    main()
