"""
Build a clean, flat snapshot of the Tiki Vietnamese corpus (all workers
merged, deduped) and push it to the Hub outside data/raw and data/bronze,
so it's easy to open without pulling raw shards by hand.

    python scripts/snapshot_tiki_to_hf.py

Same idea as scripts/snapshot_to_hf.py for the 1688 corpus, but simpler:
Tiki has only one kind of row (monolingual Vietnamese), not a
bilingual/mono split, so there is one output file, not two.

`meta` is un-nested into real typed columns (price, category_tiki...)
instead of staying a JSON string, same reasoning as the 1688 snapshot: no
hardcoded column list, because that is exactly how "page" silently went
missing from the 1688 snapshot while staying in the raw JSONL (caught by
worker_huy). `attributes_vi` stays nested (list of {name, values}) since its
length varies per product; pandas/pyarrow read that natively.

IMPORTANT for a multi-machine team: this reads every worker's raw shards
from the Hub (data/raw/tiki_mono_vi_*_worker_*.jsonl -- each worker's file
name includes its own worker id, so they never collide), not just this
machine's local files, for the same reason as the 1688 snapshot: building
it from one machine's local data/raw/ only and uploading would silently
overwrite whatever the other workers had already contributed. Run
scripts/mirror_raw_to_hf.py first so this machine's own latest shards are
on the Hub before this step reads them back.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402

PREFIX = "tiki_mono_vi_"


def load_rows_from_hub(api: HfApi, repo_id: str, repo_type: str, token: str) -> pd.DataFrame:
    files = [
        f for f in api.list_repo_files(repo_id, repo_type=repo_type)
        if f.startswith(f"data/raw/{PREFIX}") and f.endswith(".jsonl")
    ]
    rows = []
    for f in sorted(files):
        path = hf_hub_download(repo_id, f, repo_type=repo_type, token=token)
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                rows.append(json.loads(line))
    print(f"{PREFIX}*: {len(files)} shard(s) from the Hub, {len(rows)} rows total")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset="product_id", keep="first")
    if len(df) != before:
        print(f"{PREFIX}*: dropped {before - len(df)} duplicate product_id rows")
    # No fixed column whitelist on purpose -- see module docstring.
    meta_df = pd.json_normalize(df.pop("meta"))
    return pd.concat([df.reset_index(drop=True), meta_df.reset_index(drop=True)], axis=1)


def main() -> None:
    settings = load_settings()
    api = HfApi(token=settings.hf_token)

    print("Downloading every worker's Tiki raw shards from the Hub...")
    tiki = load_rows_from_hub(api, settings.hf_repo_id, settings.hf_repo_type, settings.hf_token)
    print(f"merged across all workers: {len(tiki)} rows")
    if len(tiki):
        print("per category:", tiki["category"].value_counts().to_dict())

    tmp = Path(settings.raw_data_dir).parent / "snapshot_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    out_path = tmp / "tiki_vi.parquet"
    tiki.to_parquet(out_path, engine="pyarrow", index=False)

    api.upload_file(
        path_or_fileobj=str(out_path), path_in_repo="data/snapshot/tiki_vi.parquet",
        repo_id=settings.hf_repo_id, repo_type=settings.hf_repo_type,
        commit_message=f"Snapshot: {len(tiki)} Tiki Vietnamese products (all workers)",
    )
    print(f"uploaded to {settings.hf_repo_id}/data/snapshot/tiki_vi.parquet")


if __name__ == "__main__":
    main()
