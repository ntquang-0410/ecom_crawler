"""
Mirror the local raw JSONL shards to the Hugging Face dataset repo.

    python scripts/mirror_raw_to_hf.py            # upload data/raw/*.jsonl -> data/raw/ on the Hub
    python scripts/mirror_raw_to_hf.py --dry-run

The worker already mirrors each 1,000-row batch as parquet under
data/bronze/, but a batch can be lost when the upload fails or (before the
run_id fix) when a restarted worker overwrote an earlier batch. The raw
JSONL on disk is the source of truth, so run this after a crawl pass to
make sure the Hub holds everything. Files are uploaded under their own
name; re-running replaces a shard with its longer, appended version.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from huggingface_hub import HfApi

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    raw_dir = Path(settings.raw_data_dir)
    shards = sorted(raw_dir.glob("*.jsonl"))
    if not shards:
        print(f"no .jsonl shards in {raw_dir}")
        return

    for shard in shards:
        rows = sum(1 for _ in shard.open(encoding="utf-8"))
        print(f"{shard.name}: {rows} rows, {shard.stat().st_size / 1e6:.1f} MB")
    if args.dry_run:
        return

    api = HfApi(token=settings.hf_token)
    api.upload_folder(
        folder_path=str(raw_dir),
        path_in_repo="data/raw",
        repo_id=settings.hf_repo_id,
        repo_type=settings.hf_repo_type,
        allow_patterns=["*.jsonl"],
        commit_message=f"Mirror raw JSONL shards from {settings.worker_id}",
    )
    print(f"uploaded {len(shards)} shards to {settings.hf_repo_id}/data/raw")


if __name__ == "__main__":
    main()
