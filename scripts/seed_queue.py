from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the Central Queue with URLs.")
    parser.add_argument("file", help="Path to a text file with one URL per line.")
    parser.add_argument("--category", default=None, help="Category label for these URLs.")
    args = parser.parse_args()

    settings = load_settings()
    category = args.category or settings.default_category

    urls = [
        line.strip()
        for line in Path(args.file).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    queue_manager = QueueManager(
        cred_path=settings.firebase_cred_path,
        db_url=settings.firebase_db_url,
        queue_path=settings.firebase_queue_path,
        worker_id="seeder",
    )

    count = queue_manager.enqueue_urls(urls, category=category)
    print(f"Enqueued {count} URLs under category='{category}'.")


if __name__ == "__main__":
    main()
