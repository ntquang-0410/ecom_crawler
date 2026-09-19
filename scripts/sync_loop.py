"""
Keep the Hugging Face mirror in sync with local data/raw/ while a crawl is
running, without depending on a batch reaching 1000 rows or a whole crawl
stage finishing.

    python scripts/sync_loop.py                    # every 20 minutes
    python scripts/sync_loop.py --minutes 10

Runs mirror_raw_to_hf.py (raw JSONL backup) then snapshot_to_hf.py (clean
viewable parquet) in a loop until Ctrl+C. Meant to run in its own terminal
window alongside run_crawl.bat -- independent processes, neither depends on
the other, safe to start/stop anytime.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


def run(script: str) -> None:
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] >>> {script}", flush=True)
    code = subprocess.call([PYTHON, f"scripts/{script}"], cwd=ROOT)
    if code != 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {script} exited with code {code} (will retry next cycle)", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=20)
    args = ap.parse_args()

    print(f"Syncing to Hugging Face every {args.minutes} minutes. Ctrl+C to stop.", flush=True)
    while True:
        run("mirror_raw_to_hf.py")
        run("snapshot_to_hf.py")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] sleeping {args.minutes} min...", flush=True)
        time.sleep(args.minutes * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nstopped.")
