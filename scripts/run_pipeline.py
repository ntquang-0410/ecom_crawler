"""
Unattended runner: drive the crawl stages one after another on this machine.

    python scripts/run_pipeline.py search detail
    python scripts/run_pipeline.py detail

Stages:
    search   1688_search on `queue_search_zh` (titles + search fields, raw only)
    detail   seed `queue_detail_bi` from data/raw/1688search_zh_*.jsonl, then
             1688_detail in zh+vi mode: one row per product, bilingual rows to
             data/bronze/bilingual_zh_vi/, products without a Vietnamese
             version to data/bronze/mono_zh/ and to the `queue_detail_zh`
             registry.
    mono     1688_detail in zh mode on whatever is *pending* in
             `queue_detail_zh` (normally nothing; for re-crawls).

Each stage runs main.py until its queue has no pending/processing items,
restarting the worker whenever it exits (a CAPTCHA nobody solved, a network
drop, a throttling cool-down that ended in an error). Items left in
`processing` by a dead worker are released before every restart. After a
stage completes, the raw JSONL shards are mirrored to the Hub (data/raw/).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from config import load_settings  # noqa: E402
from core.queue_manager import QueueManager  # noqa: E402

PYTHON = sys.executable

STAGES = {
    "search": {"queue": "queue_search_zh", "engine": "1688_search", "lang": "zh", "seed": None},
    "detail": {"queue": "queue_detail_bi", "engine": "1688_detail", "lang": "zh+vi", "seed": "details"},
    "mono": {"queue": "queue_detail_zh", "engine": "1688_detail", "lang": "zh", "seed": None},
}


def queue_counts(settings, path: str) -> Counter:
    qm = QueueManager(settings.firebase_cred_path, settings.firebase_db_url, path, worker_id="runner")
    items = qm.ref.get() or {}
    return Counter(v.get("status") for v in items.values() if isinstance(v, dict))


def release_mine(settings, path: str) -> int:
    qm = QueueManager(settings.firebase_cred_path, settings.firebase_db_url, path, worker_id="runner")
    items = qm.ref.order_by_child("status").equal_to("processing").get() or {}
    n = 0
    for key, v in items.items():
        if v.get("worker_id") == settings.worker_id:
            qm.ref.child(key).update({"status": "pending", "worker_id": None, "locked_at": None})
            n += 1
    return n


def run(cmd: list[str], env: dict) -> int:
    print(f"\n>>> {' '.join(cmd)}  [{env.get('FIREBASE_QUEUE_PATH')}, {env.get('CRAWLER_ENGINE')}, {env.get('SITE_LANGUAGE')}]", flush=True)
    return subprocess.call(cmd, cwd=ROOT, env=env)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stages", nargs="+", choices=list(STAGES))
    ap.add_argument("--per-category", type=int, default=0, help="detail stage: cap products per category (0 = all)")
    ap.add_argument("--min-delay", default="4")
    ap.add_argument("--max-delay", default="8")
    ap.add_argument("--no-mirror", action="store_true", help="skip mirroring raw JSONL to the Hub after a stage")
    args = ap.parse_args()

    settings = load_settings()
    base_env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1",
                    PLAYWRIGHT_MIN_DELAY_SECONDS=args.min_delay, PLAYWRIGHT_MAX_DELAY_SECONDS=args.max_delay)

    for name in args.stages:
        stage = STAGES[name]
        env = dict(base_env, FIREBASE_QUEUE_PATH=stage["queue"], CRAWLER_ENGINE=stage["engine"], SITE_LANGUAGE=stage["lang"])
        print(f"\n===== STAGE {name} =====", flush=True)

        if stage["seed"] == "details":
            seed_cmd = [PYTHON, "scripts/seed_1688_details.py"]
            if args.per_category:
                seed_cmd += ["--per-category", str(args.per_category)]
            counts = queue_counts(settings, stage["queue"])
            if counts.get("pending", 0) + counts.get("processing", 0) == 0:
                run(seed_cmd, env)
            else:
                print(f"queue {stage['queue']} already has work ({dict(counts)}); not re-seeding", flush=True)

        while True:
            released = release_mine(settings, stage["queue"])
            if released:
                print(f"released {released} stuck items", flush=True)
            counts = queue_counts(settings, stage["queue"])
            print(f"queue {stage['queue']}: {dict(counts)}", flush=True)
            if counts.get("pending", 0) + counts.get("processing", 0) == 0:
                print(f"stage {name} complete", flush=True)
                break
            code = run([PYTHON, "main.py"], env)
            print(f"worker exited with code {code}; restarting in 60s", flush=True)
            time.sleep(60)

        if not args.no_mirror:
            run([PYTHON, "scripts/mirror_raw_to_hf.py"], env)


if __name__ == "__main__":
    main()
