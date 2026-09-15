"""
Entrypoint: run this on each of the 4 worker machines.

Usage:
    python main.py

Reads WORKER_ID plus all other secrets from the local .env file (see
.env.example / README.md for setup instructions).
"""
from __future__ import annotations

import asyncio
import logging
import sys

from config import Settings, load_settings
from core.base_crawler_engine import BaseCrawlerEngine
from core.crawler_engine import CrawlerEngine
from core.data_packager import DataPackager
from core.hf_uploader import HuggingFaceUploader
from core.queue_manager import QueueManager
from core.worker import Worker
from parsers.generic_parser import GenericProductParser


def configure_logging(worker_id: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{worker_id}] %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def build_crawler_engine(settings: Settings) -> BaseCrawlerEngine:
    parser = GenericProductParser()

    if settings.crawler_engine == "playwright":
        # Imported lazily so `playwright` (an optional, heavier dependency
        # requiring a separate `playwright install chromium` step) is only
        # required when this engine is actually selected via CRAWLER_ENGINE.
        from core.playwright_crawler_engine import PlaywrightCrawlerEngine

        return PlaywrightCrawlerEngine(
            parser=parser,
            worker_id=settings.worker_id,
            max_concurrent_requests=settings.playwright_max_concurrent_pages,
            request_timeout_seconds=settings.request_timeout_seconds,
            max_fetch_attempts=settings.max_crawl_attempts,
            headless=settings.playwright_headless,
            min_delay_seconds=settings.playwright_min_delay_seconds,
            max_delay_seconds=settings.playwright_max_delay_seconds,
        )

    return CrawlerEngine(
        parser=parser,
        worker_id=settings.worker_id,
        max_concurrent_requests=settings.max_concurrent_requests,
        request_timeout_seconds=settings.request_timeout_seconds,
        max_fetch_attempts=settings.max_crawl_attempts,
    )


def build_worker(settings: Settings) -> Worker:
    queue_manager = QueueManager(
        cred_path=settings.firebase_cred_path,
        db_url=settings.firebase_db_url,
        queue_path=settings.firebase_queue_path,
        worker_id=settings.worker_id,
        max_attempts=settings.max_crawl_attempts,
        lock_timeout_seconds=settings.lock_timeout_seconds,
    )

    crawler_engine = build_crawler_engine(settings)

    uploader = HuggingFaceUploader(
        token=settings.hf_token,
        repo_id=settings.hf_repo_id,
        repo_type=settings.hf_repo_type,
        max_retries=settings.max_upload_retries,
        base_delay_seconds=settings.upload_base_delay_seconds,
        max_delay_seconds=settings.upload_max_delay_seconds,
    )

    packager = DataPackager(worker_id=settings.worker_id, batch_size=settings.batch_size)

    return Worker(
        worker_id=settings.worker_id,
        queue_manager=queue_manager,
        crawler_engine=crawler_engine,
        uploader=uploader,
        packager=packager,
        claim_chunk_size=settings.claim_chunk_size,
    )


def main() -> None:
    settings = load_settings()
    configure_logging(settings.worker_id)
    log = logging.getLogger(__name__)

    log.info(
        "Starting worker '%s' -> HF repo '%s' (engine=%s, batch_size=%s, concurrency=%s)",
        settings.worker_id,
        settings.hf_repo_id,
        settings.crawler_engine,
        settings.batch_size,
        settings.max_concurrent_requests
        if settings.crawler_engine == "aiohttp"
        else settings.playwright_max_concurrent_pages,
    )

    worker = build_worker(settings)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        log.info("Worker '%s' stopped by user (Ctrl+C).", settings.worker_id)
    except Exception:
        log.critical(
            "Worker '%s' crashed after exhausting its internal retry budget.",
            settings.worker_id,
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    main()
