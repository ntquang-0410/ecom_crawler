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

from pathlib import Path

from config import Settings, load_settings
from core.base_crawler_engine import BaseCrawlerEngine
from core.crawler_engine import CrawlerEngine
from core.data_packager import DataPackager
from core.hf_uploader import HuggingFaceUploader
from core.queue_manager import QueueManager
from core.raw_writer import RawJsonlWriter
from core.worker import Sink, Worker
from parsers.generic_parser import GenericProductParser


def configure_logging(worker_id: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{worker_id}] %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def build_crawler_engine(settings: Settings) -> BaseCrawlerEngine:
    if settings.crawler_engine == "1688_detail":
        from core.engine_1688_detail import Detail1688Engine
        from parsers.parser_1688_detail import Detail1688Parser

        return Detail1688Engine(
            parser=Detail1688Parser(lang=settings.site_language),
            worker_id=settings.worker_id,
            profile_dir=Path(settings.browser_profile_dir),
            request_timeout_seconds=settings.request_timeout_seconds,
            max_fetch_attempts=settings.max_crawl_attempts,
            min_delay_seconds=settings.playwright_min_delay_seconds,
            max_delay_seconds=settings.playwright_max_delay_seconds,
            human_wait_seconds=settings.human_wait_seconds,
            fetch_description=settings.detail_fetch_description,
            site_language=settings.site_language,
        )

    if settings.crawler_engine == "1688_search":
        from core.engine_1688_search import Search1688Engine
        from parsers.parser_1688_search import Search1688Parser

        return Search1688Engine(
            parser=Search1688Parser(lang=settings.site_language),
            worker_id=settings.worker_id,
            profile_dir=Path(settings.browser_profile_dir),
            request_timeout_seconds=settings.request_timeout_seconds,
            max_fetch_attempts=settings.max_crawl_attempts,
            min_delay_seconds=settings.playwright_min_delay_seconds,
            max_delay_seconds=settings.playwright_max_delay_seconds,
            human_wait_seconds=settings.human_wait_seconds,
            site_language=settings.site_language,
        )

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

    def make_sink(name: str, site: str, lang: str, upload: bool) -> Sink:
        uploader = None
        if upload:
            uploader = HuggingFaceUploader(
                token=settings.hf_token,
                repo_id=settings.hf_repo_id,
                repo_type=settings.hf_repo_type,
                max_retries=settings.max_upload_retries,
                base_delay_seconds=settings.upload_base_delay_seconds,
                max_delay_seconds=settings.upload_max_delay_seconds,
                source=name,
            )
        return Sink(
            name=name,
            raw_writer=RawJsonlWriter(
                raw_dir=Path(settings.raw_data_dir),
                site=site,
                lang=lang,
                worker_id=settings.worker_id,
                shard_max_bytes=settings.raw_shard_max_mb * 1024 * 1024,
            ),
            packager=DataPackager(worker_id=settings.worker_id, batch_size=settings.batch_size),
            uploader=uploader,
        )

    # Where each stage's rows go (raw shard family / bronze folder on the Hub):
    #   search        1688search_zh_*.jsonl   raw only (titles are an intermediate
    #                                         product; the detail stage re-emits them)
    #   detail zh+vi  1688_bilingual_*.jsonl  data/bronze/bilingual_zh_vi/
    #                 1688_mono_zh_*.jsonl    data/bronze/mono_zh/
    #   detail zh     1688_mono_zh_*.jsonl    data/bronze/mono_zh/
    mono_registry = None
    if settings.crawler_engine == "1688_detail" and settings.site_language == "zh+vi":
        sinks = {
            "bilingual": make_sink("bilingual_zh_vi", "1688", "bilingual", upload=settings.hf_upload_enabled),
            "mono": make_sink("mono_zh", "1688", "mono_zh", upload=settings.hf_upload_enabled),
        }
        mono_registry = QueueManager(
            cred_path=settings.firebase_cred_path,
            db_url=settings.firebase_db_url,
            queue_path=settings.firebase_mono_queue_path,
            worker_id=settings.worker_id,
        )
    elif settings.crawler_engine == "1688_detail":
        sinks = {"default": make_sink("mono_zh", "1688", "mono_zh", upload=settings.hf_upload_enabled)}
    elif settings.crawler_engine == "1688_search":
        sinks = {"default": make_sink("search_zh", "1688search", settings.site_language, upload=False)}
    else:
        sinks = {"default": make_sink("web", "web", settings.site_language, upload=settings.hf_upload_enabled)}

    return Worker(
        worker_id=settings.worker_id,
        queue_manager=queue_manager,
        crawler_engine=crawler_engine,
        sinks=sinks,
        claim_chunk_size=settings.claim_chunk_size,
        mono_registry=mono_registry,
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
