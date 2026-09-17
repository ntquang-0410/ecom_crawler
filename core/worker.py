from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from core.base_crawler_engine import BaseCrawlerEngine
from core.data_packager import DataPackager, PackagedBatch
from core.browser_1688 import AccountLanguageError, WallNotClearedError
from core.hf_uploader import HuggingFaceUploader
from core.models import ProductRecord, QueueItem
from core.queue_manager import QueueManager
from core.raw_writer import RawJsonlWriter
from core.textnorm import normalize_record

logger = logging.getLogger(__name__)


@dataclass
class Sink:
    """Where one kind of record ends up: its raw JSONL shard family, its
    parquet batcher and (optionally) its Hub folder."""

    name: str
    raw_writer: RawJsonlWriter
    packager: DataPackager
    uploader: Optional[HuggingFaceUploader]  # None = raw JSONL only


class Worker:
    def __init__(
        self,
        worker_id: str,
        queue_manager: QueueManager,
        crawler_engine: BaseCrawlerEngine,
        sinks: Dict[str, Sink],
        claim_chunk_size: int = 50,
        idle_poll_seconds: float = 5.0,
        mono_registry: Optional[QueueManager] = None,
    ) -> None:
        """`sinks` has a "default" entry and, for the bilingual detail stage,
        "bilingual" + "mono". `mono_registry` is the queue that lists the
        products found to have no Vietnamese version."""
        self.worker_id = worker_id
        self.queue_manager = queue_manager
        self.crawler_engine = crawler_engine
        self.sinks = sinks
        self.claim_chunk_size = claim_chunk_size
        self.idle_poll_seconds = idle_poll_seconds
        self.mono_registry = mono_registry

    async def run(self, max_idle_polls: int = 3, max_consecutive_errors: int = 10) -> None:
        """Main loop. Exits after `max_idle_polls` consecutive empty claims
        AND the buffers have been flushed, i.e. the queue looks drained.

        Each iteration is guarded by a broad try/except with exponential
        backoff: a transient Firebase/network blip must NOT crash the whole
        worker process (mirrors the resiliency already required for Hugging
        Face uploads). Only after `max_consecutive_errors` in a row -- which
        signals a persistent, non-transient problem such as bad credentials
        -- do we give up and let the exception propagate.
        """
        idle_rounds = 0
        consecutive_errors = 0

        try:
            while idle_rounds < max_idle_polls:
                try:
                    await asyncio.to_thread(self.queue_manager.recover_stale_locks)

                    items = await asyncio.to_thread(
                        self.queue_manager.claim_urls, self.claim_chunk_size
                    )

                    if not items:
                        idle_rounds += 1
                        await self._flush_if_any(force=True)
                        logger.info(
                            "No pending URLs (idle round %s/%s). Sleeping %ss.",
                            idle_rounds,
                            max_idle_polls,
                            self.idle_poll_seconds,
                        )
                        await asyncio.sleep(self.idle_poll_seconds)
                        consecutive_errors = 0
                        continue

                    idle_rounds = 0
                    await self._crawl_and_buffer(items)
                    await self._flush_if_any(force=False)
                    consecutive_errors = 0

                except (WallNotClearedError, AccountLanguageError) as exc:
                    logger.critical("Worker %s stopping: %s", self.worker_id, exc)
                    raise
                except Exception:
                    consecutive_errors += 1
                    if consecutive_errors > max_consecutive_errors:
                        logger.critical(
                            "Worker %s hit %s consecutive errors; giving up.",
                            self.worker_id,
                            consecutive_errors,
                        )
                        raise
                    backoff = min(2 ** consecutive_errors, 60)
                    logger.exception(
                        "Unexpected error in worker loop (consecutive_errors=%s/%s). "
                        "Retrying in %ss.",
                        consecutive_errors,
                        max_consecutive_errors,
                        backoff,
                    )
                    await asyncio.sleep(backoff)

            # Final drain in case something is left buffered.
            await self._flush_if_any(force=True)
            logger.info("Worker %s finished: queue appears drained.", self.worker_id)
        finally:
            # Always release the crawler engine's resources (e.g. a
            # Playwright browser process), whether the loop drained
            # normally or a fatal error propagated out above.
            await self.crawler_engine.close()

    def _route(self, record: ProductRecord) -> Sink:
        if "bilingual" in self.sinks and "mono" in self.sinks:
            return self.sinks["bilingual" if record.has_vi else "mono"]
        return self.sinks["default"]

    async def _crawl_and_buffer(self, items: List[QueueItem]) -> None:
        products, failures = await self.crawler_engine.crawl_batch(items)

        if products:
            routed: Dict[str, List[ProductRecord]] = {}
            for record in products:
                normalize_record(record)
                routed.setdefault(self._route(record).name, []).append(record)
            # Raw log first: once this returns the data survives an upload
            # failure or a crash further down the pipeline.
            for name, records in routed.items():
                sink = self.sinks[name]
                await asyncio.to_thread(sink.raw_writer.append, records)
                sink.packager.add(records)
            logger.info(
                "Crawled %s records from %s queue items (%s)",
                len(products),
                len(items),
                ", ".join(f"{n}={len(r)}" for n, r in routed.items()),
            )
            if self.mono_registry is not None:
                for record in routed.get("mono", []):
                    await asyncio.to_thread(
                        self.mono_registry.register_done, record.url, record.category, record.product_id
                    )

        # The raw JSONL is the source of truth, so every item that fetched
        # fine is acknowledged right here. The Hugging Face upload below is
        # only a mirror: its failure must never send pages back to the
        # queue, which would re-spend the site's rate budget on data we
        # already hold.
        failed_keys = {item.key for item in failures}
        for item in items:
            if item.key not in failed_keys:
                await asyncio.to_thread(self.queue_manager.mark_done, item.key)

        for item in failures:
            await asyncio.to_thread(self.queue_manager.release, item.key)
        if failures:
            logger.warning("Released %s failed URLs back to the queue.", len(failures))

    async def _flush_if_any(self, force: bool) -> None:
        for sink in self.sinks.values():
            while True:
                batch = sink.packager.pop_batch(force=force)
                if batch is None:
                    break
                await self._upload(sink, batch)

    async def _upload(self, sink: Sink, batch: PackagedBatch) -> None:
        if sink.uploader is None:
            self._cleanup_temp_file(batch)
            return
        success = await asyncio.to_thread(
            sink.uploader.upload_with_backoff, batch, self.worker_id
        )

        if success:
            logger.info(
                "[%s] batch %s uploaded (%s products).", sink.name, batch.batch_number, batch.record_count
            )
            self._cleanup_temp_file(batch)
        else:
            logger.error(
                "[%s] batch %s upload failed after retries; parquet kept at %s for a manual "
                "re-upload (data is also in the raw JSONL).",
                sink.name,
                batch.batch_number,
                batch.local_path,
            )

    @staticmethod
    def _cleanup_temp_file(batch: PackagedBatch) -> None:
        """Best-effort local temp-file cleanup after the upload attempt
        (success or exhausted retries) so the tmp dir doesn't grow unbounded."""
        try:
            batch.local_path.unlink(missing_ok=True)
        except OSError:
            logger.warning("Could not remove temp file %s", batch.local_path)
