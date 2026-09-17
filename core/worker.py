from __future__ import annotations

import asyncio
import logging
from typing import List

from core.base_crawler_engine import BaseCrawlerEngine
from core.data_packager import DataPackager, PackagedBatch
from core.browser_1688 import AccountLanguageError, WallNotClearedError
from core.hf_uploader import HuggingFaceUploader
from core.models import QueueItem
from core.queue_manager import QueueManager
from core.raw_writer import RawJsonlWriter

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        worker_id: str,
        queue_manager: QueueManager,
        crawler_engine: BaseCrawlerEngine,
        uploader: HuggingFaceUploader,
        packager: DataPackager,
        raw_writer: RawJsonlWriter,
        claim_chunk_size: int = 50,
        idle_poll_seconds: float = 5.0,
    ) -> None:
        self.worker_id = worker_id
        self.queue_manager = queue_manager
        self.crawler_engine = crawler_engine
        self.uploader = uploader
        self.packager = packager
        self.raw_writer = raw_writer
        self.claim_chunk_size = claim_chunk_size
        self.idle_poll_seconds = idle_poll_seconds

    async def run(self, max_idle_polls: int = 3, max_consecutive_errors: int = 10) -> None:
        """Main loop. Exits after `max_idle_polls` consecutive empty claims
        AND the buffer has been flushed, i.e. the queue looks drained.

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

    async def _crawl_and_buffer(self, items: List[QueueItem]) -> None:
        products, failures = await self.crawler_engine.crawl_batch(items)

        if products:
            # Raw log first: once this returns the data survives an upload
            # failure or a crash further down the pipeline.
            await asyncio.to_thread(self.raw_writer.append, products)
            self.packager.add(products)
            logger.info(
                "Crawled %s records from %s queue items (buffer=%s/%s)",
                len(products),
                len(items),
                self.packager.pending_count,
                self.packager.batch_size,
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
        while True:
            batch = self.packager.pop_batch(force=force)
            if batch is None:
                return
            await self._upload(batch)

    async def _upload(self, batch: PackagedBatch) -> None:
        success = await asyncio.to_thread(
            self.uploader.upload_with_backoff, batch, self.worker_id
        )

        if success:
            logger.info(
                "Batch %s uploaded (%s products).", batch.batch_number, batch.record_count
            )
            self._cleanup_temp_file(batch)
        else:
            logger.error(
                "Batch %s upload failed after retries; parquet kept at %s for a manual "
                "re-upload (data is also in the raw JSONL).",
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
