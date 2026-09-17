from __future__ import annotations

import logging
import random
import time
from typing import Optional

from huggingface_hub import HfApi
# `HfHubHTTPError` is defined in `huggingface_hub.errors`. It used to only be
# re-exported through `huggingface_hub.utils`, but that module has no
# `__all__`, so static type checkers (Pylance/pyright) flag importing it
# from there as "not exported". Import from the defining module instead.
from huggingface_hub.errors import HfHubHTTPError

from core.data_packager import PackagedBatch

logger = logging.getLogger(__name__)

# Status codes worth retrying: concurrent-commit conflicts and rate limits,
# plus transient 5xx server-side hiccups.
RETRYABLE_STATUS_CODES = {409, 429, 500, 502, 503, 504}


class HuggingFaceUploader:
    def __init__(
        self,
        token: str,
        repo_id: str,
        repo_type: str = "dataset",
        max_retries: int = 6,
        base_delay_seconds: float = 2.0,
        max_delay_seconds: float = 120.0,
        source: str = "web",
        run_id: str | None = None,
    ) -> None:
        self.api = HfApi(token=token)
        self.repo_id = repo_id
        self.repo_type = repo_type
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        # `source` is the bronze sub-folder ("bilingual_zh_vi", "mono_zh",
        # "search_zh"); `run_id` keeps a restarted worker from overwriting
        # the batches of its previous run (the batch counter starts at 1 in
        # every process).
        self.source = source
        self.run_id = run_id or time.strftime("%Y%m%dT%H%M%S")

    def build_repo_path(self, worker_id: str, batch: PackagedBatch) -> str:
        """Hive-style partitioned destination path, e.g.:
        data/bronze/bilingual_zh_vi/category=electronics/crawled_date=2026-09-17/
            worker_nhat_anh_20260917T171500_batch_001.parquet
        """
        filename = f"{worker_id}_{self.run_id}_batch_{batch.batch_number:03d}.parquet"
        return (
            f"data/bronze/{self.source}/category={batch.category}/"
            f"crawled_date={batch.crawled_date}/{filename}"
        )

    def upload_with_backoff(self, batch: PackagedBatch, worker_id: str) -> bool:
        """Upload a single packaged batch, retrying on 409/429/5xx with
        exponential backoff + jitter. Returns True on success, False if all
        retries were exhausted (caller must release the associated URLs).
        """
        path_in_repo = self.build_repo_path(worker_id, batch)

        for attempt in range(1, self.max_retries + 1):
            try:
                self.api.upload_file(
                    path_or_fileobj=str(batch.local_path),
                    path_in_repo=path_in_repo,
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                    commit_message=(
                        f"Add {batch.record_count} products from {worker_id} "
                        f"(batch {batch.batch_number:03d})"
                    ),
                )
                logger.info(
                    "Uploaded %s -> %s (attempt %s/%s)",
                    batch.local_path,
                    path_in_repo,
                    attempt,
                    self.max_retries,
                )
                return True

            except HfHubHTTPError as exc:
                status_code = self._status_code(exc)

                if status_code not in RETRYABLE_STATUS_CODES:
                    logger.error(
                        "Non-retryable HfHubHTTPError (status=%s) uploading %s: %s",
                        status_code,
                        path_in_repo,
                        exc,
                    )
                    return False

                if attempt == self.max_retries:
                    logger.error(
                        "Exhausted %s retries uploading %s (last status=%s): %s",
                        self.max_retries,
                        path_in_repo,
                        status_code,
                        exc,
                    )
                    return False

                delay = self._compute_backoff(attempt)
                logger.warning(
                    "Upload conflict/rate-limit (status=%s) for %s, "
                    "retrying in %.1fs (attempt %s/%s)",
                    status_code,
                    path_in_repo,
                    delay,
                    attempt,
                    self.max_retries,
                )
                time.sleep(delay)

            except Exception:
                # Non-HTTP errors (e.g. local filesystem, connection reset)
                # are unexpected here; do not silently retry forever.
                logger.exception("Unexpected error uploading %s", path_in_repo)
                return False

        return False

    def _compute_backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter, capped at max_delay_seconds."""
        raw_delay = self.base_delay_seconds * (2 ** (attempt - 1))
        capped = min(raw_delay, self.max_delay_seconds)
        return random.uniform(0, capped)

    @staticmethod
    def _status_code(exc: HfHubHTTPError) -> Optional[int]:
        response = getattr(exc, "response", None)
        return getattr(response, "status_code", None) if response is not None else None
