"""
DataPackager: accumulates ProductRecords and flushes full (or final partial)
batches to local `.parquet` files, ready for HuggingFaceUploader.
"""
from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd

from core.models import ProductRecord

logger = logging.getLogger(__name__)


@dataclass
class PackagedBatch:
    local_path: Path
    category: str
    crawled_date: str
    batch_number: int
    record_count: int


class DataPackager:
    """Buffers products in memory (RAM) until `batch_size` is reached, then
    serializes them to a `.parquet` file on a tmp path for fast PyTorch-side
    reads downstream."""

    def __init__(self, worker_id: str, batch_size: int = 1000, tmp_dir: str | None = None):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.tmp_dir = Path(tmp_dir) if tmp_dir else Path(tempfile.gettempdir()) / "ecom_crawler_batches"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self._buffer: List[ProductRecord] = []
        self._batch_counter = 0

    @property
    def pending_count(self) -> int:
        return len(self._buffer)

    def add(self, records: List[ProductRecord]) -> None:
        self._buffer.extend(records)

    def is_full(self) -> bool:
        return len(self._buffer) >= self.batch_size

    def pop_batch(self, force: bool = False) -> PackagedBatch | None:
        """Pop up to `batch_size` records and write them to a parquet file.

        If `force` is True, flush whatever is left even if below batch_size
        (used when the queue is drained so no data is stranded in memory).
        """
        if not self._buffer:
            return None
        if not force and len(self._buffer) < self.batch_size:
            return None

        chunk = self._buffer[: self.batch_size]
        self._buffer = self._buffer[self.batch_size :]
        self._batch_counter += 1

        df = pd.DataFrame([r.to_row() for r in chunk])
        # Variable dict shapes break columnar inference; store as a JSON string.
        df["meta"] = df["meta"].map(lambda m: json.dumps(m, ensure_ascii=False))

        # Category/date are assumed uniform per batch (a worker typically
        # drains one category/day at a time); use the majority values.
        category = chunk[0].category if chunk else "unknown"
        crawled_date = chunk[0].crawl_time[:10] if chunk else ""

        filename = f"{self.worker_id}_batch_{self._batch_counter:03d}_{uuid.uuid4().hex[:8]}.parquet"
        local_path = self.tmp_dir / filename
        df.to_parquet(local_path, engine="pyarrow", index=False)

        logger.info("Packaged %s records into %s", len(chunk), local_path)

        return PackagedBatch(
            local_path=local_path,
            category=category,
            crawled_date=crawled_date,
            batch_number=self._batch_counter,
            record_count=len(chunk),
        )
