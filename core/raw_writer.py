"""
RawJsonlWriter: append-only JSONL log of every crawled record, written to
`data/raw/` BEFORE anything else happens to the record (packaging, upload).

This is the immutable source of truth (CLAUDE.md section 4): if the upload
fails, or a cleaning rule turns out wrong weeks later, everything can be
rebuilt from these files without re-crawling.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from core.models import ProductRecord

logger = logging.getLogger(__name__)


class RawJsonlWriter:
    def __init__(
        self,
        raw_dir: Path,
        site: str,
        lang: str,
        worker_id: str,
        shard_max_bytes: int = 200 * 1024 * 1024,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.site = site
        self.lang = lang
        self.worker_id = worker_id
        self.shard_max_bytes = shard_max_bytes
        self._current: Optional[Path] = None

    def _shard_path(self) -> Path:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        prefix = f"{self.site}_{self.lang}_{today}_{self.worker_id}_"

        if (
            self._current is not None
            and self._current.name.startswith(prefix)
            and self._current.exists()
            and self._current.stat().st_size < self.shard_max_bytes
        ):
            return self._current

        existing = sorted(self.raw_dir.glob(f"{prefix}*.jsonl"))
        if existing and existing[-1].stat().st_size < self.shard_max_bytes:
            self._current = existing[-1]
        else:
            next_index = len(existing) + 1
            self._current = self.raw_dir / f"{prefix}{next_index:03d}.jsonl"
        return self._current

    def append(self, records: Iterable[ProductRecord]) -> int:
        rows = [r.to_row() for r in records]
        if not rows:
            return 0
        path = self._shard_path()
        with path.open("a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        logger.info("Appended %s records to %s", len(rows), path.name)
        return len(rows)
