from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import firebase_admin
from firebase_admin import credentials, db

from core.models import QueueItem

logger = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_FAILED = "failed"  # dead-letter: exceeded max attempts

QueueRecord = Dict[str, Any]  # a single queue node's stored fields


def _coerce_record_map(value: object) -> Dict[str, QueueRecord]:
    """Narrow the loosely-typed JSON value returned by `Query.get()`.

    firebase_admin ships plain source (no precise stub types), so static
    type checkers infer very broad/unhelpful unions (e.g. `object`,
    `list[Unknown]`) for its return values. Our queue schema always stores a
    mapping of push-id -> record dict, so this normalizes/narrows the result
    once here instead of scattering `# type: ignore` across every call site.
    In practice this is always a dict: Firebase only returns a JSON array
    instead of an object when every child key is a small sequential integer,
    which never happens with `push()`-generated ids.
    """
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    logger.warning("Unexpected queue query result shape: %s", type(value).__name__)
    return {}


def _coerce_record(value: object) -> Optional[QueueRecord]:
    """Same narrowing as `_coerce_record_map`, but for a single node's data
    (e.g. the result of a `Reference.transaction()` call)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    logger.warning("Unexpected queue record shape: %s", type(value).__name__)
    return None


class QueueManager:
    """Firebase Realtime Database backed implementation of the Central Queue."""

    _app_initialized = False

    def __init__(
        self,
        cred_path: str,
        db_url: str,
        queue_path: str = "queue",
        worker_id: str = "unknown",
        max_attempts: int = 3,
        lock_timeout_seconds: int = 600,
    ) -> None:
        self._ensure_firebase_app(cred_path, db_url)
        self.worker_id = worker_id
        self.queue_path = queue_path
        self.max_attempts = max_attempts
        self.lock_timeout_seconds = lock_timeout_seconds
        self.ref = db.reference(queue_path)

    @classmethod
    def _ensure_firebase_app(cls, cred_path: str, db_url: str) -> None:
        # firebase_admin raises if initialize_app() is called twice in the same
        # process, so guard it (relevant for tests / multiple QueueManager instances).
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {"databaseURL": db_url})
        cls._app_initialized = True

    def _run_indexed_query(self, query: db.Query) -> Dict[str, QueueRecord]:
        """Execute a `.order_by_child(...).get()` query, translating
        Firebase's "Index not defined" error into an actionable message.

        Realtime Database requires a `.indexOn` rule for any field used with
        `order_by_child()`/`equal_to()`; without it, the Admin SDK raises
        `InvalidArgumentError` (message contains ".indexOn") instead of a
        transient/network error. That makes it a permanent misconfiguration,
        not something worth retrying -- so we fail fast with a clear pointer
        to the fix instead of letting a cryptic Firebase message bubble up
        (or silently retrying forever under Worker's transient-error backoff).
        """
        try:
            return _coerce_record_map(query.get())
        except Exception as exc:
            if ".indexOn" in str(exc):
                raise RuntimeError(
                    "Firebase Realtime Database is missing the required index "
                    f"on '{self.queue_path}/status'. Apply the rules in "
                    "firebase/database.rules.json to your Firebase project "
                    "(Console -> Realtime Database -> Rules -> paste -> "
                    "Publish) then restart the worker. See the README section "
                    "'Thiết lập Firebase Realtime Database Rules' for details."
                ) from exc
            raise

    # ------------------------------------------------------------------ #
    # Producer-side helper (optional, useful for seeding the queue/tests)
    # ------------------------------------------------------------------ #
    def enqueue_urls(self, urls: Iterable[str], category: str) -> int:
        count = 0
        for url in urls:
            new_item: Dict[str, Any] = {
                "url": url,
                "category": category,
                "status": STATUS_PENDING,
                "worker_id": None,
                "attempts": 0,
                "locked_at": None,
                "updated_at": self._now_iso(),
            }
            # firebase_admin's Reference.push(self, value='') has no type
            # annotation, so type checkers infer `str` from the '' default
            # and flag any JSON-serializable dict/list argument as an error.
            # The SDK genuinely accepts any JSON-serializable value here
            # (see firebase_admin.db.Reference.push implementation).
            self.ref.push(new_item)  # type: ignore[arg-type]
            count += 1
        return count

    # ------------------------------------------------------------------ #
    # Claiming (locking)
    # ------------------------------------------------------------------ #
    def claim_urls(self, batch_size: int) -> List[QueueItem]:
        """Atomically claim up to `batch_size` pending URLs for this worker.

        Strategy: fetch a candidate window of pending items, then attempt an
        atomic transaction on each candidate node individually. The
        transaction only succeeds if the node is still `pending` by the time
        it runs, which is what prevents duplicate crawling across the 4
        worker machines.
        """
        claimed: List[QueueItem] = []

        query = (
            self.ref.order_by_child("status")
            .equal_to(STATUS_PENDING)
            .limit_to_first(max(batch_size * 3, batch_size))
        )
        candidates = self._run_indexed_query(query)

        for key in candidates:
            if len(claimed) >= batch_size:
                break
            item = self._try_lock(key)
            if item is not None:
                claimed.append(item)

        return claimed

    def _try_lock(self, key: str) -> Optional[QueueItem]:
        node_ref = self.ref.child(key)

        def _txn(current: Optional[QueueRecord]) -> Optional[QueueRecord]:
            if current is None or current.get("status") != STATUS_PENDING:
                # Abort: another worker already claimed it, or it changed state.
                return current
            current["status"] = STATUS_PROCESSING
            current["worker_id"] = self.worker_id
            current["locked_at"] = self._now_iso()
            current["updated_at"] = self._now_iso()
            return current

        try:
            raw_result = node_ref.transaction(_txn)
        except Exception as exc:  # network hiccups, etc.
            logger.warning("Transaction failed for key=%s: %s", key, exc)
            return None

        result = _coerce_record(raw_result)
        if not result or result.get("worker_id") != self.worker_id:
            return None
        if result.get("status") != STATUS_PROCESSING:
            return None

        return QueueItem(
            key=key,
            url=result["url"],
            category=result.get("category", "unknown"),
            attempts=result.get("attempts", 0),
        )

    # ------------------------------------------------------------------ #
    # Acknowledgement
    # ------------------------------------------------------------------ #
    def mark_done(self, key: str) -> None:
        """Only call this AFTER the item's records are safely on disk (raw JSONL)."""
        self.ref.child(key).update(
            {
                "status": STATUS_DONE,
                "finished_at": self._now_iso(),
                "updated_at": self._now_iso(),
            }
        )

    def release(self, key: str) -> None:
        """Release a URL back to the pool after a failed crawl/upload.

        If the item has exceeded `max_attempts`, it is routed to a `failed`
        dead-letter state instead of being retried forever.
        """
        node_ref = self.ref.child(key)

        def _txn(current: Optional[QueueRecord]) -> Optional[QueueRecord]:
            if current is None:
                return current
            attempts = current.get("attempts", 0) + 1
            current["attempts"] = attempts
            current["worker_id"] = None
            current["locked_at"] = None
            current["updated_at"] = self._now_iso()
            current["status"] = STATUS_FAILED if attempts >= self.max_attempts else STATUS_PENDING
            return current

        node_ref.transaction(_txn)

    # ------------------------------------------------------------------ #
    # Stale lock recovery (crash/network-partition resilience)
    # ------------------------------------------------------------------ #
    def recover_stale_locks(self) -> int:
        """Requeue items stuck in `processing` for longer than the configured
        lock timeout (e.g. a worker crashed mid-crawl without releasing)."""
        query = self.ref.order_by_child("status").equal_to(STATUS_PROCESSING)
        candidates = self._run_indexed_query(query)
        now = time.time()
        recovered = 0

        for key, item in candidates.items():
            locked_at = item.get("locked_at")
            if not locked_at:
                continue
            locked_ts = datetime.fromisoformat(locked_at).timestamp()
            if now - locked_ts > self.lock_timeout_seconds:
                self.release(key)
                recovered += 1

        return recovered

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
