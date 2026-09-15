from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load the nearest .env file (does not override real OS env vars already set).
load_dotenv()

VALID_WORKER_IDS = {
    "worker_quang",
    "worker_nhat_anh",
    "worker_huy",
}


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if not value:
        if required:
            raise EnvironmentError(
                f"Missing required environment variable '{name}'. "
                f"Copy .env.example to .env and fill it in."
            )
        raise ValueError(
            f"_get_env('{name}') was called with required=False and no "
            f"default; pass a non-None default or set required=True."
        )
    return value


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # Identity
    worker_id: str = field(default_factory=lambda: _get_env("WORKER_ID", required=True))

    # Hugging Face Hub
    hf_token: str = field(default_factory=lambda: _get_env("HF_TOKEN", required=True))
    hf_repo_id: str = field(default_factory=lambda: _get_env("HF_REPO_ID", required=True))
    hf_repo_type: str = field(default_factory=lambda: _get_env("HF_REPO_TYPE", "dataset"))

    # Firebase Realtime Database (Central Queue)
    firebase_cred_path: str = field(
        default_factory=lambda: _get_env("FIREBASE_CRED_PATH", required=True)
    )
    firebase_db_url: str = field(
        default_factory=lambda: _get_env("FIREBASE_DB_URL", required=True)
    )
    firebase_queue_path: str = field(
        default_factory=lambda: _get_env("FIREBASE_QUEUE_PATH", "queue")
    )

    # Crawler tuning
    batch_size: int = field(default_factory=lambda: _get_int("BATCH_SIZE", 1000))
    claim_chunk_size: int = field(default_factory=lambda: _get_int("CLAIM_CHUNK_SIZE", 50))
    max_concurrent_requests: int = field(
        default_factory=lambda: _get_int("MAX_CONCURRENT_REQUESTS", 20)
    )
    request_timeout_seconds: int = field(
        default_factory=lambda: _get_int("REQUEST_TIMEOUT_SECONDS", 30)
    )
    max_crawl_attempts: int = field(default_factory=lambda: _get_int("MAX_CRAWL_ATTEMPTS", 3))
    default_category: str = field(
        default_factory=lambda: _get_env("DEFAULT_CATEGORY", "electronics")
    )

    # Crawler engine selection: "aiohttp" (default, lightweight HTTP client)
    # or "playwright" (real headless Chromium -- for sites that require JS
    # rendering or block plain HTTP clients via bot-detection).
    crawler_engine: str = field(
        default_factory=lambda: _get_env("CRAWLER_ENGINE", "aiohttp")
    )
    playwright_headless: bool = field(
        default_factory=lambda: _get_bool("PLAYWRIGHT_HEADLESS", True)
    )
    # Each Playwright request is a full browser tab (far heavier than an
    # aiohttp socket), so this should stay much lower than
    # `max_concurrent_requests`.
    playwright_max_concurrent_pages: int = field(
        default_factory=lambda: _get_int("PLAYWRIGHT_MAX_CONCURRENT_PAGES", 4)
    )
    playwright_min_delay_seconds: float = field(
        default_factory=lambda: float(_get_env("PLAYWRIGHT_MIN_DELAY_SECONDS", "1.0"))
    )
    playwright_max_delay_seconds: float = field(
        default_factory=lambda: float(_get_env("PLAYWRIGHT_MAX_DELAY_SECONDS", "3.0"))
    )

    # Upload resiliency
    max_upload_retries: int = field(default_factory=lambda: _get_int("MAX_UPLOAD_RETRIES", 6))
    upload_base_delay_seconds: float = field(
        default_factory=lambda: float(_get_env("UPLOAD_BASE_DELAY_SECONDS", "2"))
    )
    upload_max_delay_seconds: float = field(
        default_factory=lambda: float(_get_env("UPLOAD_MAX_DELAY_SECONDS", "120"))
    )

    # Stale lock recovery
    lock_timeout_seconds: int = field(default_factory=lambda: _get_int("LOCK_TIMEOUT_SECONDS", 600))

    def __post_init__(self) -> None:
        if self.worker_id not in VALID_WORKER_IDS:
            raise ValueError(
                f"WORKER_ID '{self.worker_id}' is not recognized. "
                f"Expected one of: {sorted(VALID_WORKER_IDS)}"
            )
        if self.crawler_engine not in {"aiohttp", "playwright"}:
            raise ValueError(
                f"CRAWLER_ENGINE '{self.crawler_engine}' is not recognized. "
                f"Expected 'aiohttp' or 'playwright'."
            )
        if not Path(self.firebase_cred_path).is_file():
            raise FileNotFoundError(
                f"Firebase credential file not found at '{self.firebase_cred_path}'. "
                f"Check FIREBASE_CRED_PATH in your .env."
            )


def load_settings() -> Settings:
    """Factory so callers can reload/validate settings explicitly (e.g. in tests)."""
    return Settings()
