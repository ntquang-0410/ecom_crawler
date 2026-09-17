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
    # Parquet mirror of each batch on the Hub (raw JSONL is written regardless).
    hf_upload_enabled: bool = field(default_factory=lambda: _get_bool("HF_UPLOAD_ENABLED", True))

    # Firebase Realtime Database (Central Queue)
    firebase_cred_path: str = field(
        default_factory=lambda: _get_env("FIREBASE_CRED_PATH", required=True)
    )
    firebase_db_url: str = field(
        default_factory=lambda: _get_env("FIREBASE_DB_URL", required=True)
    )
    firebase_queue_path: str = field(
        default_factory=lambda: _get_env("FIREBASE_QUEUE_PATH", "queue_search_zh")
    )
    # Registry of products the bilingual stage found without a Vietnamese
    # version (their rows go to the mono_zh sink).
    firebase_mono_queue_path: str = field(
        default_factory=lambda: _get_env("FIREBASE_MONO_QUEUE_PATH", "queue_detail_zh")
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

    # Crawler engine selection:
    #   "aiohttp"      lightweight HTTP client (blocked by Alibaba's anti-bot)
    #   "playwright"   bundled headless Chromium (also blocked by Alibaba)
    #   "1688_search"  real Chrome with a persistent profile calling 1688's
    #                  search API (titles, 60 per queue item)
    #   "1688_detail"  same browser, product detail pages (attributes,
    #                  description) -- one product per queue item
    crawler_engine: str = field(
        default_factory=lambda: _get_env("CRAWLER_ENGINE", "1688_search")
    )
    detail_fetch_description: bool = field(
        default_factory=lambda: _get_bool("DETAIL_FETCH_DESCRIPTION", True)
    )
    # Content language requested from 1688: "zh" = sellers' original Chinese,
    # "vi" = 1688's machine translation, "zh+vi" (detail engine only) = both
    # in one pass, one bilingual row per product.
    site_language: str = field(default_factory=lambda: _get_env("SITE_LANGUAGE", "zh"))
    browser_profile_dir: str = field(
        default_factory=lambda: _get_env("BROWSER_PROFILE_DIR", ".pw_profile")
    )
    # How long to wait for a person to solve a CAPTCHA / log in before the
    # worker gives up and stops.
    human_wait_seconds: int = field(default_factory=lambda: _get_int("HUMAN_WAIT_SECONDS", 300))

    # Raw, append-only JSONL output (CLAUDE.md section 4)
    raw_data_dir: str = field(default_factory=lambda: _get_env("RAW_DATA_DIR", "data/raw"))
    raw_shard_max_mb: int = field(default_factory=lambda: _get_int("RAW_SHARD_MAX_MB", 200))
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
        if self.crawler_engine not in {"aiohttp", "playwright", "1688_search", "1688_detail"}:
            raise ValueError(
                f"CRAWLER_ENGINE '{self.crawler_engine}' is not recognized. "
                f"Expected 'aiohttp', 'playwright', '1688_search' or '1688_detail'."
            )
        if self.site_language not in {"zh", "vi", "zh+vi"}:
            raise ValueError(f"SITE_LANGUAGE '{self.site_language}' must be 'zh', 'vi' or 'zh+vi'.")
        if self.site_language == "zh+vi" and self.crawler_engine != "1688_detail":
            raise ValueError("SITE_LANGUAGE=zh+vi is only supported by CRAWLER_ENGINE=1688_detail.")
        if "/" not in self.hf_repo_id or self.hf_repo_id.startswith(("git@", "http")):
            raise ValueError(
                f"HF_REPO_ID '{self.hf_repo_id}' must be a plain '<user>/<name>' id, "
                f"not a git/ssh URL."
            )
        if not Path(self.firebase_cred_path).is_file():
            raise FileNotFoundError(
                f"Firebase credential file not found at '{self.firebase_cred_path}'. "
                f"Check FIREBASE_CRED_PATH in your .env."
            )


def load_settings() -> Settings:
    """Factory so callers can reload/validate settings explicitly (e.g. in tests)."""
    return Settings()
