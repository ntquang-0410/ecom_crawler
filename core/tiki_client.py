"""Polite, sequential client for Tiki's public JSON endpoints (no login needed)."""
from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
_RETRY_STATUS = {429, 500, 502, 503, 504}


class TikiClient:
    def __init__(
        self,
        timeout_seconds: int = 60,
        max_attempts: int = 3,
        min_delay_seconds: float = 1.0,
        max_delay_seconds: float = 2.0,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds

    def get_json(self, url: str) -> Optional[Dict[str, Any]]:
        """GET `url` after the pacing delay; None once every attempt failed."""
        for attempt in range(1, self.max_attempts + 1):
            time.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
            req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code not in _RETRY_STATUS:
                    logger.warning("HTTP %s on %s (not retried)", exc.code, url)
                    return None
                logger.warning("HTTP %s on %s (attempt %s/%s)", exc.code, url, attempt, self.max_attempts)
            except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                logger.warning("%s on %s (attempt %s/%s)", exc, url, attempt, self.max_attempts)
            time.sleep(2 ** attempt)
        return None
