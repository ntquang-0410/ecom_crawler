"""Abstract parser interface. Implement per target e-commerce site."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from core.models import ProductRecord, QueueItem


class BaseProductParser(ABC):
    """Extend this class per-site (e.g. ShopeeParser, TikiParser, LazadaParser)."""

    @abstractmethod
    def parse(self, html: str, item: QueueItem, worker_id: str) -> Optional[ProductRecord]:
        """Return a ProductRecord, or None if the page could not be parsed
        (e.g. product removed, CAPTCHA page, unexpected layout)."""
        raise NotImplementedError
