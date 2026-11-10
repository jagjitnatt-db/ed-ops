"""A tiny in-process TTL cache that protects Lakebase when many users have the app open."""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class TtlCache:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.seconds, self.clock = seconds, clock
        self._items: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, fn: Callable[[], Any]) -> Any:
        if self.seconds <= 0:
            return fn()
        now = self.clock()
        with self._lock:
            hit = self._items.get(key)
            if hit and now - hit[0] < self.seconds:
                return hit[1]
        value = fn()
        with self._lock:
            self._items[key] = (now, value)
        return value
