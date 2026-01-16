from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dumps_compact(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=True)


@dataclass
class RateLimiter:
    min_interval_s: float
    _last_ts: float = 0.0

    def wait(self) -> None:
        if self.min_interval_s <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_ts
        if elapsed < self.min_interval_s:
            time.sleep(self.min_interval_s - elapsed)
        self._last_ts = time.monotonic()


def backoff_sleep(attempt: int, base_s: float = 0.5, cap_s: float = 8.0) -> None:
    delay = min(cap_s, base_s * (2 ** (attempt - 1)))
    jitter = random.uniform(0, delay * 0.25)
    time.sleep(delay + jitter)


def parse_quantities(raw: str) -> list[float]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    quantities: list[float] = []
    for part in parts:
        try:
            quantities.append(float(part))
        except ValueError:
            continue
    return quantities


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
