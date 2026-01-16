from __future__ import annotations

import logging
from typing import Any

import httpx

from .config import ApiConfig
from .utils import RateLimiter, backoff_sleep

logger = logging.getLogger(__name__)


class ApiClient:
    def __init__(self, config: ApiConfig) -> None:
        self.config = config
        self._limiter = RateLimiter(config.min_interval_s)
        self._client = httpx.Client(timeout=config.timeout_s)

    def close(self) -> None:
        self._client.close()

    def fetch_markets(self) -> list[dict[str, Any]]:
        payload = self._get_json(self.config.markets_url)
        if isinstance(payload, list):
            return payload
        return payload.get("markets", [])

    def fetch_orderbook(self, market_id: str) -> dict[str, Any]:
        params = {"market_id": market_id}
        return self._get_json(self.config.orderbook_url, params=params)

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            self._limiter.wait()
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code >= 400:
                    raise httpx.HTTPStatusError(
                        f"HTTP {resp.status_code}", request=resp.request, response=resp
                    )
                return resp.json()
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                logger.warning(
                    "request failed", extra={"url": url, "attempt": attempt, "err": str(exc)}
                )
                if attempt < self.config.max_retries:
                    backoff_sleep(attempt)
        raise RuntimeError(f"request failed after retries: {url} ({last_error})")
