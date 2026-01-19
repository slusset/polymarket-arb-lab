from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from .config import ApiConfig
from .utils import RateLimiter, backoff_sleep

logger = logging.getLogger(__name__)


class ApiClient:
    def __init__(self, config: ApiConfig) -> None:
        self.config = config
        self._limiter = RateLimiter(config.min_interval_s)
        timeout = httpx.Timeout(
            timeout=config.timeout_s,
            connect=config.timeout_s,
            read=config.timeout_s,
            write=config.timeout_s,
        )
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def fetch_markets(self) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        params["closed"] = "false"
        if self.config.start_date_min:
            params["start_date_min"] = self.config.start_date_min
        if self.config.end_date_min:
            params["end_date_min"] = self.config.end_date_min
        payload = self._get_json(self.config.markets_url, params=params or None)
        if isinstance(payload, list):
            return payload
        return payload.get("markets", [])

    def fetch_orderbook(self, token_id: str) -> dict[str, Any]:
        params = {"token_id": token_id}
        return self._get_json(self.config.orderbook_url, params=params)

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            self._limiter.wait()
            start = time.monotonic()
            try:
                resp = self._client.get(url, params=params)
                latency_ms = int((time.monotonic() - start) * 1000)
                if resp.status_code >= 400:
                    snippet = resp.text[:200].replace("\n", " ").replace("\r", " ")
                    logger.warning(
                        "http error",
                        extra={
                            "url": url,
                            "status_code": resp.status_code,
                            "latency_ms": latency_ms,
                            "body_snippet": snippet,
                            "params": params,
                            "attempt": attempt,
                        },
                    )
                    if resp.status_code == 429 or resp.status_code >= 500:
                        raise httpx.HTTPStatusError(
                            f"HTTP {resp.status_code}", request=resp.request, response=resp
                        )
                    resp.raise_for_status()
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "http ok",
                        extra={
                            "url": url,
                            "status_code": resp.status_code,
                            "latency_ms": latency_ms,
                            "params": params,
                        },
                    )
                return resp.json()
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                    status_code = exc.response.status_code
                    if status_code < 500 and status_code != 429:
                        raise
                logger.warning(
                    "request failed", extra={"url": url, "attempt": attempt, "err": str(exc)}
                )
                if attempt < self.config.max_retries:
                    backoff_sleep(attempt)
        raise RuntimeError(f"request failed after retries: {url} ({last_error})")
