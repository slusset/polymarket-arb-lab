from __future__ import annotations

from typing import Any

import httpx

DEFAULT_EVENTS_URL = "https://gamma-api.polymarket.com/events"


class GammaClient:
    def __init__(self, events_url: str = DEFAULT_EVENTS_URL, timeout_s: float = 10.0) -> None:
        self._events_url = events_url
        self._client = httpx.Client(timeout=timeout_s)

    def fetch_events(self, limit: int | None = None) -> Any:
        params = {}
        if limit is not None:
            params["limit"] = limit
        response = self._client.get(self._events_url, params=params)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self._client.close()
