from __future__ import annotations

from typing import Any

import httpx

DEFAULT_EVENTS_URL = "https://gamma-api.polymarket.com/events"


class GammaClient:
    def __init__(self, events_url: str = DEFAULT_EVENTS_URL, timeout_s: float = 10.0) -> None:
        self._events_url = events_url
        self._client = httpx.Client(timeout=timeout_s)

    def fetch_events(
        self,
        *,
        closed: bool | None = None,
        limit: int | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> Any:
        params: dict[str, str] = {}
        if closed is not None:
            params["closed"] = "true" if closed else "false"
        if limit is not None:
            params["limit"] = str(limit)
        if order:
            params["order"] = order
        if ascending is not None:
            params["ascending"] = "true" if ascending else "false"
        response = self._client.get(self._events_url, params=params or None)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self._client.close()
