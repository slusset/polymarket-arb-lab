from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from .models import OrderBook, OrderLevel

DEFAULT_BOOK_URL = "https://clob.polymarket.com/book"


class ClobClient:
    def __init__(self, book_url: str = DEFAULT_BOOK_URL, timeout_s: float = 10.0) -> None:
        self._book_url = book_url
        self._client = httpx.Client(timeout=timeout_s)

    def fetch_book(self, token_id: str) -> dict[str, Any]:
        response = self._client.get(self._book_url, params={"token_id": token_id})
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected order book payload")
        return data

    def get_order_book(self, token_id: str) -> OrderBook:
        payload = self.fetch_book(token_id)
        bids = _parse_levels(payload.get("bids", []))
        asks = _parse_levels(payload.get("asks", []))
        market = str(payload.get("market") or payload.get("conditionId") or "")
        timestamp_ms = int(payload.get("timestamp") or payload.get("timestampMs") or 0)
        tick_size = Decimal(str(payload.get("tick_size") or payload.get("tickSize") or "0"))
        min_order_size = Decimal(
            str(payload.get("min_order_size") or payload.get("minOrderSize") or "0")
        )
        book_hash = payload.get("hash")
        return OrderBook(
            token_id=token_id,
            market=market,
            timestamp_ms=timestamp_ms,
            bids=bids,
            asks=asks,
            tick_size=tick_size,
            min_order_size=min_order_size,
            hash=str(book_hash) if book_hash else None,
        )

    def close(self) -> None:
        self._client.close()


def _parse_levels(raw_levels: Any) -> list[OrderLevel]:
    levels: list[OrderLevel] = []
    if not isinstance(raw_levels, list):
        return levels
    for item in raw_levels:
        if not isinstance(item, dict):
            continue
        price = item.get("price")
        size = item.get("size")
        if price is None or size is None:
            continue
        levels.append(OrderLevel(price=Decimal(str(price)), size=Decimal(str(size))))
    return levels
