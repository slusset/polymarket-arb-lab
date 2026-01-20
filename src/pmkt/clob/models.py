from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable


@dataclass(slots=True)
class OrderLevel:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class OrderBook:
    token_id: str
    market: str
    timestamp_ms: int
    bids: list[OrderLevel]
    asks: list[OrderLevel]
    tick_size: Decimal
    min_order_size: Decimal
    hash: str | None

    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.bids:
            return None
        level = max(self.bids, key=lambda level: level.price)
        return level.price, level.size

    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self.asks:
            return None
        level = min(self.asks, key=lambda level: level.price)
        return level.price, level.size

    def mid(self) -> Decimal | None:
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None
        return (best_bid[0] + best_ask[0]) / Decimal("2")

    def spread(self) -> Decimal | None:
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None
        return best_ask[0] - best_bid[0]

    def top_n(self, n: int) -> "OrderBook":
        bids_sorted = sorted(self.bids, key=lambda level: level.price, reverse=True)
        asks_sorted = sorted(self.asks, key=lambda level: level.price)
        return OrderBook(
            token_id=self.token_id,
            market=self.market,
            timestamp_ms=self.timestamp_ms,
            bids=bids_sorted[:n],
            asks=asks_sorted[:n],
            tick_size=self.tick_size,
            min_order_size=self.min_order_size,
            hash=self.hash,
        )


def sum_sizes(levels: Iterable[OrderLevel]) -> Decimal:
    total = Decimal("0")
    for level in levels:
        total += level.size
    return total
