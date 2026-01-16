from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .models import MarketClassification, MarketMetadata, OrderBookTop


@dataclass(frozen=True)
class FeeModel:
    bps_per_leg: float = 0.0
    pct_per_leg: float = 0.0

    def fee_for_leg(self, price: float) -> float:
        return price * (self.bps_per_leg / 10000.0 + self.pct_per_leg / 100.0)

    def total_fee(self, ask_yes: float, ask_no: float) -> float:
        return self.fee_for_leg(ask_yes) + self.fee_for_leg(ask_no)


def classify_market(market: MarketMetadata) -> MarketClassification:
    outcomes = [o.strip().lower() for o in market.outcomes]
    outcomes_set = {o for o in outcomes if o}
    semantics = "unknown"
    if outcomes_set == {"yes", "no"}:
        semantics = "binary_yes_no"
    status = market.status.lower()
    is_open = status in {"open", "active", "trading"} or market.raw.get("active") is True
    return MarketClassification(market=market, semantics=semantics, is_open=is_open)


def select_markets(
    markets: Iterable[MarketMetadata],
    min_volume: float,
    max_markets: int | None,
) -> list[MarketMetadata]:
    eligible: list[MarketMetadata] = []
    for market in markets:
        classification = classify_market(market)
        if not classification.is_open:
            continue
        if classification.semantics != "binary_yes_no":
            continue
        if market.volume < min_volume:
            continue
        if not market.market_id:
            continue
        eligible.append(market)
    if max_markets is not None:
        return eligible[: max_markets]
    return eligible


def best_ask_from_levels(levels: Any) -> tuple[float | None, float | None]:
    if not isinstance(levels, list) or not levels:
        return None, None
    first = levels[0]
    if isinstance(first, dict):
        price = first.get("price") or first.get("rate")
        size = first.get("size") or first.get("quantity") or first.get("amount")
        try:
            return float(price), float(size)
        except (TypeError, ValueError):
            return None, None
    if isinstance(first, list) and len(first) >= 2:
        try:
            return float(first[0]), float(first[1])
        except (TypeError, ValueError):
            return None, None
    return None, None


def parse_orderbook_top(data: dict[str, Any]) -> OrderBookTop:
    yes_best, yes_size = None, None
    no_best, no_size = None, None

    if "yes" in data and isinstance(data.get("yes"), dict):
        yes_best, yes_size = best_ask_from_levels(data["yes"].get("asks"))
    if "no" in data and isinstance(data.get("no"), dict):
        no_best, no_size = best_ask_from_levels(data["no"].get("asks"))

    if yes_best is None or no_best is None:
        books = data.get("orderbooks") or data.get("books") or []
        if isinstance(books, list):
            for book in books:
                outcome = str(book.get("outcome") or "").lower()
                asks = book.get("asks")
                if outcome == "yes" and yes_best is None:
                    yes_best, yes_size = best_ask_from_levels(asks)
                if outcome == "no" and no_best is None:
                    no_best, no_size = best_ask_from_levels(asks)

    return OrderBookTop(
        yes_best_ask=yes_best,
        yes_best_ask_size=yes_size,
        no_best_ask=no_best,
        no_best_ask_size=no_size,
        raw=data,
    )


def edge_for_top(
    top: OrderBookTop,
    fee_model: FeeModel,
    overhead: float,
) -> float | None:
    if top.yes_best_ask is None or top.no_best_ask is None:
        return None
    fees = fee_model.total_fee(top.yes_best_ask, top.no_best_ask)
    return 1.0 - (top.yes_best_ask + top.no_best_ask) - fees - overhead


def is_executable(top: OrderBookTop, target_qty: float) -> bool:
    if top.yes_best_ask_size is None or top.no_best_ask_size is None:
        return False
    return top.yes_best_ask_size >= target_qty and top.no_best_ask_size >= target_qty
