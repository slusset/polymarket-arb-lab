from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .models import MarketClassification, MarketMetadata, OrderBookTop
from .utils import parse_iso_datetime


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


@dataclass
class SelectionDiagnostics:
    total: int = 0
    selected: int = 0
    skipped_closed: int = 0
    skipped_ended: int = 0
    skipped_not_binary: int = 0
    skipped_missing_tokens: int = 0
    skipped_bad_token_count: int = 0
    skipped_orderbook_disabled: int = 0
    skipped_low_liquidity: int = 0
    skipped_low_volume: int = 0

    def top_reasons(self) -> list[tuple[str, int]]:
        counts = {
            "skipped_closed": self.skipped_closed,
            "skipped_ended": self.skipped_ended,
            "skipped_not_binary": self.skipped_not_binary,
            "skipped_missing_tokens": self.skipped_missing_tokens,
            "skipped_bad_token_count": self.skipped_bad_token_count,
            "skipped_orderbook_disabled": self.skipped_orderbook_disabled,
            "skipped_low_liquidity": self.skipped_low_liquidity,
            "skipped_low_volume": self.skipped_low_volume,
        }
        return sorted(
            ((name, count) for name, count in counts.items() if count > 0),
            key=lambda item: item[1],
            reverse=True,
        )


def _market_has_ended(market: MarketMetadata, now: datetime) -> bool:
    end_dt = parse_iso_datetime(market.end_date)
    if end_dt is None:
        return False
    return end_dt <= now


def select_markets(
    markets: Iterable[MarketMetadata],
    min_volume: float,
    max_markets: int | None,
    min_liquidity: float = 0.0,
    now: datetime | None = None,
) -> tuple[list[MarketMetadata], SelectionDiagnostics]:
    eligible: list[MarketMetadata] = []
    diagnostics = SelectionDiagnostics()
    now = now or datetime.now(timezone.utc)
    for market in markets:
        diagnostics.total += 1
        classification = classify_market(market)
        skip = False
        if market.closed is True or market.active is False or not classification.is_open:
            diagnostics.skipped_closed += 1
            skip = True
        if _market_has_ended(market, now):
            diagnostics.skipped_ended += 1
            skip = True
        if classification.semantics != "binary_yes_no":
            diagnostics.skipped_not_binary += 1
            skip = True
        if market.enable_order_book is not True:
            diagnostics.skipped_orderbook_disabled += 1
            skip = True
        if len(market.clob_token_ids) != 2:
            diagnostics.skipped_bad_token_count += 1
            skip = True
        if not (market.yes_clob_token_id and market.no_clob_token_id):
            diagnostics.skipped_missing_tokens += 1
            skip = True
        if min_volume > 0.0 and market.volume < min_volume:
            diagnostics.skipped_low_volume += 1
            skip = True
        if min_liquidity > 0.0 and market.liquidity < min_liquidity:
            diagnostics.skipped_low_liquidity += 1
            skip = True
        if skip:
            continue
        if not market.market_id:
            continue
        eligible.append(market)
    diagnostics.selected = len(eligible)
    if max_markets is not None:
        return eligible[: max_markets], diagnostics
    return eligible, diagnostics


def _level_price_size(level: Any) -> tuple[float | None, float | None]:
    if isinstance(level, dict):
        price = level.get("price") or level.get("rate")
        size = level.get("size") or level.get("quantity") or level.get("amount")
        try:
            return float(price), float(size)
        except (TypeError, ValueError):
            return None, None
    if isinstance(level, list) and len(level) >= 2:
        try:
            return float(level[0]), float(level[1])
        except (TypeError, ValueError):
            return None, None
    return None, None


def best_ask_from_levels(levels: Any) -> tuple[float | None, float | None]:
    if not isinstance(levels, list) or not levels:
        return None, None
    return _level_price_size(levels[0])


def vwap_for_asks(levels: Any, target_qty: float) -> float | None:
    if target_qty <= 0:
        return None
    if not isinstance(levels, list) or not levels:
        return None
    remaining = target_qty
    total_cost = 0.0
    filled = 0.0
    for level in levels:
        price, size = _level_price_size(level)
        if price is None or size is None or size <= 0:
            continue
        take = min(remaining, size)
        total_cost += take * price
        filled += take
        remaining -= take
        if remaining <= 0:
            break
    if filled < target_qty:
        return None
    return total_cost / filled


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


def parse_clob_orderbook_top(
    yes_book: dict[str, Any] | None,
    no_book: dict[str, Any] | None,
) -> OrderBookTop:
    yes_book = yes_book or {}
    no_book = no_book or {}
    yes_best, yes_size = best_ask_from_levels(yes_book.get("asks"))
    no_best, no_size = best_ask_from_levels(no_book.get("asks"))
    return OrderBookTop(
        yes_best_ask=yes_best,
        yes_best_ask_size=yes_size,
        no_best_ask=no_best,
        no_best_ask_size=no_size,
        raw={"yes": yes_book, "no": no_book},
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
