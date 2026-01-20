from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .models import OrderBook, sum_sizes


@dataclass(slots=True)
class PairedBookSnapshot:
    ts_ms: int
    condition_id: str
    token_a_id: str
    token_b_id: str
    outcome_a: str
    outcome_b: str
    a_bid: Decimal
    a_ask: Decimal
    a_mid: Decimal
    a_spread: Decimal
    a_bid_sz: Decimal
    a_ask_sz: Decimal
    b_bid: Decimal
    b_ask: Decimal
    b_mid: Decimal
    b_spread: Decimal
    b_bid_sz: Decimal
    b_ask_sz: Decimal
    mid_sum: Decimal
    spread_sum: Decimal
    buy_both_cost: Decimal
    sell_both_proceeds: Decimal
    depth_bid_5_up: Decimal
    depth_ask_5_up: Decimal
    depth_bid_5_down: Decimal
    depth_ask_5_down: Decimal


def make_paired_snapshot(
    book_a: OrderBook,
    book_b: OrderBook,
    *,
    outcome_a: str,
    outcome_b: str,
    depth_levels: int = 5,
) -> PairedBookSnapshot:
    a_best_bid = book_a.best_bid()
    a_best_ask = book_a.best_ask()
    b_best_bid = book_b.best_bid()
    b_best_ask = book_b.best_ask()
    if a_best_bid is None or a_best_ask is None:
        raise ValueError("Missing book A best bid/ask")
    if b_best_bid is None or b_best_ask is None:
        raise ValueError("Missing book B best bid/ask")

    a_bid, a_bid_sz = a_best_bid
    a_ask, a_ask_sz = a_best_ask
    b_bid, b_bid_sz = b_best_bid
    b_ask, b_ask_sz = b_best_ask

    a_mid = (a_bid + a_ask) / Decimal("2")
    b_mid = (b_bid + b_ask) / Decimal("2")
    a_spread = a_ask - a_bid
    b_spread = b_ask - b_bid

    a_top = book_a.top_n(depth_levels)
    b_top = book_b.top_n(depth_levels)
    depth_bid_5_up = sum_sizes(a_top.bids)
    depth_ask_5_up = sum_sizes(a_top.asks)
    depth_bid_5_down = sum_sizes(b_top.bids)
    depth_ask_5_down = sum_sizes(b_top.asks)

    mid_sum = a_mid + b_mid
    spread_sum = a_spread + b_spread
    buy_both_cost = a_ask + b_ask
    sell_both_proceeds = a_bid + b_bid

    return PairedBookSnapshot(
        ts_ms=max(book_a.timestamp_ms, book_b.timestamp_ms),
        condition_id=book_a.market or book_b.market,
        token_a_id=book_a.token_id,
        token_b_id=book_b.token_id,
        outcome_a=outcome_a,
        outcome_b=outcome_b,
        a_bid=a_bid,
        a_ask=a_ask,
        a_mid=a_mid,
        a_spread=a_spread,
        a_bid_sz=a_bid_sz,
        a_ask_sz=a_ask_sz,
        b_bid=b_bid,
        b_ask=b_ask,
        b_mid=b_mid,
        b_spread=b_spread,
        b_bid_sz=b_bid_sz,
        b_ask_sz=b_ask_sz,
        mid_sum=mid_sum,
        spread_sum=spread_sum,
        buy_both_cost=buy_both_cost,
        sell_both_proceeds=sell_both_proceeds,
        depth_bid_5_up=depth_bid_5_up,
        depth_ask_5_up=depth_ask_5_up,
        depth_bid_5_down=depth_bid_5_down,
        depth_ask_5_down=depth_ask_5_down,
    )
