from decimal import Decimal

from pmkt.clob.models import OrderBook, OrderLevel
from pmkt.clob.paired import make_paired_snapshot


def _book(token_id: str, bids: list[OrderLevel], asks: list[OrderLevel]) -> OrderBook:
    return OrderBook(
        token_id=token_id,
        market="cond-1",
        timestamp_ms=123,
        bids=bids,
        asks=asks,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        hash=None,
    )


def test_make_paired_snapshot_metrics() -> None:
    up_bids = [
        OrderLevel(price=Decimal("0.49"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.01"), size=Decimal("50")),
    ]
    up_asks = [
        OrderLevel(price=Decimal("0.51"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.99"), size=Decimal("50")),
    ]
    down_bids = [
        OrderLevel(price=Decimal("0.49"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.01"), size=Decimal("50")),
    ]
    down_asks = [
        OrderLevel(price=Decimal("0.51"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.99"), size=Decimal("50")),
    ]
    up = _book("token-up", up_bids, up_asks)
    down = _book("token-down", down_bids, down_asks)

    snapshot = make_paired_snapshot(up, down, outcome_a="Up", outcome_b="Down")

    assert snapshot.mid_sum == Decimal("1.00")
    assert snapshot.spread_sum == Decimal("0.04")
    assert snapshot.buy_both_cost == Decimal("1.02")
    assert snapshot.sell_both_proceeds == Decimal("0.98")
