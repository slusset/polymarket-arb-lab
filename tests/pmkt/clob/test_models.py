from decimal import Decimal

from pmkt.clob.models import OrderBook, OrderLevel


def test_best_bid_ask_selects_extrema() -> None:
    bids = [
        OrderLevel(price=Decimal("0.01"), size=Decimal("1000")),
        OrderLevel(price=Decimal("0.49"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.30"), size=Decimal("50")),
    ]
    asks = [
        OrderLevel(price=Decimal("0.99"), size=Decimal("1000")),
        OrderLevel(price=Decimal("0.51"), size=Decimal("687")),
        OrderLevel(price=Decimal("0.70"), size=Decimal("10")),
    ]
    book = OrderBook(
        token_id="token-up",
        market="cond-1",
        timestamp_ms=1,
        bids=bids,
        asks=asks,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        hash=None,
    )

    best_bid = book.best_bid()
    best_ask = book.best_ask()
    assert best_bid is not None
    assert best_ask is not None
    assert best_bid[0] == Decimal("0.49")
    assert best_ask[0] == Decimal("0.51")
