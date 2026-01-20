import csv
from decimal import Decimal
from pathlib import Path

from pmkt.clob.models import OrderBook, OrderLevel
from pmkt.clob.paired_recorder import TradablePair, record_paired_quotes


class _FakeClient:
    def __init__(self, book: OrderBook) -> None:
        self._book = book

    def get_order_book(self, token_id: str) -> OrderBook:
        return OrderBook(
            token_id=token_id,
            market=self._book.market,
            timestamp_ms=self._book.timestamp_ms,
            bids=list(self._book.bids),
            asks=list(self._book.asks),
            tick_size=self._book.tick_size,
            min_order_size=self._book.min_order_size,
            hash=self._book.hash,
        )

    def close(self) -> None:
        pass


def test_record_paired_quotes_writes_csv(tmp_path: Path) -> None:
    book = OrderBook(
        token_id="token-up",
        market="cond-1",
        timestamp_ms=1000,
        bids=[OrderLevel(price=Decimal("0.49"), size=Decimal("687"))],
        asks=[OrderLevel(price=Decimal("0.51"), size=Decimal("687"))],
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        hash=None,
    )
    pairs = [
        TradablePair(
            condition_id="cond-1",
            token_a_id="token-up",
            token_b_id="token-down",
            outcome_a="Up",
            outcome_b="Down",
        )
    ]
    out_dir = tmp_path / "quotes"
    record_paired_quotes(
        pairs,
        out_dir=out_dir,
        interval_seconds=0,
        max_iters=1,
        client=_FakeClient(book),
    )

    quotes_path = out_dir / "paired_quotes.csv"
    assert quotes_path.exists()
    with quotes_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["condition_id"] == "cond-1"
