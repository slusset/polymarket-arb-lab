from datetime import datetime, timezone

from polymarket_arb_lab.logic import select_markets
from polymarket_arb_lab.models import MarketMetadata


def test_select_markets_closed_and_ended() -> None:
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)
    open_market = MarketMetadata(
        market_id="m1",
        title="Open",
        status="open",
        outcomes=["Yes", "No"],
        clob_token_ids=["y1", "n1"],
        yes_clob_token_id="y1",
        no_clob_token_id="n1",
        volume=10.0,
        liquidity=5.0,
        enable_order_book=True,
    )
    closed_market = MarketMetadata(
        market_id="m2",
        title="Closed",
        status="open",
        outcomes=["Yes", "No"],
        clob_token_ids=["y2", "n2"],
        yes_clob_token_id="y2",
        no_clob_token_id="n2",
        volume=10.0,
        liquidity=5.0,
        closed=True,
        enable_order_book=True,
    )
    ended_market = MarketMetadata(
        market_id="m3",
        title="Ended",
        status="open",
        outcomes=["Yes", "No"],
        clob_token_ids=["y3", "n3"],
        yes_clob_token_id="y3",
        no_clob_token_id="n3",
        volume=10.0,
        liquidity=5.0,
        end_date="2020-01-01T00:00:00Z",
        enable_order_book=True,
    )

    candidates, diag = select_markets(
        [open_market, closed_market, ended_market],
        min_volume=0.0,
        max_markets=None,
        min_liquidity=0.0,
        now=now,
    )

    assert [m.market_id for m in candidates] == ["m1"]
    assert diag.skipped_closed == 1
    assert diag.skipped_ended == 1


def test_select_markets_skip_reasons() -> None:
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)
    non_binary = MarketMetadata(
        market_id="m4",
        title="Non-binary",
        status="open",
        outcomes=["A", "B"],
        clob_token_ids=["y4", "n4"],
        yes_clob_token_id="y4",
        no_clob_token_id="n4",
        volume=10.0,
        liquidity=5.0,
        enable_order_book=True,
    )
    missing_tokens = MarketMetadata(
        market_id="m5",
        title="Missing tokens",
        status="open",
        outcomes=["Yes", "No"],
        volume=10.0,
        liquidity=5.0,
        enable_order_book=True,
    )
    low_volume = MarketMetadata(
        market_id="m6",
        title="Low volume",
        status="open",
        outcomes=["Yes", "No"],
        clob_token_ids=["y6", "n6"],
        yes_clob_token_id="y6",
        no_clob_token_id="n6",
        volume=1.0,
        liquidity=50.0,
        enable_order_book=True,
    )
    low_liquidity = MarketMetadata(
        market_id="m7",
        title="Low liquidity",
        status="open",
        outcomes=["Yes", "No"],
        clob_token_ids=["y7", "n7"],
        yes_clob_token_id="y7",
        no_clob_token_id="n7",
        volume=100.0,
        liquidity=1.0,
        enable_order_book=True,
    )

    _, diag = select_markets(
        [non_binary, missing_tokens, low_volume, low_liquidity],
        min_volume=10.0,
        max_markets=None,
        min_liquidity=10.0,
        now=now,
    )

    assert diag.skipped_not_binary == 1
    assert diag.skipped_missing_tokens == 1
    assert diag.skipped_low_volume == 1
    assert diag.skipped_low_liquidity == 3
    assert diag.skipped_bad_token_count == 1
