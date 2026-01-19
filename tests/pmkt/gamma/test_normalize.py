import json
from pathlib import Path

from pmkt.gamma.normalize import parse_events, parse_tokens


def _load_raw_events() -> list[dict[str, object]]:
    repo_root = Path(__file__).resolve().parents[3]
    path = repo_root / "data" / "polymarket_events_10.json"
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    assert isinstance(data, list)
    return data


def test_parse_events_from_fixture() -> None:
    raw_events = _load_raw_events()
    assert raw_events
    events = parse_events(raw_events)
    assert len(events) == len(raw_events)


def test_parse_tokens_from_fixture() -> None:
    raw_events = _load_raw_events()
    events = parse_events(raw_events)
    markets = [market for event in events for market in event.markets]
    tokens = parse_tokens(markets)
    assert tokens
    assert all(token.token_id for token in tokens)


def test_market_fields_map_cleanly() -> None:
    raw_events = _load_raw_events()
    events = parse_events(raw_events)
    markets = [market for event in events for market in event.markets]

    raw_market = None
    for raw_event in raw_events:
        for raw_entry in raw_event.get("markets", []):
            if "enableOrderBook" in raw_entry or "acceptingOrders" in raw_entry:
                raw_market = raw_entry
                break
        if raw_market is not None:
            break

    assert raw_market is not None
    market_id = str(raw_market.get("id") or raw_market.get("market_id") or "")
    parsed = next(market for market in markets if market.market_id == market_id)

    if "enableOrderBook" in raw_market:
        assert parsed.enable_order_book == raw_market["enableOrderBook"]
    if "acceptingOrders" in raw_market:
        assert parsed.accepting_orders == raw_market["acceptingOrders"]


def test_market_outcomes_and_tokens_lengths() -> None:
    raw_events = _load_raw_events()
    events = parse_events(raw_events)
    markets = [market for event in events for market in event.markets]

    assert any(len(market.outcomes) == 2 for market in markets)
    assert any(len(market.clob_token_ids) == 2 for market in markets)
