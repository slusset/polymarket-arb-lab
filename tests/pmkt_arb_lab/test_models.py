import json
from pathlib import Path

from pmkt_arb_lab.models import MarketMetadata


def test_parse_clob_token_ids_flat_list() -> None:
    data = {
        "id": "mkt-1",
        "question": "Test",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["token-1-yes", "token-1-no"],
    }
    market = MarketMetadata.from_api(data)
    assert market.clob_token_ids == ["token-1-yes", "token-1-no"]
    assert market.yes_clob_token_id == "token-1-yes"
    assert market.no_clob_token_id == "token-1-no"


def test_parse_clob_token_ids_nested_json_string() -> None:
    data = {
        "id": "mkt-2",
        "question": "Test",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": [
            "[\"105104581338576429268357347529823581162598821797457643758239969314113345373365\","
            "\"6671922635678023460317612186903727524883421159043360342126653511917319848209\"]"
        ],
    }
    market = MarketMetadata.from_api(data)
    assert market.clob_token_ids == [
        "105104581338576429268357347529823581162598821797457643758239969314113345373365",
        "6671922635678023460317612186903727524883421159043360342126653511917319848209",
    ]
    assert market.yes_clob_token_id == market.clob_token_ids[0]
    assert market.no_clob_token_id == market.clob_token_ids[1]


def test_parse_outcomes_and_tokens_from_gamma_fixture() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "data" / "polymarket_events_10.json"
    with path.open(encoding="utf-8") as handle:
        raw_events = json.load(handle)

    raw_market = None
    for raw_event in raw_events:
        for market in raw_event.get("markets", []):
            if market.get("outcomes") and market.get("clobTokenIds"):
                raw_market = market
                break
        if raw_market is not None:
            break

    assert raw_market is not None
    parsed = MarketMetadata.from_api(raw_market)
    assert len(parsed.outcomes) == 2
    assert len(parsed.clob_token_ids) == 2
