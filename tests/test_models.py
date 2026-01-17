from polymarket_arb_lab.models import MarketMetadata


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
