from __future__ import annotations

from typing import Any

from pytest_bdd import given, parsers, scenario, then, when

from pmkt.gamma.normalize import parse_markets


@scenario("market_lifecycle.feature", "classify market lifecycle from flags")
def test_market_lifecycle_classification() -> None:
    pass


@given(
    parsers.parse(
        'a market with active "{active}", closed "{closed}", enable_order_book "{enable_order_book}", accepting_orders "{accepting_orders}", and event_start_time "{event_start_time}"'
    ),
    target_fixture="market_payload",
)
def market_payload(
    active: str, closed: str, enable_order_book: str, accepting_orders: str, event_start_time: str
) -> dict[str, Any]:
    def _to_bool(value: str) -> bool:
        return value.strip().lower() == "true"

    return {
        "id": "mkt-1",
        "question": "Test market",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["token-yes", "token-no"],
        "active": _to_bool(active),
        "closed": _to_bool(closed),
        "enableOrderBook": _to_bool(enable_order_book),
        "acceptingOrders": _to_bool(accepting_orders),
        "eventStartTime": event_start_time,
    }


@when("I normalize the market", target_fixture="normalized_market")
def normalized_market(market_payload: dict[str, Any]) -> Any:
    return parse_markets([market_payload])[0]


@then(parsers.parse('the lifecycle_state is "{lifecycle_state}"'))
def assert_lifecycle_state(normalized_market: Any, lifecycle_state: str) -> None:
    assert normalized_market.lifecycle_state == lifecycle_state
