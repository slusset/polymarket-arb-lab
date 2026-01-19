from pmkt_arb_lab.logic import FeeModel, edge_for_top, is_executable
from pmkt_arb_lab.models import OrderBookTop


def test_edge_for_top() -> None:
    top = OrderBookTop(
        yes_best_ask=0.4,
        yes_best_ask_size=10.0,
        no_best_ask=0.5,
        no_best_ask_size=10.0,
        raw={},
    )
    edge = edge_for_top(top, FeeModel(), overhead=0.0)
    assert edge is not None
    assert abs(edge - 0.1) < 1e-9


def test_edge_requires_prices() -> None:
    top = OrderBookTop(
        yes_best_ask=None,
        yes_best_ask_size=10.0,
        no_best_ask=0.5,
        no_best_ask_size=10.0,
        raw={},
    )
    edge = edge_for_top(top, FeeModel(), overhead=0.0)
    assert edge is None


def test_is_executable() -> None:
    top = OrderBookTop(
        yes_best_ask=0.4,
        yes_best_ask_size=1.0,
        no_best_ask=0.5,
        no_best_ask_size=2.0,
        raw={},
    )
    assert is_executable(top, 1.0)
    assert not is_executable(top, 5.0)
