from polymarket_arb_lab.logic import vwap_for_asks


def test_vwap_for_asks_dict_levels() -> None:
    levels = [
        {"price": 0.4, "size": 2},
        {"price": 0.5, "size": 3},
    ]
    vwap = vwap_for_asks(levels, target_qty=4)
    assert vwap is not None
    assert abs(vwap - 0.45) < 1e-9


def test_vwap_for_asks_insufficient() -> None:
    levels = [{"price": 0.4, "size": 1}]
    assert vwap_for_asks(levels, target_qty=2) is None


def test_vwap_for_asks_list_levels() -> None:
    levels = [
        [0.3, 1],
        [0.35, 2],
    ]
    vwap = vwap_for_asks(levels, target_qty=2)
    assert vwap is not None
    assert abs(vwap - 0.325) < 1e-9
