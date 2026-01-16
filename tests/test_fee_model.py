from polymarket_arb_lab.logic import FeeModel


def test_fee_model_total_fee() -> None:
    model = FeeModel(bps_per_leg=10.0, pct_per_leg=0.5)
    fee = model.total_fee(0.4, 0.6)
    expected = (0.4 + 0.6) * (0.001 + 0.005)
    assert abs(fee - expected) < 1e-9
