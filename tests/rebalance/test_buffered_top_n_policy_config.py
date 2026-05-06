from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance import BufferedTopNPolicyConfig, calc_transaction_cost, calc_turnover


def test_default_config_creation() -> None:
    cfg = BufferedTopNPolicyConfig()

    assert cfg.target_n == 50
    assert cfg.buy_rank == 50
    assert cfg.sell_rank == 100
    assert cfg.rebalance == "weekly"


def test_invalid_sell_rank_less_than_buy_rank() -> None:
    with pytest.raises(ValueError, match="sell_rank"):
        BufferedTopNPolicyConfig(buy_rank=20, sell_rank=10)


def test_invalid_target_n_outside_bounds() -> None:
    with pytest.raises(ValueError, match="target_n"):
        BufferedTopNPolicyConfig(target_n=70, min_holding_n=45, max_holding_n=60)


def test_invalid_rebalance_value() -> None:
    with pytest.raises(ValueError, match="rebalance"):
        BufferedTopNPolicyConfig(rebalance="quarterly")


def test_calc_turnover_aligns_missing_assets() -> None:
    prev = pd.Series({"A": 0.5, "B": 0.5})
    target = pd.Series({"A": 0.2, "C": 0.8})

    # |0.2-0.5| + |0.0-0.5| + |0.8-0.0| = 0.3 + 0.5 + 0.8 = 1.6
    turnover = calc_turnover(prev, target)

    assert turnover == pytest.approx(1.6)


def test_calc_transaction_cost_calculation() -> None:
    cost = calc_transaction_cost(turnover=1.6, cost_bps=15.0)
    assert cost == pytest.approx(0.0024)
