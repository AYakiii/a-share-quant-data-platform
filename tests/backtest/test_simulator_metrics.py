from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.backtest.simulator import BacktestConfig, run_backtest_from_signal


def test_simulator_and_metrics_contract() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    idx = pd.MultiIndex.from_product([dates, ["A", "B"]], names=["date", "asset"])

    signal = pd.Series([0.2, 0.1, 0.1, 0.2, 0.3, 0.1, 0.2, 0.3], index=idx)
    ret_1d = pd.Series([0.01, -0.01, 0.02, -0.02, 0.01, 0.00, -0.01, 0.03], index=idx)

    result = run_backtest_from_signal(
        signal,
        ret_1d,
        config=BacktestConfig(top_n=1, long_only=True, rebalance="daily", transaction_cost_bps=0.0, slippage_bps=0.0),
    )

    returns = result["returns"]
    summary = result["summary"]

    assert len(returns) > 0
    assert "cumulative_return" in summary
    assert "sharpe" in summary
    assert "turnover" in summary
