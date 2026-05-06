from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance.benchmarks import build_equal_weight_benchmark


def _mk_returns() -> pd.DataFrame:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-08", "2024-01-09"])
    assets = ["A", "B"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])
    vals = [0.01, -0.01, 0.02, 0.00, None, 0.01, 0.00, 0.02, 0.03, -0.01]
    return pd.DataFrame({"ret_1d": vals}, index=idx)


def test_validate_multiindex_names() -> None:
    df = pd.DataFrame({"ret_1d": [0.01]}, index=pd.Index([0]))
    with pytest.raises(ValueError):
        build_equal_weight_benchmark(df)


def test_daily_equal_weight_uses_all_assets() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="daily")
    w = result["weights"].xs(pd.Timestamp("2024-01-02"), level="date")["target_weight"]
    assert set(w.index) == {"A", "B"}
    assert float(w.sum()) == pytest.approx(1.0)


def test_weekly_rebalance_last_available_date_of_week() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="weekly")
    t = result["turnover"]["turnover"]
    # weeks end on 2024-01-03 and 2024-01-09 in available dates
    assert t.loc[pd.Timestamp("2024-01-01")] == 0.0
    assert t.loc[pd.Timestamp("2024-01-03")] > 0
    assert t.loc[pd.Timestamp("2024-01-09")] >= 0


def test_no_lookahead_first_date_zero() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="daily")
    assert result["daily_returns"].iloc[0]["gross_return"] == 0.0


def test_turnover_cost_only_rebalance_dates() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="weekly", cost_bps=10.0)
    t = result["turnover"]["turnover"]
    c = result["costs"]["cost"]
    assert t.loc[pd.Timestamp("2024-01-02")] == 0.0
    assert c.loc[pd.Timestamp("2024-01-02")] == 0.0


def test_cumulative_return_computed() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="daily")
    assert "cumulative_net_return" in result["daily_returns"].columns


def test_summary_keys_exist() -> None:
    df = _mk_returns()
    summary = build_equal_weight_benchmark(df, rebalance="daily")["summary"]
    required = {
        "start_date", "end_date", "n_dates", "total_return", "annualized_return", "annualized_vol", "sharpe", "max_drawdown", "average_turnover", "total_cost"
    }
    assert required.issubset(summary.keys())


def test_missing_asset_returns_treated_as_zero() -> None:
    df = _mk_returns()
    result = build_equal_weight_benchmark(df, rebalance="daily")
    # On 2024-01-03, asset A return is missing -> treated as 0; B is 0.01, prev weights are 0.5/0.5
    val = result["daily_returns"].loc[pd.Timestamp("2024-01-03"), "gross_return"]
    assert val == pytest.approx(0.005)


def test_invalid_rebalance_raises() -> None:
    df = _mk_returns()
    with pytest.raises(ValueError):
        build_equal_weight_benchmark(df, rebalance="quarterly")
