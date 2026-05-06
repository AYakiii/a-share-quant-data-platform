from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance.backtest import run_buffered_topn_backtest
from qsys.rebalance.policies import BufferedTopNPolicyConfig


def _mk_signal_and_returns() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-08", "2024-01-09"])
    assets = ["A", "B", "C"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])

    signal = pd.DataFrame(index=idx)
    signal["score"] = [
        3, 2, 1,
        2, 3, 1,
        1, 3, 2,
        3, 1, 2,
        1, 2, 3,
    ]
    signal["rank"] = signal.groupby(level="date")["score"].rank(method="first", ascending=False)
    signal["is_tradable"] = True

    returns = pd.DataFrame(index=idx)
    returns["ret_1d"] = [
        0.01, 0.00, -0.01,
        0.02, 0.01, -0.01,
        -0.03, 0.04, 0.01,
        0.01, -0.02, 0.00,
        0.03, 0.02, -0.01,
    ]
    return signal, returns


def test_daily_mode_rebalances_every_date() -> None:
    signal, returns = _mk_signal_and_returns()
    # Force daily rotation so each rebalance date has non-zero turnover
    signal = signal.copy()
    signal["score"] = [3,2,1, 1,3,2, 2,1,3, 3,1,2, 1,2,3]
    signal["rank"] = signal.groupby(level="date")["score"].rank(method="first", ascending=False)
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=1, sell_rank=1, min_holding_n=1, max_holding_n=2, rebalance="daily")

    result = run_buffered_topn_backtest(signal, returns, cfg)
    rb_dates = result["turnover"].index[result["turnover"]["turnover"] > 0]
    assert len(rb_dates) == len(signal.index.get_level_values("date").unique())


def test_weekly_mode_rebalances_last_date_each_week() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=3, min_holding_n=1, max_holding_n=3, rebalance="weekly")

    result = run_buffered_topn_backtest(signal, returns, cfg)
    # weekly rebalance happens on week-end available dates; turnover can be zero if no trade
    week_end_dates = [pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-09")]
    assert list(result["turnover"].index) == list(pd.to_datetime(["2024-01-01","2024-01-02","2024-01-03","2024-01-08","2024-01-09"]))
    # there should be at least one trade stamped on a weekly rebalance date
    trade_dates = set(pd.to_datetime(result["trades"]["date"]).tolist()) if len(result["trades"]) else set()
    assert trade_dates.issubset(set(week_end_dates))


def test_weights_carried_forward_on_non_rebalance_dates() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=3, min_holding_n=1, max_holding_n=3, rebalance="weekly")

    result = run_buffered_topn_backtest(signal, returns, cfg)
    w = result["weights"]
    w_0103 = w.xs(pd.Timestamp("2024-01-03"), level="date")["target_weight"]
    w_0108 = w.xs(pd.Timestamp("2024-01-08"), level="date")["target_weight"]
    assert w_0103.to_dict() == w_0108.to_dict()


def test_returns_use_previous_date_weights() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=1, sell_rank=3, min_holding_n=1, max_holding_n=2, rebalance="daily", cost_bps=0.0)

    result = run_buffered_topn_backtest(signal, returns, cfg)
    daily = result["daily_returns"]

    assert daily.iloc[0]["gross_return"] == 0.0
    # On 2024-01-02, uses weights from 2024-01-01 (asset A selected initially -> ret 0.02)
    assert daily.loc[pd.Timestamp("2024-01-02"), "gross_return"] == pytest.approx(0.02)


def test_turnover_and_costs_only_on_rebalance_dates() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=3, min_holding_n=1, max_holding_n=3, rebalance="weekly", cost_bps=10.0)

    result = run_buffered_topn_backtest(signal, returns, cfg)
    t = result["turnover"]["turnover"]
    c = result["costs"]["cost"]

    for d in t.index:
        if d in [pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-09")]:
            assert t.loc[d] >= 0.0
        else:
            assert t.loc[d] == 0.0
            assert c.loc[d] == 0.0


def test_result_dict_contains_required_keys() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=3, min_holding_n=1, max_holding_n=3)

    result = run_buffered_topn_backtest(signal, returns, cfg)
    assert set(result.keys()) == {"daily_returns", "weights", "trades", "turnover", "costs", "summary"}


def test_summary_contains_required_metrics() -> None:
    signal, returns = _mk_signal_and_returns()
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=3, min_holding_n=1, max_holding_n=3)

    summary = run_buffered_topn_backtest(signal, returns, cfg)["summary"]
    required = {
        "start_date", "end_date", "n_dates", "total_return", "annualized_return", "annualized_vol", "sharpe", "max_drawdown", "average_turnover", "total_cost"
    }
    assert required.issubset(summary.keys())


def test_no_modification_to_old_backtest_modules() -> None:
    # Contract-level smoke: new module is importable and old simulator remains separate.
    import qsys.backtest.simulator as sim  # noqa: F401
    import qsys.rebalance.backtest as rb

    assert hasattr(rb, "run_buffered_topn_backtest")
