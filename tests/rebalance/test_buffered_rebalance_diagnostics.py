from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance.diagnostics import (
    analyze_trade_forward_returns,
    holding_period_summary,
    rank_migration_matrix,
    summarize_trades,
)


def test_summarize_trades_counts_and_turnover() -> None:
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-02"]),
            "asset": ["A", "B", "C", "A"],
            "prev_weight": [0.0, 0.5, 0.5, 0.6],
            "target_weight": [0.5, 0.0, 0.5, 0.4],
            "trade_weight": [0.5, -0.5, 0.0, -0.2],
            "action": ["buy", "sell", "keep", "trim"],
            "reason": ["", "", "", ""],
            "rank": [2, 8, 4, 5],
            "score": [0.8, 0.2, 0.5, 0.4],
            "is_tradable": [True, True, True, True],
        }
    )

    out = summarize_trades(trades)
    assert out.loc[pd.Timestamp("2024-01-01"), "n_buy"] == 1
    assert out.loc[pd.Timestamp("2024-01-01"), "n_sell"] == 1
    assert out.loc[pd.Timestamp("2024-01-01"), "n_keep"] == 1
    assert out.loc[pd.Timestamp("2024-01-01"), "gross_turnover"] == pytest.approx(1.0)


def test_summarize_trades_average_buy_sell_rank() -> None:
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "asset": ["A", "B", "C"],
            "prev_weight": [0.0, 0.0, 0.4],
            "target_weight": [0.3, 0.3, 0.0],
            "trade_weight": [0.3, 0.3, -0.4],
            "action": ["buy", "buy", "sell"],
            "reason": ["", "", ""],
            "rank": [2, 4, 9],
            "score": [0.8, 0.7, 0.2],
            "is_tradable": [True, True, True],
        }
    )
    out = summarize_trades(trades)
    assert out.loc[pd.Timestamp("2024-01-01"), "average_buy_rank"] == pytest.approx(3.0)
    assert out.loc[pd.Timestamp("2024-01-01"), "average_sell_rank"] == pytest.approx(9.0)


def test_holding_period_summary_segments() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-01-02"), "A"),
            (pd.Timestamp("2026-01-05"), "A"),
            (pd.Timestamp("2026-01-06"), "A"),
            (pd.Timestamp("2026-01-02"), "B"),
            (pd.Timestamp("2026-01-05"), "B"),
        ],
        names=["date", "asset"],
    )
    weights = pd.DataFrame({"target_weight": [0.5, 0.5, 0.6, 0.2, 0.2]}, index=idx)

    summary = holding_period_summary(weights)
    by_asset = summary["by_asset"]

    assert summary["n_completed_positions"] == 2
    assert int(by_asset[by_asset["asset"] == "A"]["holding_days"].max()) == 3
    assert summary["max_holding_days"] == 3


def test_analyze_trade_forward_returns_strictly_after_trade_date() -> None:
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "asset": ["A", "A"],
            "action": ["buy", "buy"],
        }
    )
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-01"), "A"),
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-03"), "A"),
        ],
        names=["date", "asset"],
    )
    returns = pd.DataFrame({"ret_1d": [0.50, 0.10, 0.10]}, index=idx)

    out = analyze_trade_forward_returns(trades, returns, horizons=(2,), return_col="ret_1d")
    # First event ignores same-day 0.50 and uses 0.10, 0.10 => 0.21
    row = out[(out["action"] == "buy") & (out["horizon"] == 2)].iloc[0]
    assert row["n"] == 1
    assert row["mean_forward_return"] == pytest.approx(0.21)
    assert row["median_forward_return"] == pytest.approx(0.21)


def test_rank_migration_matrix_counts_transitions() -> None:
    widx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-01"), "A"),
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-03"), "A"),
        ],
        names=["date", "asset"],
    )
    weights = pd.DataFrame({"target_weight": [0.3, 0.3, 0.3]}, index=widx)

    sidx = widx
    signal = pd.DataFrame({"rank": [40, 70, 180]}, index=sidx)

    m = rank_migration_matrix(weights, signal)
    assert m.loc["1-50", "51-100"] == 1
    assert m.loc["51-100", "101-200"] == 1


def test_empty_inputs_graceful() -> None:
    empty_trades = pd.DataFrame(columns=["date", "asset", "prev_weight", "target_weight", "trade_weight", "action", "reason", "rank", "score", "is_tradable"])
    s = summarize_trades(empty_trades)
    assert s.empty

    empty_weights = pd.DataFrame(columns=["target_weight"], index=pd.MultiIndex.from_arrays([[], []], names=["date", "asset"]))
    hp = holding_period_summary(empty_weights)
    assert hp["n_completed_positions"] == 0

    idx = pd.MultiIndex.from_arrays([[], []], names=["date", "asset"])
    returns = pd.DataFrame({"ret_1d": []}, index=idx)
    afr = analyze_trade_forward_returns(empty_trades[["date", "asset", "action"]], returns)
    assert afr.empty

    rm = rank_migration_matrix(empty_weights, pd.DataFrame({"rank": []}, index=idx))
    assert rm.values.sum() == 0
