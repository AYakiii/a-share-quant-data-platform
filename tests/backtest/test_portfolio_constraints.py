from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.backtest.portfolio import build_top_n_portfolio


def test_portfolio_constraints_contract() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-02"), "C"),
            (pd.Timestamp("2024-01-02"), "D"),
        ],
        names=["date", "asset"],
    )

    signal = pd.Series([0.4, 0.3, 0.2, 0.1], index=idx)
    liquidity = pd.Series([100.0, 50.0, 10.0, 100.0], index=idx)
    market_cap = pd.Series([200.0, 100.0, 50.0, 300.0], index=idx)
    groups = pd.Series(["G1", "G1", "G2", "G2"], index=idx)

    w = build_top_n_portfolio(
        signal,
        top_n=3,
        long_only=True,
        max_single_weight=0.6,
        liquidity=liquidity,
        min_liquidity=20.0,
        market_cap=market_cap,
        size_aware_scaling=True,
        group_labels=groups,
        group_cap=0.7,
    )

    assert w.index.names == ["date", "asset"]
    assert round(float(w.sum()), 6) == 1.0
    assert float(w.max()) <= 0.6 + 1e-9
    assert float(w.loc[(pd.Timestamp("2024-01-02"), "C")]) == 0.0  # filtered by liquidity
