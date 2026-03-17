from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.backtest.portfolio import build_top_n_portfolio


def test_build_top_n_portfolio_contract() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-02"), "C"),
        ],
        names=["date", "asset"],
    )
    sig = pd.Series([0.1, 0.3, 0.2], index=idx)

    w = build_top_n_portfolio(sig, top_n=2, long_only=True)

    assert w.index.names == ["date", "asset"]
    assert round(float(w.sum()), 6) == 1.0
    assert float(w.loc[(pd.Timestamp("2024-01-02"), "B")]) == 0.5
    assert float(w.loc[(pd.Timestamp("2024-01-02"), "C")]) == 0.5
    assert float(w.loc[(pd.Timestamp("2024-01-02"), "A")]) == 0.0
