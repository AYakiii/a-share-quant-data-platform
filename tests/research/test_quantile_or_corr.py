from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.correlation import pairwise_signal_correlation
from qsys.research.quantiles import quantile_mean_forward_returns


def test_quantile_analysis_cross_sectional_contract() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-02"), "C"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
            (pd.Timestamp("2024-01-03"), "C"),
        ],
        names=["date", "asset"],
    )

    signal = pd.Series([1, 2, 3, 3, 2, 1], index=idx, dtype=float)
    label = pd.Series([0.01, 0.02, 0.03, 0.03, 0.02, 0.01], index=idx, dtype=float)

    q = quantile_mean_forward_returns(signal, label, n_quantiles=3)
    assert set(q.columns) == {"date", "quantile", "mean_forward_return"}
    assert q.groupby("date")["quantile"].nunique().min() >= 3


def test_pairwise_signal_correlation_intersection_only() -> None:
    idx1 = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "A"), (pd.Timestamp("2024-01-02"), "B")], names=["date", "asset"]
    )
    idx2 = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "B"), (pd.Timestamp("2024-01-02"), "C")], names=["date", "asset"]
    )
    s1 = pd.Series([1.0, 2.0], index=idx1)
    s2 = pd.Series([2.0, 3.0], index=idx2)

    corr = pairwise_signal_correlation({"s1": s1, "s2": s2})
    assert int(corr.iloc[0]["n_obs"]) == 1
