from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.signal_quality.ic import compute_ic_by_date
from qsys.research.signal_quality.quantile import (
    assign_quantiles_by_date,
    compute_quantile_forward_returns,
    compute_quantile_spread,
)


def _make_df(kind: str) -> pd.DataFrame:
    dates = pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])
    assets = ["A", "B", "C", "D", "E"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])

    base = pd.Series([1, 2, 3, 4, 5] * len(dates), index=idx, dtype=float)
    if kind == "positive":
        signal = base
        fwd = base
    elif kind == "negative":
        signal = base
        fwd = -base
    else:
        signal = base
        fwd = pd.Series([3, 1, 4, 5, 2] * len(dates), index=idx, dtype=float)

    return pd.DataFrame({"signal": signal, "fwd_ret_5d": fwd}, index=idx)


def test_positive_signal_positive_rank_ic() -> None:
    df = _make_df("positive")
    ic = compute_ic_by_date(df, signal_col="signal", return_col="fwd_ret_5d", method="spearman")
    assert float(ic.mean()) > 0.9


def test_inverted_signal_negative_rank_ic() -> None:
    df = _make_df("negative")
    ic = compute_ic_by_date(df, signal_col="signal", return_col="fwd_ret_5d", method="spearman")
    assert float(ic.mean()) < -0.9


def test_random_like_signal_near_zero_ic() -> None:
    df = _make_df("random")
    ic = compute_ic_by_date(df, signal_col="signal", return_col="fwd_ret_5d", method="spearman")
    assert abs(float(ic.mean())) < 0.6


def test_quantile_spread_positive_for_positive_signal() -> None:
    df = _make_df("positive")
    qdf = assign_quantiles_by_date(df, signal_col="signal", q=5)
    qret = compute_quantile_forward_returns(qdf, quantile_col="quantile", return_col="fwd_ret_5d")
    spread_df, summary = compute_quantile_spread(qret, top_quantile=5, bottom_quantile=1)
    assert summary["mean_top_minus_bottom"] > 0
    assert "top_minus_bottom" in spread_df.columns
