from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.signal_quality.ic import compute_ic_by_date
from qsys.research.signal_quality.quantile import (
    assign_quantiles_by_date,
    compute_quantile_forward_returns,
    compute_quantile_spread,
)
from qsys.utils.run_signal_quality_mvp import build_signal_quality_input


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


def _make_feature_df(include_ret20: bool = True, include_vol20: bool = True) -> pd.DataFrame:
    dates = pd.to_datetime(["2025-01-01", "2025-01-02"])
    assets = ["A", "B", "C", "D", "E"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])
    data = {"fwd_ret_5d": [1, 2, 3, 4, 5] * 2, "signal": [1, 1, 1, 1, 1] * 2}
    if include_ret20:
        data["ret_20d"] = [1, 2, 3, 4, 5] * 2
    if include_vol20:
        data["vol_20d"] = [5, 4, 3, 2, 1] * 2
    return pd.DataFrame(data, index=idx)


def test_signal_preset_momentum_vol_creates_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_df = _make_feature_df()
    monkeypatch.setattr("qsys.utils.run_signal_quality_mvp.load_feature_store_frame", lambda **_: feature_df)

    df, _ = build_signal_quality_input(
        feature_root="dummy",
        signal_col=None,
        signal_preset="momentum_vol",
        fwd_cols=["fwd_ret_5d"],
        start_date=None,
        end_date=None,
    )
    assert "signal" in df.columns


def test_signal_preset_missing_ret20_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_df = _make_feature_df(include_ret20=False)
    monkeypatch.setattr("qsys.utils.run_signal_quality_mvp.load_feature_store_frame", lambda **_: feature_df)
    with pytest.raises(KeyError, match="ret_20d"):
        build_signal_quality_input("dummy", None, "momentum_vol", ["fwd_ret_5d"], None, None)


def test_signal_preset_missing_vol20_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_df = _make_feature_df(include_vol20=False)
    monkeypatch.setattr("qsys.utils.run_signal_quality_mvp.load_feature_store_frame", lambda **_: feature_df)
    with pytest.raises(KeyError, match="vol_20d"):
        build_signal_quality_input("dummy", None, "momentum_vol", ["fwd_ret_5d"], None, None)


def test_existing_signal_col_behavior_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    feature_df = _make_feature_df()
    def fake_prepare(**kwargs):
        return feature_df[["signal", "fwd_ret_5d"]], {"n_rows_before": 10.0}

    monkeypatch.setattr("qsys.utils.run_signal_quality_mvp.prepare_signal_quality_frame", fake_prepare)
    df, _ = build_signal_quality_input("dummy", "signal", None, ["fwd_ret_5d"], None, None)
    assert "signal" in df.columns and "fwd_ret_5d" in df.columns
