from __future__ import annotations

import numpy as np
import pandas as pd

from qsys.factors.factor_diagnostics import (
    compute_factor_correlation,
    compute_factor_ic_by_date,
    find_highly_correlated_factors,
    run_basic_factor_diagnostics,
    summarize_factor_coverage,
    summarize_factor_distribution,
    summarize_ic,
)


def _make_factor_frame() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=4, freq="D")
    assets = ["A", "B", "C", "D"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])

    f1 = np.tile([1.0, 2.0, 3.0, 4.0], len(dates))
    f2 = f1 * 2.0
    f3 = np.tile([4.0, 3.0, 2.0, 1.0], len(dates))
    out = pd.DataFrame({"factor_1": f1, "factor_2": f2, "factor_3": f3}, index=idx)
    out.iloc[0, 0] = np.nan
    return out


def _make_labels(factors: pd.DataFrame) -> pd.DataFrame:
    y = factors["factor_1"].copy()
    return pd.DataFrame({"fwd_ret_5d": y, "fwd_ret_20d": -y}, index=factors.index)


def test_coverage_summary_expected_missingness() -> None:
    factors = _make_factor_frame()
    cov = summarize_factor_coverage(factors)
    r = cov[cov["factor_name"] == "factor_1"].iloc[0]
    assert int(r["n_missing"]) == 1
    assert int(r["n_non_null"]) == len(factors) - 1


def test_distribution_summary_quantile_columns_present() -> None:
    factors = _make_factor_frame()
    dist = summarize_factor_distribution(factors)
    expected = {"factor_name", "mean", "std", "min", "p01", "p05", "p25", "median", "p75", "p95", "p99", "max", "n_non_null"}
    assert expected.issubset(dist.columns)


def test_correlation_matrix_square_and_has_factor_columns() -> None:
    factors = _make_factor_frame()
    corr = compute_factor_correlation(factors, method="spearman", min_periods=1)
    assert corr.shape[0] == corr.shape[1]
    assert set(corr.columns) == {"factor_1", "factor_2", "factor_3"}


def test_find_highly_correlated_factors_expected_pair() -> None:
    factors = _make_factor_frame()
    corr = compute_factor_correlation(factors, method="pearson", min_periods=1)
    pairs = find_highly_correlated_factors(corr, threshold=0.95)
    assert ((pairs["factor_a"] == "factor_1") & (pairs["factor_b"] == "factor_2")).any()
    assert not (pairs["factor_a"] == pairs["factor_b"]).any()


def test_ic_by_date_computes_expected_spearman() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    ic = compute_factor_ic_by_date(factors, labels, label_col="fwd_ret_5d", method="spearman", min_assets=3)
    d = ic[(ic["factor_name"] == "factor_1") & (ic["date"] == pd.Timestamp("2025-01-02"))].iloc[0]
    assert abs(float(d["ic"]) - 1.0) < 1e-12


def test_ic_by_date_handles_insufficient_assets_deterministically() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    ic = compute_factor_ic_by_date(factors, labels, label_col="fwd_ret_5d", method="spearman", min_assets=10)
    assert ic["ic"].isna().all()


def test_summarize_ic_returns_required_stats() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    ic = compute_factor_ic_by_date(factors, labels, label_col="fwd_ret_5d", method="spearman", min_assets=3)
    s = summarize_ic(ic)
    expected = {"factor_name", "label_col", "mean_ic", "median_ic", "std_ic", "icir", "t_stat", "positive_rate", "n_dates"}
    assert expected.issubset(s.columns)


def test_run_basic_factor_diagnostics_without_labels() -> None:
    factors = _make_factor_frame()
    out = run_basic_factor_diagnostics(factors)
    assert {"coverage", "distribution", "correlation", "high_correlation_pairs"}.issubset(out.keys())
    assert "ic_by_date" not in out


def test_run_basic_factor_diagnostics_with_labels() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    out = run_basic_factor_diagnostics(factors, labels=labels, label_cols=["fwd_ret_5d", "fwd_ret_20d"], min_assets=3)
    assert "ic_by_date" in out and "ic_summary" in out
    assert set(out["ic_by_date"]["label_col"].unique()) == {"fwd_ret_5d", "fwd_ret_20d"}


def test_labels_not_included_as_factor_columns() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    out = run_basic_factor_diagnostics(factors, labels=labels, label_cols=["fwd_ret_5d"], min_assets=3)
    assert "fwd_ret_5d" not in out["correlation"].columns


def test_diagnostics_do_not_mutate_inputs() -> None:
    factors = _make_factor_frame()
    labels = _make_labels(factors)
    factors_before = factors.copy(deep=True)
    labels_before = labels.copy(deep=True)
    _ = run_basic_factor_diagnostics(factors, labels=labels, label_cols=["fwd_ret_5d"], min_assets=3)
    pd.testing.assert_frame_equal(factors, factors_before)
    pd.testing.assert_frame_equal(labels, labels_before)
