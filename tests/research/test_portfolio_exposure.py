from __future__ import annotations

import pandas as pd
import pytest

from qsys.research.portfolio_exposure import compute_portfolio_exposure, summarize_exposure_stability


def _base_index() -> pd.MultiIndex:
    return pd.MultiIndex.from_tuples(
        [
            ("2025-01-01", "A"),
            ("2025-01-01", "B"),
            ("2025-01-01", "C"),
            ("2025-01-02", "A"),
            ("2025-01-02", "B"),
            ("2025-01-02", "C"),
        ],
        names=["date", "asset"],
    )


def _weights_df() -> pd.DataFrame:
    return pd.DataFrame({"target_weight": [0.6, 0.4, 0.0, 0.5, 0.3, -0.2]}, index=_base_index())


def _exposures_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "vol_20d_z": [1.0, 2.0, 3.0, 1.5, 0.5, -1.0],
            "liquidity_z": [0.2, 0.0, -0.5, 0.1, 0.3, -0.2],
            "size_z": [2.0, 1.0, 0.0, 1.5, 1.0, 0.5],
        },
        index=_base_index(),
    )


def test_multiindex_requirement_for_weights_and_exposures() -> None:
    bad_weights = pd.DataFrame({"target_weight": [1.0]})
    good_exposures = _exposures_df()
    with pytest.raises(ValueError, match="weights must be MultiIndex"):
        compute_portfolio_exposure(bad_weights, good_exposures)

    bad_exposures = pd.DataFrame({"vol_20d_z": [1.0]})
    with pytest.raises(ValueError, match="exposures must be MultiIndex"):
        compute_portfolio_exposure(_weights_df(), bad_exposures)


def test_missing_weight_col_error_when_weights_is_dataframe() -> None:
    w = _weights_df().rename(columns={"target_weight": "w"})
    with pytest.raises(ValueError, match="weights missing required column"):
        compute_portfolio_exposure(w, _exposures_df())


def test_series_weights_support_and_output_columns() -> None:
    out = compute_portfolio_exposure(_weights_df()["target_weight"], _exposures_df())
    assert list(out.columns) == [
        "n_holdings",
        "gross_weight",
        "net_weight",
        "portfolio_vol_20d_z",
        "portfolio_liquidity_z",
        "portfolio_size_z",
    ]


def test_strict_index_alignment_and_formula() -> None:
    w = _weights_df().copy()
    ex = _exposures_df().copy()

    # Drop one row from exposures so it cannot be used in aligned formula.
    ex = ex.drop(index=[("2025-01-02", "C")])

    out = compute_portfolio_exposure(w, ex)

    # 2025-01-01 vol exposure = 0.6*1.0 + 0.4*2.0 + 0.0*3.0 = 1.4
    assert out.loc[pd.Timestamp("2025-01-01"), "portfolio_vol_20d_z"] == pytest.approx(1.4)
    # 2025-01-02 vol exposure aligns only A,B rows in exposures (C dropped): 0.5*1.5 + 0.3*0.5 = 0.9
    assert out.loc[pd.Timestamp("2025-01-02"), "portfolio_vol_20d_z"] == pytest.approx(0.9)


def test_n_holdings_gross_weight_net_weight() -> None:
    out = compute_portfolio_exposure(_weights_df(), _exposures_df())

    assert out.loc[pd.Timestamp("2025-01-01"), "n_holdings"] == 2
    assert out.loc[pd.Timestamp("2025-01-01"), "gross_weight"] == pytest.approx(1.0)
    assert out.loc[pd.Timestamp("2025-01-01"), "net_weight"] == pytest.approx(1.0)

    assert out.loc[pd.Timestamp("2025-01-02"), "n_holdings"] == 3
    assert out.loc[pd.Timestamp("2025-01-02"), "gross_weight"] == pytest.approx(1.0)
    assert out.loc[pd.Timestamp("2025-01-02"), "net_weight"] == pytest.approx(0.6)


def test_missing_exposure_values_not_filled_with_zero() -> None:
    ex = _exposures_df().copy()
    ex.loc[("2025-01-01", "A"), "vol_20d_z"] = pd.NA

    out = compute_portfolio_exposure(_weights_df(), ex)

    # A row is dropped for vol only; so 2025-01-01 vol exposure should be 0.4*2.0 = 0.8
    assert out.loc[pd.Timestamp("2025-01-01"), "portfolio_vol_20d_z"] == pytest.approx(0.8)
    # Other exposures still use full valid rows.
    assert out.loc[pd.Timestamp("2025-01-01"), "portfolio_size_z"] == pytest.approx(1.6)


def test_summarize_exposure_stability_output_schema_and_partial_nan() -> None:
    out = compute_portfolio_exposure(_weights_df(), _exposures_df())
    out.loc[pd.Timestamp("2025-01-02"), "portfolio_liquidity_z"] = pd.NA

    summary = summarize_exposure_stability(out)
    assert list(summary.columns) == ["mean", "std", "min", "p25", "median", "p75", "max"]
    assert "portfolio_vol_20d_z" in summary.index
    assert "n_holdings" in summary.index


def test_no_crash_with_partial_nans_in_inputs() -> None:
    w = _weights_df().copy()
    ex = _exposures_df().copy()
    w.loc[("2025-01-02", "B"), "target_weight"] = pd.NA
    ex.loc[("2025-01-02", "A"), "liquidity_z"] = pd.NA

    out = compute_portfolio_exposure(w, ex)
    assert isinstance(out, pd.DataFrame)
    assert len(out) >= 1
