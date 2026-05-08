from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qsys.risk.exposure import build_risk_exposure_matrix


def _sample_features() -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples(
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
    return pd.DataFrame(
        {
            "vol_20d": [1.0, 2.0, 3.0, 2.0, 4.0, 6.0],
            "amount_20d": [10.0, 100.0, 1000.0, 20.0, 200.0, 2000.0],
            "market_cap": [100.0, 400.0, 1600.0, 200.0, 800.0, 3200.0],
        },
        index=idx,
    )


def test_requires_multiindex_date_asset() -> None:
    df = pd.DataFrame({"vol_20d": [1.0], "amount_20d": [1.0], "market_cap": [1.0]})
    with pytest.raises(ValueError, match="MultiIndex"):
        build_risk_exposure_matrix(df)


def test_output_columns_and_same_index() -> None:
    features = _sample_features()
    out = build_risk_exposure_matrix(features)

    assert list(out.columns) == ["vol_20d_z", "liquidity_z", "size_z"]
    assert out.index.equals(features.index)


def test_per_date_cross_sectional_zscore_behavior() -> None:
    features = _sample_features()
    out = build_risk_exposure_matrix(features)

    for col in ["vol_20d_z", "liquidity_z", "size_z"]:
        g = out[col].groupby(level="date")
        means = g.mean()
        stds = g.std(ddof=0)
        assert np.allclose(means.values, 0.0, atol=1e-12)
        assert np.allclose(stds.values, 1.0, atol=1e-12)


def test_non_positive_amount_and_market_cap_become_nan() -> None:
    features = _sample_features().copy()
    features.loc[("2025-01-01", "A"), "amount_20d"] = 0.0
    features.loc[("2025-01-02", "B"), "market_cap"] = -10.0

    out = build_risk_exposure_matrix(features)

    assert pd.isna(out.loc[("2025-01-01", "A"), "liquidity_z"])
    assert pd.isna(out.loc[("2025-01-02", "B"), "size_z"])


def test_missing_column_error() -> None:
    features = _sample_features().drop(columns=["amount_20d"])
    with pytest.raises(ValueError, match="missing required columns"):
        build_risk_exposure_matrix(features)
