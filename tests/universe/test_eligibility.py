from __future__ import annotations

import pandas as pd
import pytest

from qsys.universe.eligibility import apply_eligibility_mask, build_eligibility_mask


def _index() -> pd.MultiIndex:
    return pd.MultiIndex.from_tuples(
        [
            ("2025-01-01", "A"),
            ("2025-01-01", "B"),
            ("2025-01-02", "A"),
            ("2025-01-02", "B"),
        ],
        names=["date", "asset"],
    )


def _features() -> pd.DataFrame:
    idx = _index()
    return pd.DataFrame(
        {
            "is_tradable": [True, False, True, True],
            "amount_20d": [100.0, 30.0, 120.0, 80.0],
            "turnover_20d": [0.2, 0.05, 0.3, 0.08],
            "market_cap": [500.0, 200.0, 600.0, 400.0],
            "ret_20d": [0.1, 0.2, 0.3, None],
        },
        index=idx,
    )


def test_build_requires_multiindex() -> None:
    df = pd.DataFrame({"is_tradable": [True]})
    with pytest.raises(ValueError, match="MultiIndex"):
        build_eligibility_mask(df)


def test_output_bool_series_name_and_same_index() -> None:
    features = _features()
    out = build_eligibility_mask(features)
    assert isinstance(out, pd.Series)
    assert out.name == "is_eligible"
    assert pd.api.types.is_bool_dtype(out.dtype)
    assert out.index.equals(features.index)


def test_require_columns_non_null_behavior() -> None:
    features = _features()
    out = build_eligibility_mask(features, require_columns=("ret_20d",))

    assert out.loc[("2025-01-02", "B")] == False


def test_tradable_filtering_when_column_exists() -> None:
    features = _features()
    out = build_eligibility_mask(features, require_tradable=True)
    assert out.loc[("2025-01-01", "B")] == False
    assert out.loc[("2025-01-01", "A")] == True


def test_missing_tradable_col_does_not_fail_by_default() -> None:
    features = _features().drop(columns=["is_tradable"])
    out = build_eligibility_mask(features, require_tradable=True)
    assert out.all()


def test_optional_threshold_filters() -> None:
    features = _features()
    out = build_eligibility_mask(
        features,
        min_amount_20d=90.0,
        min_turnover_20d=0.1,
        min_market_cap=450.0,
    )
    assert out.loc[("2025-01-01", "A")] == True
    assert out.loc[("2025-01-01", "B")] == False
    assert out.loc[("2025-01-02", "B")] == False


def test_threshold_column_missing_raises() -> None:
    features = _features().drop(columns=["amount_20d"])
    with pytest.raises(ValueError, match="min_amount_20d"):
        build_eligibility_mask(features, min_amount_20d=10.0)


def test_apply_mask_for_series_and_dataframe_and_intersection() -> None:
    idx = _index()
    s = pd.Series([1, 2, 3, 4], index=idx, name="x")
    df = pd.DataFrame({"x": [1, 2, 3, 4]}, index=idx)

    elig_idx = pd.MultiIndex.from_tuples(
        [("2025-01-01", "A"), ("2025-01-02", "A")], names=["date", "asset"]
    )
    eligible = pd.Series([True, False], index=elig_idx, name="is_eligible")

    out_s = apply_eligibility_mask(s, eligible)
    out_df = apply_eligibility_mask(df, eligible)

    assert isinstance(out_s, pd.Series)
    assert isinstance(out_df, pd.DataFrame)
    assert list(out_s.index) == [("2025-01-01", "A")]
    assert list(out_df.index) == [("2025-01-01", "A")]


def test_apply_mask_missing_eligibility_not_treated_true() -> None:
    idx = _index()
    df = pd.DataFrame({"x": [1, 2, 3, 4]}, index=idx)

    # Missing two keys from eligibility; those should not be included.
    elig_idx = pd.MultiIndex.from_tuples(
        [("2025-01-01", "A"), ("2025-01-01", "B")], names=["date", "asset"]
    )
    eligible = pd.Series([True, False], index=elig_idx, name="is_eligible")

    out = apply_eligibility_mask(df, eligible)
    assert list(out.index) == [("2025-01-01", "A")]
