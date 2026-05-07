"""Universe eligibility mask builders and appliers."""

from __future__ import annotations

import pandas as pd


def _validate_multiindex(obj: pd.Series | pd.DataFrame, *, arg_name: str) -> None:
    if not isinstance(obj.index, pd.MultiIndex) or obj.index.names != ["date", "asset"]:
        raise ValueError(f"{arg_name} must be MultiIndex ['date', 'asset']")


def build_eligibility_mask(
    features: pd.DataFrame,
    *,
    require_columns: tuple[str, ...] | None = None,
    require_tradable: bool = True,
    tradable_col: str = "is_tradable",
    min_amount_20d: float | None = None,
    amount_col: str = "amount_20d",
    min_turnover_20d: float | None = None,
    turnover_col: str = "turnover_20d",
    min_market_cap: float | None = None,
    market_cap_col: str = "market_cap",
) -> pd.Series:
    """Build daily asset eligibility mask from data-availability and hard filters."""

    _validate_multiindex(features, arg_name="features")

    eligible = pd.Series(True, index=features.index, name="is_eligible", dtype=bool)

    if require_columns is not None:
        missing = [c for c in require_columns if c not in features.columns]
        if missing:
            raise ValueError(f"features missing required columns: {missing}")
        for col in require_columns:
            eligible &= features[col].notna()

    if require_tradable and tradable_col in features.columns:
        tradable = features[tradable_col]
        eligible &= tradable.eq(True)

    if min_amount_20d is not None:
        if amount_col not in features.columns:
            raise ValueError(f"features missing required column for min_amount_20d: {amount_col}")
        eligible &= pd.to_numeric(features[amount_col], errors="coerce") >= float(min_amount_20d)

    if min_turnover_20d is not None:
        if turnover_col not in features.columns:
            raise ValueError(f"features missing required column for min_turnover_20d: {turnover_col}")
        eligible &= pd.to_numeric(features[turnover_col], errors="coerce") >= float(min_turnover_20d)

    if min_market_cap is not None:
        if market_cap_col not in features.columns:
            raise ValueError(f"features missing required column for min_market_cap: {market_cap_col}")
        eligible &= pd.to_numeric(features[market_cap_col], errors="coerce") >= float(min_market_cap)

    return eligible.astype(bool).rename("is_eligible")


def apply_eligibility_mask(obj: pd.Series | pd.DataFrame, eligible: pd.Series) -> pd.Series | pd.DataFrame:
    """Filter MultiIndex object by eligible intersection where eligible is True."""

    _validate_multiindex(obj, arg_name="obj")
    _validate_multiindex(eligible, arg_name="eligible")

    if not isinstance(eligible, pd.Series):
        raise ValueError("eligible must be a Series")

    if not pd.api.types.is_bool_dtype(eligible.dtype):
        non_na = eligible.dropna()
        if len(non_na) and not non_na.isin([True, False]).all():
            raise ValueError("eligible must be bool-like")

    common_idx = obj.index.intersection(eligible.index)
    if len(common_idx) == 0:
        return obj.iloc[0:0].copy()

    obj_aligned = obj.loc[common_idx]
    elig_aligned = eligible.loc[common_idx].astype("boolean")
    keep = elig_aligned == True

    return obj_aligned.loc[keep]
