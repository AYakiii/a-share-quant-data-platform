"""Risk exposure matrix construction helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _validate_features_frame(features: pd.DataFrame, required: list[str]) -> None:
    if not isinstance(features.index, pd.MultiIndex) or features.index.names != ["date", "asset"]:
        raise ValueError("features must be MultiIndex [date, asset]")

    missing = [c for c in required if c not in features.columns]
    if missing:
        raise ValueError(f"features missing required columns: {missing}")


def _cross_sectional_zscore_by_date(series: pd.Series, *, winsorize: bool = False) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")

    def _z(g: pd.Series) -> pd.Series:
        gg = g.copy()
        if winsorize:
            lo = gg.quantile(0.01)
            hi = gg.quantile(0.99)
            gg = gg.clip(lower=lo, upper=hi)

        mean = gg.mean()
        std = gg.std(ddof=0)
        if pd.isna(std) or std == 0:
            return pd.Series(np.nan, index=gg.index, dtype=float)
        return (gg - mean) / std

    return s.groupby(level="date", group_keys=False).apply(_z)


def build_risk_exposure_matrix(
    features: pd.DataFrame,
    *,
    vol_col: str = "vol_20d",
    liquidity_col: str = "amount_20d",
    size_col: str = "market_cap",
    winsorize: bool = False,
) -> pd.DataFrame:
    """Build cross-sectional risk exposure z-scores by date."""

    _validate_features_frame(features, [vol_col, liquidity_col, size_col])

    out = pd.DataFrame(index=features.index)

    vol = pd.to_numeric(features[vol_col], errors="coerce")
    liq_raw = pd.to_numeric(features[liquidity_col], errors="coerce")
    size_raw = pd.to_numeric(features[size_col], errors="coerce")

    liq_log = np.log(liq_raw.where(liq_raw > 0))
    size_log = np.log(size_raw.where(size_raw > 0))

    out["vol_20d_z"] = _cross_sectional_zscore_by_date(vol, winsorize=winsorize)
    out["liquidity_z"] = _cross_sectional_zscore_by_date(liq_log, winsorize=winsorize)
    out["size_z"] = _cross_sectional_zscore_by_date(size_log, winsorize=winsorize)

    return out
