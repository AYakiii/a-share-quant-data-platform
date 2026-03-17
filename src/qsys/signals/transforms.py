"""Cross-sectional signal transforms for Signal Engine v1."""

from __future__ import annotations

import numpy as np
import pandas as pd


MultiIndexed = pd.Series | pd.DataFrame


def _ensure_multiindex(obj: MultiIndexed) -> None:
    if not isinstance(obj.index, pd.MultiIndex) or obj.index.names != ["date", "asset"]:
        raise ValueError("Input must be indexed by MultiIndex [date, asset]")


def winsorize_cross_section(series: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
    """Winsorize a signal cross-sectionally by date."""

    _ensure_multiindex(series)

    def _clip(group: pd.Series) -> pd.Series:
        lo = group.quantile(lower_q)
        hi = group.quantile(upper_q)
        return group.clip(lower=lo, upper=hi)

    out = series.groupby(level="date", group_keys=False).apply(_clip)
    out.name = series.name
    return out.sort_index()


def zscore_cross_section(series: pd.Series) -> pd.Series:
    """Z-score normalize a signal cross-sectionally by date."""

    _ensure_multiindex(series)

    def _z(group: pd.Series) -> pd.Series:
        std = group.std(ddof=0)
        if pd.isna(std) or std == 0:
            return pd.Series(0.0, index=group.index)
        return (group - group.mean()) / std

    out = series.groupby(level="date", group_keys=False).apply(_z)
    out.name = series.name
    return out.sort_index()


def rank_cross_section(series: pd.Series, pct: bool = True) -> pd.Series:
    """Rank a signal cross-sectionally by date."""

    _ensure_multiindex(series)
    out = series.groupby(level="date").rank(method="average", pct=pct)
    out.name = series.name
    return out.sort_index()


def neutralize_by_size(series: pd.Series, market_cap: pd.Series) -> pd.Series:
    """Neutralize a signal by log market cap within each date cross-section."""

    _ensure_multiindex(series)
    _ensure_multiindex(market_cap)

    joined = pd.concat([series.rename("y"), market_cap.rename("market_cap")], axis=1)

    def _neutralize(group: pd.DataFrame) -> pd.Series:
        g = group.dropna(subset=["y", "market_cap"])
        out = pd.Series(np.nan, index=group.index)
        if len(g) < 2:
            return out
        x = np.log(g["market_cap"].astype(float).clip(lower=1e-12)).to_numpy()
        y = g["y"].astype(float).to_numpy()
        x_mat = np.column_stack([np.ones_like(x), x])
        beta, *_ = np.linalg.lstsq(x_mat, y, rcond=None)
        resid = y - x_mat @ beta
        out.loc[g.index] = resid
        return out

    out = joined.groupby(level="date", group_keys=False).apply(_neutralize)
    out.name = series.name
    return out.sort_index()


def neutralize_by_group(series: pd.Series, group_labels: pd.Series) -> pd.Series:
    """Neutralize by demeaning within (date, group) buckets."""

    _ensure_multiindex(series)
    _ensure_multiindex(group_labels)

    joined = pd.concat([series.rename("signal"), group_labels.rename("group")], axis=1)
    demeaned = joined.groupby([joined.index.get_level_values("date"), joined["group"]])["signal"].transform(
        lambda s: s - s.mean()
    )
    demeaned.name = series.name
    return demeaned.sort_index()
