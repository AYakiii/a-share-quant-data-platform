"""Information Coefficient diagnostics."""

from __future__ import annotations

import pandas as pd


def _validate_strict_alignment(signal: pd.Series, label: pd.Series) -> pd.DataFrame:
    if not isinstance(signal.index, pd.MultiIndex) or signal.index.names != ["date", "asset"]:
        raise ValueError("signal must be MultiIndex [date, asset]")
    if not isinstance(label.index, pd.MultiIndex) or label.index.names != ["date", "asset"]:
        raise ValueError("label must be MultiIndex [date, asset]")
    if not signal.index.equals(label.index):
        raise ValueError("signal and label must have identical [date, asset] index")
    return pd.concat([signal.rename("signal"), label.rename("label")], axis=1)


def daily_ic(signal: pd.Series, forward_return: pd.Series) -> pd.Series:
    """Compute daily cross-sectional IC (Pearson) between signal and forward return."""

    df = _validate_strict_alignment(signal, forward_return)

    def _corr(g: pd.DataFrame) -> float:
        gg = g.dropna(subset=["signal", "label"])
        if len(gg) < 2:
            return float("nan")
        return float(gg["signal"].corr(gg["label"], method="pearson"))

    out = df.groupby(level="date").apply(_corr)
    out.name = "daily_ic"
    return out.sort_index()


def daily_rank_ic(signal: pd.Series, forward_return: pd.Series) -> pd.Series:
    """Compute daily cross-sectional Rank IC (Spearman) between signal and forward return."""

    df = _validate_strict_alignment(signal, forward_return)

    def _corr(g: pd.DataFrame) -> float:
        gg = g.dropna(subset=["signal", "label"])
        if len(gg) < 2:
            return float("nan")
        return float(gg["signal"].corr(gg["label"], method="spearman"))

    out = df.groupby(level="date").apply(_corr)
    out.name = "daily_rank_ic"
    return out.sort_index()


def ic_summary(signal: pd.Series, forward_return: pd.Series) -> pd.Series:
    """Summarize daily IC diagnostics."""

    ic = daily_ic(signal, forward_return)
    ric = daily_rank_ic(signal, forward_return)
    return pd.Series(
        {
            "ic_mean": float(ic.mean()),
            "ic_std": float(ic.std(ddof=0)),
            "rank_ic_mean": float(ric.mean()),
            "rank_ic_std": float(ric.std(ddof=0)),
            "n_days": int(ic.notna().sum()),
        }
    )
