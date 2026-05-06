"""Rank IC diagnostics."""

from __future__ import annotations

import math

import pandas as pd


def compute_ic_by_date(
    df: pd.DataFrame,
    signal_col: str = "signal",
    return_col: str = "fwd_ret_5d",
    method: str = "spearman",
) -> pd.Series:
    """Compute date-wise IC between signal and forward return."""

    if signal_col not in df.columns or return_col not in df.columns:
        raise KeyError("signal or return column missing")
    if method not in {"spearman", "pearson"}:
        raise ValueError("method must be 'spearman' or 'pearson'")

    def _corr(g: pd.DataFrame) -> float:
        d = g[[signal_col, return_col]].dropna()
        if len(d) < 2:
            return float("nan")
        return float(d[signal_col].corr(d[return_col], method=method))

    ic = df.groupby(level="date", group_keys=False).apply(_corr)
    ic.name = f"ic_{return_col}"
    return ic.sort_index()


def summarize_ic(ic_series: pd.Series) -> dict[str, float]:
    """Summarize IC distribution and ICIR/t-stat."""

    s = ic_series.dropna().astype(float)
    n = len(s)
    mean_ic = float(s.mean()) if n else float("nan")
    std_ic = float(s.std(ddof=1)) if n > 1 else float("nan")
    icir = float(mean_ic / std_ic) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")
    t_stat = float(mean_ic / (std_ic / (n**0.5))) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")

    return {
        "mean_ic": mean_ic,
        "median_ic": float(s.median()) if n else float("nan"),
        "std_ic": std_ic,
        "icir": icir,
        "t_stat": t_stat,
        "positive_rate": float((s > 0).mean()) if n else float("nan"),
        "n_dates": float(n),
    }
