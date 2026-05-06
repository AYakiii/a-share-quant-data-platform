"""Quantile forward-return diagnostics for cross-sectional signals."""

from __future__ import annotations

import pandas as pd


def assign_quantiles_by_date(df: pd.DataFrame, signal_col: str = "signal", q: int = 5) -> pd.DataFrame:
    """Assign cross-sectional quantiles by date (1..q)."""

    if signal_col not in df.columns:
        raise KeyError(f"missing signal column: {signal_col}")

    def _qcut(g: pd.DataFrame) -> pd.Series:
        s = g[signal_col]
        try:
            return pd.qcut(s.rank(method="first"), q=q, labels=False) + 1
        except ValueError:
            return pd.Series([pd.NA] * len(g), index=g.index)

    out = df.copy()
    out["quantile"] = out.groupby(level="date", group_keys=False).apply(_qcut)
    return out


def compute_quantile_forward_returns(
    df: pd.DataFrame,
    quantile_col: str = "quantile",
    return_col: str = "fwd_ret_5d",
) -> pd.DataFrame:
    """Compute per-date average forward return by quantile."""

    qret = (
        df.dropna(subset=[quantile_col, return_col])
        .groupby([df.index.get_level_values("date"), quantile_col])[return_col]
        .mean()
        .rename("mean_forward_return")
        .reset_index()
        .rename(columns={"level_0": "date"})
    )
    qret.columns = ["date", "quantile", "mean_forward_return"]
    return qret.sort_values(["date", "quantile"], kind="mergesort").reset_index(drop=True)


def compute_quantile_spread(
    quantile_return_by_date: pd.DataFrame,
    top_quantile: int = 5,
    bottom_quantile: int = 1,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compute top-minus-bottom quantile spread by date and summary stats."""

    q = quantile_return_by_date.copy()
    piv = q.pivot(index="date", columns="quantile", values="mean_forward_return")
    spread = (piv.get(top_quantile) - piv.get(bottom_quantile)).rename("top_minus_bottom")
    universe = piv.mean(axis=1)
    top_minus_universe = (piv.get(top_quantile) - universe).rename("top_minus_universe")

    spread_df = pd.concat([spread, top_minus_universe], axis=1).reset_index()

    s = spread.dropna()
    n = len(s)
    std = float(s.std(ddof=1)) if n > 1 else float("nan")
    t_stat = float(s.mean() / (std / (n**0.5))) if n > 1 and std and pd.notna(std) else float("nan")

    summary = {
        "mean_top_minus_bottom": float(s.mean()) if n else float("nan"),
        "mean_top_minus_universe": float(top_minus_universe.dropna().mean()) if len(top_minus_universe.dropna()) else float("nan"),
        "spread_t_stat": t_stat,
        "n_dates": float(n),
    }
    return spread_df, summary
