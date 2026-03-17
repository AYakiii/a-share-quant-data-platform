"""Cross-sectional quantile diagnostics."""

from __future__ import annotations

import pandas as pd

from qsys.research.ic import _validate_strict_alignment


def quantile_mean_forward_returns(
    signal: pd.Series,
    forward_return: pd.Series,
    *,
    n_quantiles: int = 5,
) -> pd.DataFrame:
    """Compute per-date forward return means by cross-sectional signal quantiles."""

    if n_quantiles < 2:
        raise ValueError("n_quantiles must be >= 2")

    df = _validate_strict_alignment(signal, forward_return).dropna(subset=["signal", "label"])

    def _assign_quantile(g: pd.DataFrame) -> pd.DataFrame:
        if g["signal"].nunique() < 2:
            g = g.copy()
            g["quantile"] = pd.NA
            return g
        g = g.copy()
        g["quantile"] = pd.qcut(g["signal"], q=n_quantiles, labels=False, duplicates="drop")
        g["quantile"] = g["quantile"].astype("Int64") + 1
        return g

    tagged = df.groupby(level="date", group_keys=False).apply(_assign_quantile)
    tagged = tagged.dropna(subset=["quantile"])
    out = tagged.groupby([tagged.index.get_level_values("date"), tagged["quantile"]])["label"].mean()
    out = out.rename("mean_forward_return").reset_index().rename(columns={"level_0": "date"})
    return out.sort_values(["date", "quantile"]).reset_index(drop=True)


def quantile_spread(
    signal: pd.Series,
    forward_return: pd.Series,
    *,
    n_quantiles: int = 5,
) -> pd.Series:
    """Compute top-minus-bottom quantile forward-return spread by date."""

    qdf = quantile_mean_forward_returns(signal, forward_return, n_quantiles=n_quantiles)
    if qdf.empty:
        return pd.Series(dtype=float, name="quantile_spread")

    top = qdf[qdf["quantile"] == qdf["quantile"].max()].set_index("date")["mean_forward_return"]
    bottom = qdf[qdf["quantile"] == qdf["quantile"].min()].set_index("date")["mean_forward_return"]
    spread = top - bottom
    spread.name = "quantile_spread"
    return spread.sort_index()
