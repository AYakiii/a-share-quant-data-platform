"""Signal persistence and turnover-style diagnostics."""

from __future__ import annotations

import pandas as pd


def signal_autocorrelation(signal: pd.Series, lag: int = 1) -> pd.Series:
    """Compute cross-sectional signal autocorrelation by date for a given lag."""

    if lag <= 0:
        raise ValueError("lag must be > 0")
    if not isinstance(signal.index, pd.MultiIndex) or signal.index.names != ["date", "asset"]:
        raise ValueError("signal must be MultiIndex [date, asset]")

    s = signal.sort_index()
    prev = s.groupby(level="asset").shift(lag)
    df = pd.concat([s.rename("cur"), prev.rename("prev")], axis=1)

    def _corr(g: pd.DataFrame) -> float:
        gg = g.dropna()
        if len(gg) < 2:
            return float("nan")
        return float(gg["cur"].corr(gg["prev"], method="pearson"))

    out = df.groupby(level="date").apply(_corr)
    out.name = f"autocorr_lag_{lag}"
    return out.sort_index()


def top_n_turnover(signal: pd.Series, top_n: int = 20) -> pd.Series:
    """Compute gross membership turnover of top-N bucket by date.

    Definition: ``1 - overlap_ratio`` where overlap ratio is
    ``|TopN_t ∩ TopN_{t-1}| / top_n``.
    """

    if top_n <= 0:
        raise ValueError("top_n must be > 0")
    if not isinstance(signal.index, pd.MultiIndex) or signal.index.names != ["date", "asset"]:
        raise ValueError("signal must be MultiIndex [date, asset]")

    s = signal.dropna().sort_index()
    top_sets: dict[pd.Timestamp, set[str]] = {}
    for d, g in s.groupby(level="date"):
        top_assets = g.droplevel("date").nlargest(min(top_n, len(g))).index
        top_sets[pd.Timestamp(d)] = set(top_assets.tolist())

    dates = sorted(top_sets.keys())
    turnover_vals: dict[pd.Timestamp, float] = {}
    prev: set[str] | None = None
    for d in dates:
        cur = top_sets[d]
        if prev is None:
            turnover_vals[d] = 1.0
        else:
            overlap = len(cur.intersection(prev)) / float(top_n)
            turnover_vals[d] = float(1.0 - overlap)
        prev = cur

    out = pd.Series(turnover_vals, name="top_n_turnover")
    return out.sort_index()
