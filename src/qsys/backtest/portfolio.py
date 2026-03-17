"""Portfolio construction helpers for Backtest MVP."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _normalize_weights(w: pd.Series, *, long_only: bool) -> pd.Series:
    if long_only:
        total = w[w > 0].sum()
        return (w / total) if total > 0 else w

    longs = w[w > 0]
    shorts = w[w < 0]
    out = w.copy()
    if len(longs):
        out.loc[longs.index] = longs / longs.sum()
    if len(shorts):
        out.loc[shorts.index] = shorts / abs(shorts.sum())
    return out


def _apply_group_cap_long_only(
    w: pd.Series,
    groups: pd.Series,
    group_cap: float,
    *,
    max_iter: int = 8,
) -> pd.Series:
    out = w.copy()
    for _ in range(max_iter):
        changed = False
        grp_sum = out.groupby(groups).sum()
        for g, total in grp_sum.items():
            if pd.isna(g):
                continue
            if total > group_cap and total > 0:
                factor = group_cap / total
                idx = groups[groups == g].index
                out.loc[idx] = out.loc[idx] * factor
                changed = True
        out = _normalize_weights(out, long_only=True)
        if not changed:
            break
    return out


def build_top_n_portfolio(
    signal: pd.Series,
    top_n: int,
    *,
    long_only: bool = True,
    bottom_n: int | None = None,
    max_single_weight: float | None = None,
    liquidity: pd.Series | None = None,
    min_liquidity: float | None = None,
    market_cap: pd.Series | None = None,
    size_aware_scaling: bool = False,
    group_labels: pd.Series | None = None,
    group_cap: float | None = None,
) -> pd.Series:
    """Build constrained top-N portfolio from cross-sectional signals by date.

    Constraints are applied using same-date data only (no look-ahead):
    - liquidity filter
    - optional size-aware scaling by log market cap
    - max single-name cap
    - optional long-only group cap
    """

    if not isinstance(signal.index, pd.MultiIndex) or signal.index.names != ["date", "asset"]:
        raise ValueError("signal must be MultiIndex [date, asset]")
    if top_n <= 0:
        raise ValueError("top_n must be > 0")
    if max_single_weight is not None and max_single_weight <= 0:
        raise ValueError("max_single_weight must be > 0")

    base = signal.dropna().sort_index()

    def _build(group: pd.Series) -> pd.Series:
        if group.empty:
            return pd.Series(dtype=float, index=group.index)

        date = group.index.get_level_values("date")[0]
        g = group.copy()

        # liquidity filter (same-date only)
        if liquidity is not None and min_liquidity is not None:
            liq_full = liquidity[liquidity.index.get_level_values("date") == date]
            liq_g = liq_full.droplevel("date") if len(liq_full) else pd.Series(dtype=float)
            keep_assets = liq_g[liq_g >= float(min_liquidity)].index
            g = g[g.droplevel("date").isin(keep_assets)]

        if g.empty:
            return pd.Series(0.0, index=group.index)

        longs = g.nlargest(min(top_n, len(g)))
        w = pd.Series(0.0, index=group.index)
        if len(longs) > 0:
            w.loc[longs.index] = 1.0 / len(longs)

        if not long_only:
            n_short = bottom_n if bottom_n is not None else top_n
            short_pool = g.drop(index=longs.index, errors="ignore")
            shorts = short_pool.nsmallest(min(n_short, len(short_pool)))
            if len(shorts) > 0:
                w.loc[shorts.index] = -(1.0 / len(shorts))

        # size-aware scaling (same-date market cap only)
        if size_aware_scaling and market_cap is not None:
            mc_full = market_cap[market_cap.index.get_level_values("date") == date]
            mc_g = mc_full.reindex(w.index)
            scale = pd.to_numeric(mc_g, errors="coerce").clip(lower=1e-12)
            scale = np.log(scale)
            scale = scale / scale.abs().max() if scale.abs().max() and not pd.isna(scale.abs().max()) else scale
            scale = scale.fillna(0.0)
            w = w * (1.0 + 0.5 * scale)

        # max single-name cap
        if max_single_weight is not None:
            if long_only:
                w = w.clip(lower=0.0, upper=float(max_single_weight))
            else:
                w = w.clip(lower=-float(max_single_weight), upper=float(max_single_weight))

        # group cap (long-only)
        if long_only and group_labels is not None and group_cap is not None:
            grp_g = group_labels[group_labels.index.get_level_values("date") == date].reindex(w.index)
            grp_s = grp_g.droplevel("date")
            long_w = w.clip(lower=0.0)
            long_w = _normalize_weights(long_w, long_only=True)
            long_w = _apply_group_cap_long_only(long_w, grp_s, float(group_cap))
            w = long_w

        w = _normalize_weights(w, long_only=long_only)
        return w

    out = base.groupby(level="date", group_keys=False).apply(_build)
    out.name = "target_weight"
    return out.sort_index()
