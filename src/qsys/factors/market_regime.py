"""Deterministic market-regime candidate factor builder (Phase 17R)."""

from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = ["close"]
FORBIDDEN_COLUMNS = {"fwd_ret_5d", "fwd_ret_20d", "signal", "position"}
DEFAULT_WINDOWS: dict[str, list[int]] = {
    "return": [5, 20, 60],
    "volatility": [20, 60],
    "drawdown": [20, 60],
    "liquidity": [20, 60],
    "valuation": [60],
}


def _as_index_panel(index_panel: pd.DataFrame, date_level: str, index_level: str | None) -> tuple[pd.DataFrame, str | None]:
    if isinstance(index_panel.index, pd.MultiIndex):
        idx_name = index_level or "index"
        if list(index_panel.index.names) != [date_level, idx_name]:
            raise ValueError(f"MultiIndex must be ['{date_level}', '{idx_name}']")
        return index_panel, idx_name
    if isinstance(index_panel.index, pd.DatetimeIndex):
        return index_panel, None
    raise ValueError("index_panel must use DatetimeIndex or MultiIndex")


def _rolling_pct_rank(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window, min_periods=window).apply(lambda a: pd.Series(a).rank(pct=True).iloc[-1], raw=False)


def build_market_regime_factors(
    index_panel: pd.DataFrame,
    windows: dict[str, list[int]] | None = None,
    date_level: str = "date",
    index_level: str | None = None,
) -> pd.DataFrame:
    w = DEFAULT_WINDOWS if windows is None else windows
    frame, idx_level = _as_index_panel(index_panel, date_level=date_level, index_level=index_level)

    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = pd.DataFrame(index=frame.index)
    close = pd.to_numeric(frame["close"], errors="coerce")

    if idx_level is None:
        sh = lambda s, n: s.shift(n)
        roll = lambda s, n, fn: getattr(s.rolling(n, min_periods=n), fn)()
        roll_rank = lambda s, n: _rolling_pct_rank(s, n)
    else:
        sh = lambda s, n: s.groupby(level=idx_level, sort=False).shift(n)
        roll = lambda s, n, fn: s.groupby(level=idx_level, sort=False).transform(lambda x: getattr(x.rolling(n, min_periods=n), fn)())
        roll_rank = lambda s, n: s.groupby(level=idx_level, sort=False).transform(lambda x: _rolling_pct_rank(x, n))

    for x in w.get("return", []):
        out[f"index_ret_{x}d"] = close / sh(close, x) - 1.0
    if 20 in w.get("return", []):
        out["index_momentum_20d"] = out["index_ret_20d"]
    if 60 in w.get("return", []):
        out["index_momentum_60d"] = out["index_ret_60d"]

    daily_ret = close / sh(close, 1) - 1.0
    for x in w.get("volatility", []):
        out[f"index_realized_vol_{x}d"] = roll(daily_ret, x, "std")
    for x in w.get("drawdown", []):
        roll_max = roll(close, x, "max")
        out[f"index_max_drawdown_{x}d"] = close / roll_max - 1.0

    if "high" in frame.columns:
        high = pd.to_numeric(frame["high"], errors="coerce")
        for x in [20, 60]:
            rolling_high = roll(high, x, "max")
            out[f"index_close_to_high_{x}d"] = close / rolling_high - 1.0

    if "amount" in frame.columns:
        amount = pd.to_numeric(frame["amount"], errors="coerce")
        amt_means: dict[int, pd.Series] = {5: roll(amount, 5, "mean")}
        for x in w.get("liquidity", []):
            amt_means[x] = roll(amount, x, "mean")
            out[f"index_amount_mean_{x}d"] = amt_means[x]
        if 20 in amt_means:
            denom = amt_means[20].where(amt_means[20] > 0)
            out["index_amount_shock_5d_vs_20d"] = amt_means[5] / denom - 1.0

    if "valuation_pe_ttm" in frame.columns and 60 in w.get("valuation", []):
        pe = pd.to_numeric(frame["valuation_pe_ttm"], errors="coerce")
        out["index_pe_ttm"] = pe
        mean60 = roll(pe, 60, "mean")
        std60 = roll(pe, 60, "std")
        out["index_pe_ttm_z_60d"] = (pe - mean60) / std60
        out["index_pe_ttm_pct_rank_60d"] = roll_rank(pe, 60)

    bad = sorted(FORBIDDEN_COLUMNS.intersection(set(out.columns)))
    if bad:
        raise ValueError(f"Output contains forbidden columns: {bad}")

    return out
