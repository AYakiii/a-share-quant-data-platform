"""Multi-horizon technical and liquidity candidate factor builder."""

from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = ["close", "high", "low", "amount", "turnover"]
FORBIDDEN_OUTPUT_COLUMNS = {
    "fwd_ret_5d",
    "fwd_ret_20d",
    "解禁后20日涨跌幅",
    "上榜后1日",
    "上榜后2日",
    "上榜后5日",
    "上榜后10日",
}

DEFAULT_WINDOWS: dict[str, list[int]] = {
    "return": [5, 20, 60],
    "volatility": [20, 60],
    "liquidity": [5, 20, 60],
    "drawdown": [20, 60],
    "range": [20, 60],
}


def _ensure_panel_index(panel: pd.DataFrame, date_level: str, asset_level: str) -> pd.DataFrame:
    if isinstance(panel.index, pd.MultiIndex) and list(panel.index.names) == [date_level, asset_level]:
        return panel

    if date_level in panel.columns and asset_level in panel.columns:
        out = panel.set_index([date_level, asset_level])
        return out

    raise ValueError(
        f"panel must be MultiIndex ['{date_level}', '{asset_level}'] or contain these columns"
    )


def build_technical_liquidity_factors(
    panel: pd.DataFrame,
    windows: dict[str, list[int]] | None = None,
    date_level: str = "date",
    asset_level: str = "asset",
) -> pd.DataFrame:
    """Build deterministic multi-horizon technical/liquidity factor columns."""

    w = DEFAULT_WINDOWS if windows is None else windows
    frame = _ensure_panel_index(panel, date_level=date_level, asset_level=asset_level)

    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    g = frame.groupby(level=asset_level, sort=False)
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    amount = pd.to_numeric(frame["amount"], errors="coerce")
    turnover = pd.to_numeric(frame["turnover"], errors="coerce")

    factors = pd.DataFrame(index=frame.index)

    # Return / trend
    for x in w.get("return", []):
        factors[f"ret_{x}d"] = close / g["close"].shift(x) - 1.0

    if 20 in w.get("return", []):
        factors["momentum_20d"] = factors["ret_20d"]
        factors["reversal_20d"] = -factors["ret_20d"]
    if 60 in w.get("return", []):
        factors["momentum_60d"] = factors["ret_60d"]
    if 5 in w.get("return", []):
        factors["reversal_5d"] = -factors["ret_5d"]

    daily_ret = close / g["close"].shift(1) - 1.0

    # Risk
    for x in w.get("volatility", []):
        factors[f"realized_vol_{x}d"] = daily_ret.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).std()
        )

        downside = daily_ret.where(daily_ret < 0)
        factors[f"downside_vol_{x}d"] = downside.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).std()
        )

    for x in w.get("drawdown", []):
        roll_max = close.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).max()
        )
        factors[f"max_drawdown_{x}d"] = close / roll_max - 1.0

    # Liquidity means
    amount_means: dict[int, pd.Series] = {}
    turnover_means: dict[int, pd.Series] = {}
    for x in w.get("liquidity", []):
        amount_mean = amount.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).mean()
        )
        turnover_mean = turnover.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).mean()
        )
        amount_means[x] = amount_mean
        turnover_means[x] = turnover_mean
        if x in {20, 60}:
            factors[f"amount_mean_{x}d"] = amount_mean
            factors[f"turnover_mean_{x}d"] = turnover_mean

    if 5 in amount_means and 20 in amount_means:
        factors["amount_shock_5d_vs_20d"] = amount_means[5] / amount_means[20] - 1.0
    if 5 in turnover_means and 20 in turnover_means:
        factors["turnover_shock_5d_vs_20d"] = turnover_means[5] / turnover_means[20] - 1.0

    positive_amount = amount.where(amount > 0)
    amihud_base = daily_ret.abs() / positive_amount
    for x in [20, 60]:
        if x in w.get("liquidity", []):
            factors[f"amihud_illiquidity_{x}d"] = amihud_base.groupby(level=asset_level, sort=False).transform(
                lambda s: s.rolling(x, min_periods=x).mean()
            )

    # Range / position
    for x in w.get("range", []):
        rolling_high = high.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).max()
        )
        rolling_low = low.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).min()
        )
        factors[f"high_low_range_{x}d"] = rolling_high / rolling_low - 1.0
        factors[f"close_to_high_{x}d"] = close / rolling_high - 1.0

    bad_cols = sorted(FORBIDDEN_OUTPUT_COLUMNS.intersection(set(factors.columns)))
    if bad_cols:
        raise ValueError(f"Output contains forbidden columns: {bad_cols}")

    return factors
