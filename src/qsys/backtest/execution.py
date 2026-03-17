"""Execution alignment for Backtest MVP."""

from __future__ import annotations

import warnings

import pandas as pd


def align_next_day_returns(
    asset_returns: pd.Series,
    *,
    execution: str = "next_close",
) -> pd.Series:
    """Align realized returns to signal date without look-ahead.

    For MVP we support ``next_close`` using close-to-close returns.
    ``next_open`` is accepted but falls back to ``next_close`` if open-based
    return inputs are unavailable.
    """

    if not isinstance(asset_returns.index, pd.MultiIndex) or asset_returns.index.names != ["date", "asset"]:
        raise ValueError("asset_returns must be MultiIndex [date, asset]")

    if execution not in {"next_close", "next_open"}:
        raise ValueError("execution must be one of {'next_close', 'next_open'}")

    if execution == "next_open":
        warnings.warn(
            "execution='next_open' requested but open-return series is unavailable; falling back to next_close",
            RuntimeWarning,
            stacklevel=2,
        )

    # MVP fallback: both modes map to next close return unless open-return data exists.
    shifted = asset_returns.sort_index().groupby(level="asset").shift(-1)
    shifted.name = "realized_return"
    return shifted


def align_weights_and_returns(weights: pd.Series, realized_returns: pd.Series) -> pd.DataFrame:
    """Align target weights with realized returns on the same signal date index."""

    if not isinstance(weights.index, pd.MultiIndex) or weights.index.names != ["date", "asset"]:
        raise ValueError("weights must be MultiIndex [date, asset]")
    if not isinstance(realized_returns.index, pd.MultiIndex) or realized_returns.index.names != ["date", "asset"]:
        raise ValueError("realized_returns must be MultiIndex [date, asset]")

    df = pd.concat([weights.rename("weight"), realized_returns.rename("asset_return")], axis=1)
    return df.dropna(subset=["weight", "asset_return"]).sort_index()
