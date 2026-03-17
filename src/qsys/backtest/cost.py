"""Transaction cost model for Backtest MVP."""

from __future__ import annotations

import pandas as pd


def compute_turnover(weights: pd.Series) -> pd.Series:
    """Compute per-date turnover from target weights.

    Turnover is defined as ``sum(abs(w_t - w_{t-1}))`` across assets.
    """

    if not isinstance(weights.index, pd.MultiIndex) or weights.index.names != ["date", "asset"]:
        raise ValueError("weights must be MultiIndex [date, asset]")

    w = weights.rename("weight").reset_index().pivot(index="date", columns="asset", values="weight").fillna(0.0)
    tw = w.diff().abs().sum(axis=1)
    tw.iloc[0] = w.iloc[0].abs().sum()
    tw.name = "turnover"
    return tw.sort_index()


def compute_cost_rate_bps(*, transaction_cost_bps: float = 10.0, slippage_bps: float = 5.0) -> float:
    """Return total one-way cost rate from bps to decimal."""

    return (float(transaction_cost_bps) + float(slippage_bps)) / 10000.0


def compute_daily_cost(
    turnover: pd.Series,
    *,
    transaction_cost_bps: float = 10.0,
    slippage_bps: float = 5.0,
) -> pd.Series:
    """Compute daily trading cost as turnover times total bps rate."""

    rate = compute_cost_rate_bps(transaction_cost_bps=transaction_cost_bps, slippage_bps=slippage_bps)
    out = turnover.astype(float) * rate
    out.name = "cost"
    return out
