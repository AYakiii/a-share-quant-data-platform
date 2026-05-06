"""Turnover and transaction cost helpers for rebalance policies."""

from __future__ import annotations

import pandas as pd


def calc_turnover(prev_weights: pd.Series, target_weights: pd.Series) -> float:
    """Calculate full absolute turnover between previous and target weights."""

    all_assets = prev_weights.index.union(target_weights.index)
    prev = prev_weights.reindex(all_assets).fillna(0.0).astype(float)
    target = target_weights.reindex(all_assets).fillna(0.0).astype(float)
    return float((target - prev).abs().sum())


def calc_transaction_cost(turnover: float, cost_bps: float) -> float:
    """Calculate transaction cost from turnover and one-way basis points."""

    if cost_bps < 0:
        raise ValueError("cost_bps must be non-negative")
    return float(turnover) * float(cost_bps) / 10000.0
