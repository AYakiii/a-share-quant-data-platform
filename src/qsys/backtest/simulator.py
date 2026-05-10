"""Backtest simulator MVP on top of signal/feature layers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qsys.backtest.cost import compute_daily_cost, compute_turnover
from qsys.backtest.execution import align_next_day_returns, align_weights_and_returns
from qsys.backtest.metrics import summarize_metrics
from qsys.backtest.portfolio import build_top_n_portfolio


@dataclass(frozen=True)
class BacktestConfig:
    """Configuration for backtest simulation."""

    top_n: int = 20
    long_only: bool = True
    bottom_n: int | None = None
    rebalance: str = "daily"  # daily | weekly | monthly
    execution: str = "next_close"
    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0


def _rebalance_dates(dates: pd.Index, rebalance: str) -> set[pd.Timestamp]:
    dt = pd.to_datetime(pd.Index(dates).unique()).sort_values()
    if rebalance == "daily":
        return set(dt)
    if rebalance == "weekly":
        # Use period-end available date, consistent with qsys.rebalance.backtest.
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("W")).tail(1).index)
    if rebalance == "monthly":
        # Use period-end available date, consistent with qsys.rebalance.backtest.
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("M")).tail(1).index)
    raise ValueError("rebalance must be one of {'daily', 'weekly', 'monthly'}")


def _hold_weights_between_rebalances(weights: pd.Series, rebalance: str) -> pd.Series:
    if rebalance == "daily":
        return weights

    df = weights.rename("weight").reset_index()
    date_idx = pd.to_datetime(df["date"])
    rb_dates = _rebalance_dates(date_idx, rebalance)

    # keep only rebalance rows then forward fill by asset for all dates
    all_dates = sorted(date_idx.unique())
    assets = sorted(df["asset"].unique())

    rb = df[df["date"].isin(rb_dates)].pivot(index="date", columns="asset", values="weight")
    rb = rb.reindex(all_dates).ffill().fillna(0.0)
    rb = rb.reindex(columns=assets).fillna(0.0)

    out = rb.stack().rename("target_weight")
    out.index = out.index.set_names(["date", "asset"])
    return out.sort_index()


def run_backtest_from_weights(
    weights: pd.Series,
    asset_returns: pd.Series,
    *,
    config: BacktestConfig | None = None,
) -> dict[str, object]:
    """Run backtest from provided target weights and asset returns."""

    cfg = config or BacktestConfig()

    eff_weights = _hold_weights_between_rebalances(weights, cfg.rebalance)
    realized = align_next_day_returns(asset_returns, execution=cfg.execution)
    aligned = align_weights_and_returns(eff_weights, realized)

    weighted = aligned.assign(pnl=aligned["weight"] * aligned["asset_return"])
    gross = weighted.groupby(level="date")["pnl"].sum().rename("gross_return")

    turnover = compute_turnover(eff_weights)
    cost = compute_daily_cost(
        turnover,
        transaction_cost_bps=cfg.transaction_cost_bps,
        slippage_bps=cfg.slippage_bps,
    )

    net = gross.reindex(turnover.index).fillna(0.0) - cost
    net.name = "strategy_return"

    summary = summarize_metrics(net, turnover=turnover)

    execution_note = (
        "next_open requested; fallback to next_close realized returns used"
        if cfg.execution == "next_open"
        else "next_close realized returns used"
    )

    return {
        "returns": net,
        "gross_returns": gross,
        "turnover": turnover,
        "cost": cost,
        "weights": eff_weights,
        "summary": summary,
        "execution_note": execution_note,
    }


def run_backtest_from_signal(
    signal: pd.Series,
    asset_returns: pd.Series,
    *,
    config: BacktestConfig | None = None,
) -> dict[str, object]:
    """Construct portfolio from signal then run backtest."""

    cfg = config or BacktestConfig()
    weights = build_top_n_portfolio(
        signal,
        top_n=cfg.top_n,
        long_only=cfg.long_only,
        bottom_n=cfg.bottom_n,
    )
    return run_backtest_from_weights(weights, asset_returns, config=cfg)
