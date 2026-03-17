"""Constraint Impact Analysis v1."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qsys.backtest.portfolio import build_top_n_portfolio
from qsys.backtest.simulator import BacktestConfig, run_backtest_from_weights
from qsys.research.exposure import size_exposure_daily
from qsys.research.ic import daily_ic


@dataclass(frozen=True)
class ConstraintImpactConfig:
    """Configuration for constraint impact comparison."""

    top_n: int = 20
    long_only: bool = True
    bottom_n: int | None = None
    rebalance: str = "daily"
    execution: str = "next_close"
    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0


def compare_constraint_impact(
    signal: pd.Series,
    asset_returns: pd.Series,
    label_forward_return: pd.Series,
    *,
    market_cap: pd.Series | None = None,
    group_labels: pd.Series | None = None,
    unconstrained_kwargs: dict | None = None,
    constrained_kwargs: dict | None = None,
    config: ConstraintImpactConfig | None = None,
) -> dict[str, pd.DataFrame]:
    """Compare unconstrained vs constrained portfolio outcomes.

    Returns a dict with:
    - summary: one-row DataFrame of key metric differences
    - per_date: per-date comparison DataFrame
    """

    cfg = config or ConstraintImpactConfig()
    base_kwargs = {
        "top_n": cfg.top_n,
        "long_only": cfg.long_only,
        "bottom_n": cfg.bottom_n,
    }

    uncon_kwargs = {**base_kwargs, **(unconstrained_kwargs or {})}
    cons_kwargs = {**base_kwargs, **(constrained_kwargs or {})}

    w_uncon = build_top_n_portfolio(signal, **uncon_kwargs)
    w_con = build_top_n_portfolio(signal, **cons_kwargs)

    bt_cfg = BacktestConfig(
        top_n=cfg.top_n,
        long_only=cfg.long_only,
        bottom_n=cfg.bottom_n,
        rebalance=cfg.rebalance,
        execution=cfg.execution,
        transaction_cost_bps=cfg.transaction_cost_bps,
        slippage_bps=cfg.slippage_bps,
    )

    r_uncon = run_backtest_from_weights(w_uncon, asset_returns, config=bt_cfg)
    r_con = run_backtest_from_weights(w_con, asset_returns, config=bt_cfg)

    ic_uncon = daily_ic(w_uncon, label_forward_return)
    ic_con = daily_ic(w_con, label_forward_return)

    size_uncon_mean = float("nan")
    size_con_mean = float("nan")
    if market_cap is not None:
        size_uncon_mean = float(size_exposure_daily(w_uncon, market_cap).mean())
        size_con_mean = float(size_exposure_daily(w_con, market_cap).mean())

    group_uncon_mean = float("nan")
    group_con_mean = float("nan")
    if group_labels is not None:
        # group exposure proxy: average group concentration (mean abs group weights)
        wu = pd.concat([w_uncon.rename("w"), group_labels.rename("g")], axis=1).dropna()
        wc = pd.concat([w_con.rename("w"), group_labels.rename("g")], axis=1).dropna()
        if len(wu):
            group_uncon_mean = float(
                wu.groupby([wu.index.get_level_values("date"), wu["g"]])["w"].sum().abs().groupby(level=0).mean().mean()
            )
        if len(wc):
            group_con_mean = float(
                wc.groupby([wc.index.get_level_values("date"), wc["g"]])["w"].sum().abs().groupby(level=0).mean().mean()
            )

    summary = pd.DataFrame(
        [
            {
                "return_diff": float(r_con["summary"]["cumulative_return"] - r_uncon["summary"]["cumulative_return"]),
                "sharpe_diff": float(r_con["summary"]["sharpe"] - r_uncon["summary"]["sharpe"]),
                "turnover_diff": float(r_con["summary"]["turnover"] - r_uncon["summary"]["turnover"]),
                "ic_diff": float(ic_con.mean() - ic_uncon.mean()),
                "size_exposure_diff": float(size_con_mean - size_uncon_mean),
                "group_exposure_diff": float(group_con_mean - group_uncon_mean),
            }
        ]
    )

    per_date = pd.concat(
        [
            r_uncon["returns"].rename("unconstrained_return"),
            r_con["returns"].rename("constrained_return"),
            r_uncon["turnover"].rename("unconstrained_turnover"),
            r_con["turnover"].rename("constrained_turnover"),
            ic_uncon.rename("unconstrained_ic"),
            ic_con.rename("constrained_ic"),
        ],
        axis=1,
    ).sort_index()
    per_date["return_diff"] = per_date["constrained_return"] - per_date["unconstrained_return"]
    per_date["turnover_diff"] = per_date["constrained_turnover"] - per_date["unconstrained_turnover"]
    per_date["ic_diff"] = per_date["constrained_ic"] - per_date["unconstrained_ic"]

    return {
        "summary": summary,
        "per_date": per_date,
    }
