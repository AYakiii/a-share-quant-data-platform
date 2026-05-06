"""Benchmark builders for rebalance experiments."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _rebalance_dates(dates: pd.Index, rebalance: str) -> set[pd.Timestamp]:
    dt = pd.to_datetime(pd.Index(dates).unique()).sort_values()
    if rebalance == "daily":
        return set(dt)
    if rebalance == "weekly":
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("W")).tail(1).index)
    if rebalance == "monthly":
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("M")).tail(1).index)
    raise ValueError("rebalance must be one of {'daily', 'weekly', 'monthly'}")


def build_equal_weight_benchmark(
    returns_df: pd.DataFrame,
    rebalance: str = "weekly",
    return_col: str = "ret_1d",
    cost_bps: float = 0.0,
) -> dict:
    """Build equal-weight benchmark over available universe with no-lookahead accounting."""

    if not isinstance(returns_df.index, pd.MultiIndex) or returns_df.index.names != ["date", "asset"]:
        raise ValueError("returns_df index must be MultiIndex ['date', 'asset']")
    if return_col not in returns_df.columns:
        raise ValueError(f"returns_df missing required column: {return_col}")
    if cost_bps < 0:
        raise ValueError("cost_bps must be non-negative")

    ret = returns_df[[return_col]].copy().sort_index()
    dates = pd.to_datetime(ret.index.get_level_values("date").unique()).sort_values()
    rb_dates = _rebalance_dates(dates, rebalance)

    current_weights = pd.Series(dtype=float)
    prev_for_return = pd.Series(dtype=float)

    weight_rows: list[tuple[pd.Timestamp, object, float]] = []
    turnover_rows: list[tuple[pd.Timestamp, float]] = []
    daily_rows: list[tuple[pd.Timestamp, float, float, float]] = []

    for i, d in enumerate(dates):
        d = pd.Timestamp(d)
        day_ret = ret.xs(d, level="date")[return_col]

        turnover = 0.0
        if d in rb_dates:
            universe = pd.Index(day_ret.index)
            if len(universe) > 0:
                target = pd.Series(1.0 / len(universe), index=universe, dtype=float)
            else:
                target = pd.Series(dtype=float)

            all_assets = current_weights.index.union(target.index)
            prev_al = current_weights.reindex(all_assets).fillna(0.0)
            tgt_al = target.reindex(all_assets).fillna(0.0)
            turnover = float((tgt_al - prev_al).abs().sum())
            current_weights = target.copy()

        if i == 0:
            gross = 0.0
        else:
            aligned = prev_for_return.reindex(day_ret.index).fillna(0.0)
            day_ret_filled = day_ret.fillna(0.0)
            gross = float((aligned * day_ret_filled).sum())

        cost = float(turnover * float(cost_bps) / 10000.0)
        net = gross - cost

        turnover_rows.append((d, turnover))
        daily_rows.append((d, gross, cost, net))

        for a, w in current_weights[current_weights > 0].items():
            weight_rows.append((d, a, float(w)))

        prev_for_return = current_weights.copy()

    daily = pd.DataFrame(daily_rows, columns=["date", "gross_return", "cost", "net_return"]).set_index("date")
    daily["cumulative_net_return"] = (1.0 + daily["net_return"]).cumprod() - 1.0

    turnover_df = pd.DataFrame(turnover_rows, columns=["date", "turnover"]).set_index("date")
    costs_df = daily[["cost"]].copy()

    if weight_rows:
        weights_df = pd.DataFrame(weight_rows, columns=["date", "asset", "target_weight"]).set_index(["date", "asset"]).sort_index()
    else:
        weights_df = pd.DataFrame(columns=["target_weight"], index=pd.MultiIndex.from_arrays([[], []], names=["date", "asset"]))

    summary = summarize_benchmark_result(
        {
            "daily_returns": daily,
            "turnover": turnover_df,
            "costs": costs_df,
        }
    )
    summary["start_date"] = str(dates.min().date()) if len(dates) else None
    summary["end_date"] = str(dates.max().date()) if len(dates) else None
    summary["n_dates"] = int(len(dates))

    return {
        "daily_returns": daily,
        "weights": weights_df,
        "turnover": turnover_df,
        "costs": costs_df,
        "summary": summary,
    }


def summarize_benchmark_result(result: dict) -> dict:
    """Summarize benchmark returns/costs/turnover into compact metrics."""

    daily = result["daily_returns"]
    turnover = result["turnover"]
    costs = result["costs"]

    net = daily["net_return"] if len(daily) else pd.Series(dtype=float)
    n = len(net)
    total_return = float((1.0 + net).prod() - 1.0) if n else 0.0
    ann_return = float((1.0 + total_return) ** (252.0 / n) - 1.0) if n > 0 else 0.0
    ann_vol = float(net.std(ddof=0) * np.sqrt(252.0)) if n > 0 else 0.0
    sharpe = float(ann_return / ann_vol) if ann_vol > 0 else 0.0
    cum = (1.0 + net).cumprod()
    max_dd = float((cum / cum.cummax() - 1.0).min()) if n > 0 else 0.0

    return {
        "total_return": total_return,
        "annualized_return": ann_return,
        "annualized_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "average_turnover": float(turnover["turnover"].mean()) if len(turnover) else 0.0,
        "total_cost": float(costs["cost"].sum()) if len(costs) else 0.0,
    }
