"""Independent buffered top-N backtest entrypoint."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qsys.rebalance.costs import calc_transaction_cost, calc_turnover
from qsys.rebalance.policies import BufferedTopNPolicyConfig, build_buffered_top_n_weights


def _rebalance_dates(dates: pd.Index, rebalance: str) -> set[pd.Timestamp]:
    dt = pd.to_datetime(pd.Index(dates).unique()).sort_values()
    if rebalance == "daily":
        return set(dt)
    if rebalance == "weekly":
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("W")).tail(1).index)
    if rebalance == "monthly":
        return set(pd.Series(dt, index=dt).groupby(dt.to_period("M")).tail(1).index)
    raise ValueError("rebalance must be one of {'daily', 'weekly', 'monthly'}")


def run_buffered_topn_backtest(
    signal_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    config: BufferedTopNPolicyConfig,
    return_col: str = "ret_1d",
) -> dict:
    """Run backtest using buffered top-N policy independent from legacy simulator."""

    if not isinstance(signal_df.index, pd.MultiIndex) or signal_df.index.names != ["date", "asset"]:
        raise ValueError("signal_df index must be MultiIndex ['date', 'asset']")
    if not isinstance(returns_df.index, pd.MultiIndex) or returns_df.index.names != ["date", "asset"]:
        raise ValueError("returns_df index must be MultiIndex ['date', 'asset']")

    for col in ["score", "rank", "is_tradable"]:
        if col not in signal_df.columns:
            raise ValueError(f"signal_df missing required column: {col}")
    if return_col not in returns_df.columns:
        raise ValueError(f"returns_df missing required column: {return_col}")

    signal = signal_df.sort_index()
    returns = returns_df.sort_index()

    dates = pd.to_datetime(signal.index.get_level_values("date").unique()).sort_values()
    rb_dates = _rebalance_dates(dates, config.rebalance)

    current_weights = pd.Series(dtype=float)
    prev_for_return = pd.Series(dtype=float)

    weight_rows: list[tuple[pd.Timestamp, object, float]] = []
    trade_frames: list[pd.DataFrame] = []
    turnover_rows: list[tuple[pd.Timestamp, float]] = []
    daily_rows: list[tuple[pd.Timestamp, float, float, float]] = []

    for i, d in enumerate(dates):
        d = pd.Timestamp(d)
        day_signal = signal.xs(d, level="date")

        turnover_value = 0.0
        if d in rb_dates:
            before = current_weights.copy()
            target, trade_log = build_buffered_top_n_weights(day_signal, current_weights, config)
            current_weights = target.copy()
            turnover_value = calc_turnover(before, current_weights)
            if len(trade_log):
                t = trade_log.copy()
                t.insert(0, "date", d)
                trade_frames.append(t)

        if i == 0:
            gross = 0.0
        else:
            day_ret = returns.xs(d, level="date")[[return_col]].iloc[:, 0]
            aligned = prev_for_return.reindex(day_ret.index).fillna(0.0)
            gross = float((aligned * day_ret).sum())

        cost = calc_transaction_cost(turnover_value, config.cost_bps)
        net = gross - cost

        turnover_rows.append((d, turnover_value))
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

    if trade_frames:
        trades_df = pd.concat(trade_frames, ignore_index=True)
    else:
        trades_df = pd.DataFrame(columns=["date", "asset", "prev_weight", "target_weight", "trade_weight", "action", "reason", "rank", "score", "is_tradable"])

    net = daily["net_return"]
    n = len(net)
    total_return = float((1.0 + net).prod() - 1.0) if n else 0.0
    ann_return = float((1.0 + total_return) ** (252.0 / n) - 1.0) if n > 0 else 0.0
    ann_vol = float(net.std(ddof=0) * np.sqrt(252.0)) if n > 0 else 0.0
    sharpe = float(ann_return / ann_vol) if ann_vol > 0 else 0.0
    cum = (1.0 + net).cumprod()
    mdd = float((cum / cum.cummax() - 1.0).min()) if n > 0 else 0.0

    summary = {
        "start_date": str(dates.min().date()) if len(dates) else None,
        "end_date": str(dates.max().date()) if len(dates) else None,
        "n_dates": int(len(dates)),
        "total_return": total_return,
        "annualized_return": ann_return,
        "annualized_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": mdd,
        "average_turnover": float(turnover_df["turnover"].mean()) if len(turnover_df) else 0.0,
        "total_cost": float(costs_df["cost"].sum()) if len(costs_df) else 0.0,
    }

    return {
        "daily_returns": daily,
        "weights": weights_df,
        "trades": trades_df,
        "turnover": turnover_df,
        "costs": costs_df,
        "summary": summary,
    }
