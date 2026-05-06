"""Diagnostics helpers for independent buffered rebalance workflows."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _validate_weights(weights: pd.DataFrame) -> None:
    if not isinstance(weights.index, pd.MultiIndex) or weights.index.names != ["date", "asset"]:
        raise ValueError("weights index must be MultiIndex ['date', 'asset']")
    if "target_weight" not in weights.columns:
        raise ValueError("weights missing required column: target_weight")


def summarize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Summarize trade actions and turnover by date."""

    required = {"date", "action", "trade_weight", "rank"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"trades missing required columns: {sorted(missing)}")

    if trades.empty:
        return pd.DataFrame(
            columns=[
                "n_buy",
                "n_sell",
                "n_keep",
                "n_add",
                "n_trim",
                "n_total",
                "gross_turnover",
                "average_buy_rank",
                "average_sell_rank",
            ]
        ).astype(
            {
                "n_buy": int,
                "n_sell": int,
                "n_keep": int,
                "n_add": int,
                "n_trim": int,
                "n_total": int,
                "gross_turnover": float,
                "average_buy_rank": float,
                "average_sell_rank": float,
            }
        )

    t = trades.copy()
    t["date"] = pd.to_datetime(t["date"])

    out = pd.DataFrame(index=sorted(t["date"].unique()))
    out.index.name = "date"
    for action in ["buy", "sell", "keep", "add", "trim"]:
        out[f"n_{action}"] = t[t["action"] == action].groupby("date").size().reindex(out.index).fillna(0).astype(int)

    out["n_total"] = t.groupby("date").size().reindex(out.index).fillna(0).astype(int)
    out["gross_turnover"] = t.groupby("date")["trade_weight"].apply(lambda s: s.abs().sum()).reindex(out.index).fillna(0.0)
    out["average_buy_rank"] = t[t["action"] == "buy"].groupby("date")["rank"].mean().reindex(out.index)
    out["average_sell_rank"] = t[t["action"] == "sell"].groupby("date")["rank"].mean().reindex(out.index)

    return out


def holding_period_summary(weights: pd.DataFrame) -> dict[str, Any]:
    """Compute holding-period segment statistics from positive-weight holdings."""

    _validate_weights(weights)
    held = weights[weights["target_weight"] > 0].copy()

    by_asset_rows: list[dict[str, Any]] = []
    if held.empty:
        by_asset = pd.DataFrame(columns=["asset", "start_date", "end_date", "holding_days"])
        return {
            "average_holding_days": 0.0,
            "median_holding_days": 0.0,
            "max_holding_days": 0,
            "n_completed_positions": 0,
            "by_asset": by_asset,
        }

    held = held.reset_index().sort_values(["asset", "date"], kind="mergesort")
    available_dates = sorted(pd.to_datetime(weights.index.get_level_values("date").unique()))
    date_pos = {d: i for i, d in enumerate(available_dates)}

    for asset, g in held.groupby("asset", sort=True):
        dates = list(pd.to_datetime(g["date"]).sort_values())
        start = dates[0]
        prev = dates[0]
        count = 1
        for d in dates[1:]:
            if date_pos[d] == date_pos[prev] + 1:
                count += 1
            else:
                by_asset_rows.append({"asset": asset, "start_date": start, "end_date": prev, "holding_days": count})
                start = d
                count = 1
            prev = d
        by_asset_rows.append({"asset": asset, "start_date": start, "end_date": prev, "holding_days": count})

    by_asset = pd.DataFrame(by_asset_rows).sort_values(["asset", "start_date"], kind="mergesort").reset_index(drop=True)
    days = by_asset["holding_days"].astype(float)

    return {
        "average_holding_days": float(days.mean()) if len(days) else 0.0,
        "median_holding_days": float(days.median()) if len(days) else 0.0,
        "max_holding_days": int(days.max()) if len(days) else 0,
        "n_completed_positions": int(len(by_asset)),
        "by_asset": by_asset,
    }


def analyze_trade_forward_returns(
    trades: pd.DataFrame,
    returns_df: pd.DataFrame,
    horizons: tuple[int, ...] = (5, 20),
    return_col: str = "ret_1d",
) -> pd.DataFrame:
    """Analyze ex-post forward returns for buy/sell trade events."""

    if not isinstance(returns_df.index, pd.MultiIndex) or returns_df.index.names != ["date", "asset"]:
        raise ValueError("returns_df index must be MultiIndex ['date', 'asset']")
    if return_col not in returns_df.columns:
        raise ValueError(f"returns_df missing required column: {return_col}")

    req_trade_cols = {"date", "asset", "action"}
    missing = req_trade_cols.difference(trades.columns)
    if missing:
        raise ValueError(f"trades missing required columns: {sorted(missing)}")

    events = trades[trades["action"].isin(["buy", "sell"])].copy()
    if events.empty:
        return pd.DataFrame(columns=["action", "horizon", "n", "mean_forward_return", "median_forward_return"])

    events["date"] = pd.to_datetime(events["date"])
    ret = returns_df[[return_col]].copy().sort_index()

    rows = []
    for _, r in events.iterrows():
        a = r["asset"]
        d = pd.Timestamp(r["date"])
        action = r["action"]

        try:
            asset_ret = ret.xs(a, level="asset")
        except KeyError:
            continue

        fut = asset_ret[asset_ret.index > d][return_col]
        for h in horizons:
            window = fut.iloc[:h]
            if len(window) == 0:
                fwd = float("nan")
            else:
                fwd = float((1.0 + window).prod() - 1.0)
            rows.append({"action": action, "horizon": int(h), "forward_return": fwd})

    if not rows:
        return pd.DataFrame(columns=["action", "horizon", "n", "mean_forward_return", "median_forward_return"])

    raw = pd.DataFrame(rows)
    out = (
        raw.groupby(["action", "horizon"], as_index=False)
        .agg(n=("forward_return", lambda s: int(s.notna().sum())), mean_forward_return=("forward_return", "mean"), median_forward_return=("forward_return", "median"))
        .sort_values(["action", "horizon"], kind="mergesort")
        .reset_index(drop=True)
    )
    return out


def rank_migration_matrix(
    weights: pd.DataFrame,
    signal_df: pd.DataFrame,
    bins: tuple[float, ...] = (0, 50, 100, 200, float("inf")),
) -> pd.DataFrame:
    """Count rank-bucket migrations for held assets across consecutive dates."""

    _validate_weights(weights)
    if not isinstance(signal_df.index, pd.MultiIndex) or signal_df.index.names != ["date", "asset"]:
        raise ValueError("signal_df index must be MultiIndex ['date', 'asset']")
    if "rank" not in signal_df.columns:
        raise ValueError("signal_df missing required column: rank")

    held = weights[weights["target_weight"] > 0][["target_weight"]].copy()
    if held.empty:
        labels = ["1-50", "51-100", "101-200", "201+"]
        return pd.DataFrame(0, index=labels, columns=labels)

    ranks = signal_df[["rank"]].copy()
    joined = held.join(ranks, how="left").dropna(subset=["rank"]).reset_index()

    labels = ["1-50", "51-100", "101-200", "201+"]
    joined["bucket"] = pd.cut(joined["rank"], bins=list(bins), labels=labels, include_lowest=False, right=True)

    matrix = pd.DataFrame(0, index=labels, columns=labels)

    for _, g in joined.sort_values(["asset", "date"], kind="mergesort").groupby("asset", sort=True):
        g = g.reset_index(drop=True)
        for i in range(1, len(g)):
            prev_date = pd.Timestamp(g.loc[i - 1, "date"])
            cur_date = pd.Timestamp(g.loc[i, "date"])
            if cur_date <= prev_date:
                continue
            prev_b = g.loc[i - 1, "bucket"]
            cur_b = g.loc[i, "bucket"]
            if pd.isna(prev_b) or pd.isna(cur_b):
                continue
            matrix.loc[str(prev_b), str(cur_b)] += 1

    return matrix
