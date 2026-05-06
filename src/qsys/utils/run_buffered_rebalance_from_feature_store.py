"""Run buffered rebalance backtest from an existing feature store frame.

This utility loads feature-store data, builds a simple demo signal,
runs the independent buffered rebalance backtest, and prints diagnostics.
"""

from __future__ import annotations

import argparse

import pandas as pd

from qsys.rebalance import BufferedTopNPolicyConfig, run_buffered_topn_backtest
from qsys.rebalance.diagnostics import (
    analyze_trade_forward_returns,
    holding_period_summary,
    rank_migration_matrix,
    summarize_trades,
)
from qsys.signals.engine import load_feature_store_frame


def build_demo_signal_and_returns(feature_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build demo signal_df and returns_df from feature-store frame."""

    if not isinstance(feature_df.index, pd.MultiIndex) or feature_df.index.names != ["date", "asset"]:
        raise ValueError("feature_df must be MultiIndex ['date', 'asset']")

    required_signal_cols = ["ret_20d", "vol_20d"]
    missing = [c for c in required_signal_cols if c not in feature_df.columns]
    if missing:
        raise ValueError(f"feature_df missing required signal columns: {missing}")
    if "ret_1d" not in feature_df.columns:
        raise ValueError("feature_df missing required returns column: ret_1d")

    frame = feature_df.copy()

    signal_base = frame.dropna(subset=["ret_20d", "vol_20d"])[["ret_20d", "vol_20d"]].copy()
    rank_pct = signal_base.groupby(level="date")["ret_20d"].rank(pct=True)
    vol_mean = signal_base.groupby(level="date")["vol_20d"].transform("mean")
    vol_std = signal_base.groupby(level="date")["vol_20d"].transform("std").replace(0.0, pd.NA)
    vol_z = ((signal_base["vol_20d"] - vol_mean) / vol_std).fillna(0.0)

    score = rank_pct - 0.5 * vol_z
    signal_df = pd.DataFrame(index=signal_base.index)
    signal_df["score"] = score.astype(float)
    signal_df["rank"] = signal_df.groupby(level="date")["score"].rank(method="first", ascending=False).astype(int)

    if "is_tradable" in frame.columns:
        signal_df["is_tradable"] = frame["is_tradable"].reindex(signal_df.index).fillna(True).astype(bool)
    else:
        signal_df["is_tradable"] = True

    returns_df = frame.dropna(subset=["ret_1d"])[["ret_1d"]].copy()
    signal_df = signal_df.sort_index()
    returns_df = returns_df.sort_index()

    return signal_df, returns_df


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    p = argparse.ArgumentParser(description="Run buffered rebalance backtest from feature store")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--target-n", type=int, default=50)
    p.add_argument("--buy-rank", type=int, default=50)
    p.add_argument("--sell-rank", type=int, default=100)
    p.add_argument("--min-holding-n", type=int, default=45)
    p.add_argument("--max-holding-n", type=int, default=60)
    p.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="weekly")
    p.add_argument("--min-trade-weight", type=float, default=0.003)
    p.add_argument("--max-single-weight", type=float, default=0.025)
    p.add_argument("--cost-bps", type=float, default=20.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    signal_df, returns_df = build_demo_signal_and_returns(features)

    aligned_idx = signal_df.index.intersection(returns_df.index)
    signal_df = signal_df.reindex(aligned_idx).sort_index()
    returns_df = returns_df.reindex(aligned_idx).sort_index()

    cfg = BufferedTopNPolicyConfig(
        target_n=args.target_n,
        buy_rank=args.buy_rank,
        sell_rank=args.sell_rank,
        min_holding_n=args.min_holding_n,
        max_holding_n=args.max_holding_n,
        rebalance=args.rebalance,
        min_trade_weight=args.min_trade_weight,
        max_single_weight=args.max_single_weight,
        cost_bps=args.cost_bps,
    )

    result = run_buffered_topn_backtest(signal_df, returns_df, cfg)

    trade_summary = summarize_trades(result["trades"])
    holding_summary = holding_period_summary(result["weights"])
    forward_diag = analyze_trade_forward_returns(result["trades"], returns_df, horizons=(5, 20), return_col="ret_1d")
    migration = rank_migration_matrix(result["weights"], signal_df)

    print("=== Config ===")
    print(cfg)
    print("\n=== Backtest Summary ===")
    print(result["summary"])
    print("\n=== Daily Returns Tail ===")
    print(result["daily_returns"].tail())
    print("\n=== Trade Summary Head ===")
    print(trade_summary.head())
    print("\n=== Holding Period Summary ===")
    compact_holding = {k: v for k, v in holding_summary.items() if k != "by_asset"}
    print(compact_holding)
    print("\n=== Trade Forward Returns ===")
    print(forward_diag)
    print("\n=== Rank Migration Matrix ===")
    print(migration)


if __name__ == "__main__":
    main()
