"""Compare strict Top-N vs buffered Top-N from feature-store input."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from qsys.rebalance import BufferedTopNPolicyConfig, run_buffered_topn_backtest
from qsys.rebalance.diagnostics import holding_period_summary, summarize_trades
from qsys.signals.engine import load_feature_store_frame
from qsys.utils.run_buffered_rebalance_from_feature_store import build_demo_signal_and_returns


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for policy comparison utility."""

    p = argparse.ArgumentParser(description="Compare strict vs buffered rebalance from feature store")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--target-n", type=int, default=50)
    p.add_argument("--buy-rank", type=int, default=None)
    p.add_argument("--sell-rank", type=int, default=None)
    p.add_argument("--min-holding-n", type=int, default=None)
    p.add_argument("--max-holding-n", type=int, default=None)
    p.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="weekly")
    p.add_argument("--min-trade-weight", type=float, default=0.003)
    p.add_argument("--max-single-weight", type=float, default=0.025)
    p.add_argument("--cost-bps", type=float, default=20.0)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--run-name", default=None)
    return p.parse_args()


def build_policy_configs(args: argparse.Namespace) -> tuple[BufferedTopNPolicyConfig, BufferedTopNPolicyConfig]:
    """Build strict and buffered policy configs from CLI args."""

    target_n = int(args.target_n)

    strict = BufferedTopNPolicyConfig(
        target_n=target_n,
        buy_rank=target_n,
        sell_rank=target_n,
        min_holding_n=target_n,
        max_holding_n=target_n,
        rebalance=args.rebalance,
        min_trade_weight=0.0,
        max_single_weight=float(args.max_single_weight),
        cost_bps=float(args.cost_bps),
    )

    buy_rank = int(args.buy_rank) if args.buy_rank is not None else target_n
    sell_rank = int(args.sell_rank) if args.sell_rank is not None else int(2 * target_n)
    min_holding_n = int(args.min_holding_n) if args.min_holding_n is not None else int(0.9 * target_n)
    max_holding_n = int(args.max_holding_n) if args.max_holding_n is not None else int(1.2 * target_n)

    buffered = BufferedTopNPolicyConfig(
        target_n=target_n,
        buy_rank=buy_rank,
        sell_rank=sell_rank,
        min_holding_n=min_holding_n,
        max_holding_n=max_holding_n,
        rebalance=args.rebalance,
        min_trade_weight=float(args.min_trade_weight),
        max_single_weight=float(args.max_single_weight),
        cost_bps=float(args.cost_bps),
    )

    return strict, buffered


def compare_policy_results(results: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Build compact policy comparison table from backtest results."""

    rows: list[dict[str, Any]] = []
    for policy, result in results.items():
        summary = result["summary"]
        trades = result["trades"]
        trade_sum = summarize_trades(trades)
        holding_sum = holding_period_summary(result["weights"])

        n_buy = int(trade_sum["n_buy"].sum()) if len(trade_sum) else 0
        n_sell = int(trade_sum["n_sell"].sum()) if len(trade_sum) else 0

        rows.append(
            {
                "policy": policy,
                "total_return": summary.get("total_return", 0.0),
                "annualized_return": summary.get("annualized_return", 0.0),
                "annualized_vol": summary.get("annualized_vol", 0.0),
                "sharpe": summary.get("sharpe", 0.0),
                "max_drawdown": summary.get("max_drawdown", 0.0),
                "average_turnover": summary.get("average_turnover", 0.0),
                "total_cost": summary.get("total_cost", 0.0),
                "n_trades": int(len(trades)),
                "n_buy": n_buy,
                "n_sell": n_sell,
                "average_holding_days": float(holding_sum.get("average_holding_days", 0.0)),
                "median_holding_days": float(holding_sum.get("median_holding_days", 0.0)),
            }
        )

    return pd.DataFrame(rows).sort_values("policy", kind="mergesort").reset_index(drop=True)


def save_policy_comparison_outputs(
    output_dir: str | Path,
    comparison: pd.DataFrame,
    results: dict[str, dict],
) -> dict[str, Path]:
    """Save comparison and per-policy outputs to CSV and return file mapping."""

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved: dict[str, Path] = {}

    cmp_path = out / "comparison.csv"
    comparison.to_csv(cmp_path, index=False)
    saved["comparison"] = cmp_path

    strict = results["strict_top_n"]
    buffered = results["buffered_top_n"]

    file_specs = [
        ("strict_daily_returns", strict["daily_returns"]),
        ("buffered_daily_returns", buffered["daily_returns"]),
        ("strict_turnover", strict["turnover"]),
        ("buffered_turnover", buffered["turnover"]),
        ("strict_trades", strict["trades"]),
        ("buffered_trades", buffered["trades"]),
        ("strict_weights", strict["weights"]),
        ("buffered_weights", buffered["weights"]),
    ]

    for name, df in file_specs:
        path = out / f"{name}.csv"
        if isinstance(df.index, pd.MultiIndex):
            df.reset_index().to_csv(path, index=False)
        else:
            df.reset_index().to_csv(path, index=False)
        saved[name] = path

    return saved


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

    strict_cfg, buffered_cfg = build_policy_configs(args)

    strict_result = run_buffered_topn_backtest(signal_df, returns_df, strict_cfg)
    buffered_result = run_buffered_topn_backtest(signal_df, returns_df, buffered_cfg)

    results = {"strict_top_n": strict_result, "buffered_top_n": buffered_result}
    comparison = compare_policy_results(results)

    print("=== Policy Comparison ===")
    print(comparison)
    print("\n=== Strict Config ===")
    print(strict_cfg)
    print("\n=== Buffered Config ===")
    print(buffered_cfg)

    if args.output_dir:
        base_dir = Path(args.output_dir)
        target_dir = base_dir / args.run_name if args.run_name else base_dir
        saved = save_policy_comparison_outputs(target_dir, comparison, results)
        print("\n=== Saved Outputs ===")
        print(saved)


if __name__ == "__main__":
    main()
