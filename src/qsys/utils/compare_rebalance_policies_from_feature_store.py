"""Compare strict Top-N vs buffered Top-N from feature-store input."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from qsys.rebalance import BufferedTopNPolicyConfig, build_equal_weight_benchmark, run_buffered_topn_backtest
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

        trades = result.get("trades", None)
        weights = result.get("weights", None)

        if trades is not None:
            trade_sum = summarize_trades(trades)
            n_trades = int(len(trades))
            n_buy = int(trade_sum["n_buy"].sum()) if len(trade_sum) else 0
            n_sell = int(trade_sum["n_sell"].sum()) if len(trade_sum) else 0
            holding_sum = holding_period_summary(weights) if weights is not None else {}
            avg_holding = float(holding_sum.get("average_holding_days", float("nan")))
            med_holding = float(holding_sum.get("median_holding_days", float("nan")))
        else:
            turnover = result.get("turnover")
            n_trades = int((turnover["turnover"] > 0).sum()) if turnover is not None and len(turnover) else 0
            n_buy = float("nan")
            n_sell = float("nan")
            avg_holding = float("nan")
            med_holding = float("nan")

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
                "n_trades": n_trades,
                "n_buy": n_buy,
                "n_sell": n_sell,
                "average_holding_days": avg_holding,
                "median_holding_days": med_holding,
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

    file_specs: list[tuple[str, pd.DataFrame]] = [
        ("strict_daily_returns", results["strict_top_n"]["daily_returns"]),
        ("buffered_daily_returns", results["buffered_top_n"]["daily_returns"]),
        ("strict_turnover", results["strict_top_n"]["turnover"]),
        ("buffered_turnover", results["buffered_top_n"]["turnover"]),
        ("strict_trades", results["strict_top_n"]["trades"]),
        ("buffered_trades", results["buffered_top_n"]["trades"]),
        ("strict_weights", results["strict_top_n"]["weights"]),
        ("buffered_weights", results["buffered_top_n"]["weights"]),
        ("equal_weight_daily_returns", results["equal_weight"]["daily_returns"]),
        ("equal_weight_turnover", results["equal_weight"]["turnover"]),
        ("equal_weight_weights", results["equal_weight"]["weights"]),
    ]

    for name, df in file_specs:
        path = out / f"{name}.csv"
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
    equal_weight_result = build_equal_weight_benchmark(
        returns_df=returns_df,
        rebalance=args.rebalance,
        return_col="ret_1d",
        cost_bps=args.cost_bps,
    )

    results = {
        "strict_top_n": strict_result,
        "buffered_top_n": buffered_result,
        "equal_weight": equal_weight_result,
    }
    comparison = compare_policy_results(results)

    print("=== Policy Comparison ===")
    print(comparison)
    print("\n=== Strict Config ===")
    print(strict_cfg)
    print("\n=== Buffered Config ===")
    print(buffered_cfg)
    print("\n=== Equal Weight Benchmark ===")
    print("Same-universe equal-weight benchmark using the same returns_df universe.")

    if args.output_dir:
        base_dir = Path(args.output_dir)
        target_dir = base_dir / args.run_name if args.run_name else base_dir
        saved = save_policy_comparison_outputs(target_dir, comparison, results)
        print("\n=== Saved Outputs ===")
        print(saved)


if __name__ == "__main__":
    main()
