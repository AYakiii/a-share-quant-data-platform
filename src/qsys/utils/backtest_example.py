"""End-to-end Backtest MVP example on top of feature/signal layers."""

from __future__ import annotations

import argparse

from qsys.backtest.simulator import BacktestConfig, run_backtest_from_signal
from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Backtest Engine MVP demo")
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="daily")
    parser.add_argument("--transaction-cost-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    args = parser.parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )

    signal = demo_alpha_signal(features)

    if "ret_1d" not in features.columns:
        raise ValueError("Feature store frame must include ret_1d for backtest MVP")

    result = run_backtest_from_signal(
        signal,
        features["ret_1d"],
        config=BacktestConfig(
            top_n=args.top_n,
            long_only=True,
            rebalance=args.rebalance,
            execution="next_close",
            transaction_cost_bps=args.transaction_cost_bps,
            slippage_bps=args.slippage_bps,
        ),
    )

    print("Backtest summary:")
    for k, v in result["summary"].items():
        print(f"  {k}: {v:.6f}")


if __name__ == "__main__":
    main()
