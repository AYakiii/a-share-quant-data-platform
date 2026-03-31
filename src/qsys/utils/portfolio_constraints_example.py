"""Minimal CLI demo for Portfolio Constraints v1."""

from __future__ import annotations

import argparse

from qsys.backtest.portfolio import build_top_n_portfolio
from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run constrained portfolio construction demo")
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-single-weight", type=float, default=0.1)
    parser.add_argument("--min-liquidity", type=float, default=None, help="Uses amount as liquidity proxy")
    parser.add_argument("--size-aware-scaling", action="store_true")
    parser.add_argument("--group-cap", type=float, default=None)
    args = parser.parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )

    signal = demo_alpha_signal(features)
    liquidity = features["amount"] if "amount" in features.columns else None
    market_cap = features["market_cap"] if "market_cap" in features.columns else None
    group_labels = features["industry"] if "industry" in features.columns else None

    weights = build_top_n_portfolio(
        signal,
        top_n=args.top_n,
        long_only=True,
        max_single_weight=args.max_single_weight,
        liquidity=liquidity,
        min_liquidity=args.min_liquidity,
        market_cap=market_cap,
        size_aware_scaling=args.size_aware_scaling,
        group_labels=group_labels,
        group_cap=args.group_cap,
    )

    print(weights.head())
    date_sum = weights.groupby(level="date").sum().tail(3)
    print("last date sums:")
    print(date_sum)


if __name__ == "__main__":
    main()
