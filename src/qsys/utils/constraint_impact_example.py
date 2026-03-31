"""Minimal CLI demo for Constraint Impact Analysis v1."""

from __future__ import annotations

import argparse

from qsys.research.constraint_impact import compare_constraint_impact
from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare constrained vs unconstrained portfolios")
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-single-weight", type=float, default=0.1)
    parser.add_argument("--min-liquidity", type=float, default=None)
    parser.add_argument("--size-aware-scaling", action="store_true")
    parser.add_argument("--group-cap", type=float, default=None)
    args = parser.parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )

    alpha = demo_alpha_signal(features)

    if "ret_1d" not in features.columns or "fwd_ret_5d" not in features.columns:
        raise ValueError("Feature store needs ret_1d and fwd_ret_5d for constraint impact demo")

    result = compare_constraint_impact(
        alpha,
        asset_returns=features["ret_1d"],
        label_forward_return=features["fwd_ret_5d"],
        market_cap=features["market_cap"] if "market_cap" in features.columns else None,
        group_labels=features["industry"] if "industry" in features.columns else None,
        unconstrained_kwargs={},
        constrained_kwargs={
            "max_single_weight": args.max_single_weight,
            "liquidity": features["amount"] if "amount" in features.columns else None,
            "min_liquidity": args.min_liquidity,
            "market_cap": features["market_cap"] if "market_cap" in features.columns else None,
            "size_aware_scaling": args.size_aware_scaling,
            "group_labels": features["industry"] if "industry" in features.columns else None,
            "group_cap": args.group_cap,
        },
    )

    print("[summary]")
    print(result["summary"])
    print("\n[per_date tail]")
    print(result["per_date"].tail())


if __name__ == "__main__":
    main()
