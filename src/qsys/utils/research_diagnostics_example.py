"""Minimal CLI demo for Research Diagnostics v1."""

from __future__ import annotations

import argparse

from qsys.research.ic import daily_ic, daily_rank_ic, ic_summary
from qsys.research.quantiles import quantile_spread
from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run research diagnostics on demo alpha")
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    args = parser.parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )

    alpha = demo_alpha_signal(features)

    if "fwd_ret_5d" not in features.columns:
        raise ValueError("Feature store frame must include fwd_ret_5d for diagnostics demo")

    label = features["fwd_ret_5d"]
    daily_ic_series = daily_ic(alpha, label)
    daily_rank_ic_series = daily_rank_ic(alpha, label)
    spread = quantile_spread(alpha, label, n_quantiles=5)
    summary = ic_summary(alpha, label)

    print("IC summary:")
    for k, v in summary.items():
        print(f"  {k}: {v:.6f}" if isinstance(v, float) else f"  {k}: {v}")

    print("latest daily_ic:", daily_ic_series.dropna().tail(1).to_dict())
    print("latest daily_rank_ic:", daily_rank_ic_series.dropna().tail(1).to_dict())
    print("latest quantile_spread:", spread.dropna().tail(1).to_dict())


if __name__ == "__main__":
    main()
