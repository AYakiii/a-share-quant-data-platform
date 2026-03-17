"""Minimal CLI demo for exposure analysis diagnostics."""

from __future__ import annotations

import argparse

from qsys.research.exposure import exposure_summary
from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run exposure diagnostics on demo alpha")
    parser.add_argument("--feature-root", default="data/processed/feature_store/v1")
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

    feat_cols = [c for c in ["ret_20d", "vol_20d", "ret_5d"] if c in features.columns]
    feat_df = features[feat_cols] if feat_cols else None
    mc = features["market_cap"] if "market_cap" in features.columns else None
    grp = features["industry"] if "industry" in features.columns else None

    result = exposure_summary(alpha, market_cap=mc, group_labels=grp, features=feat_df)

    for k, v in result.items():
        print(f"\n[{k}]")
        print(v.head() if hasattr(v, "head") else v)


if __name__ == "__main__":
    main()
