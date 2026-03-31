"""Minimal CLI for Signal Engine v1 demo alpha generation."""

from __future__ import annotations

import argparse

from qsys.signals.engine import demo_alpha_signal, load_feature_store_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Signal Engine v1 demo alpha")
    parser.add_argument("--feature-root", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--head", type=int, default=5)
    args = parser.parse_args()

    features = load_feature_store_frame(
        feature_root=args.feature_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )
    alpha = demo_alpha_signal(features)
    print(alpha.head(args.head))
    print(f"rows={len(alpha)}")


if __name__ == "__main__":
    main()
