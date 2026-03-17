"""Minimal CLI for Feature Store v1 materialization."""

from __future__ import annotations

import argparse

from qsys.features.store import materialize_and_store_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize Feature Store v1 features")
    parser.add_argument("--features", nargs="+", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--head", type=int, default=5)
    args = parser.parse_args()

    df = materialize_and_store_features(
        feature_names=args.features,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
    )
    print(df.head(args.head))
    print(f"rows={len(df)} cols={list(df.columns)}")


if __name__ == "__main__":
    main()
