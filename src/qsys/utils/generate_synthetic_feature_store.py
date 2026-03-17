from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate_synthetic_feature_frame(
    start_date: str = "2024-01-01",
    periods: int = 40,
    n_assets: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate a small synthetic feature-store frame for smoke testing.

    Output columns:
    - date
    - asset
    - ret_20d
    - vol_20d
    - fwd_ret_5d
    - fwd_ret_20d
    - market_cap
    - amount_20d
    """

    rng = np.random.default_rng(seed)
    dates = pd.date_range(start_date, periods=periods, freq="B")
    assets = [f"{i:06d}.SZ" for i in range(1, n_assets + 1)]

    rows: list[dict] = []

    for d in dates:
        for i, asset in enumerate(assets):
            ret_20d = rng.normal(loc=0.02 * i, scale=0.15)
            vol_20d = abs(rng.normal(loc=1.0, scale=0.2))
            fwd_ret_5d = 0.03 * ret_20d - 0.01 * vol_20d + rng.normal(0, 0.01)
            fwd_ret_20d = 0.05 * ret_20d - 0.015 * vol_20d + rng.normal(0, 0.02)
            market_cap = 1e9 * (i + 1)
            amount_20d = rng.uniform(1e7, 5e7)

            rows.append(
                {
                    "date": d,
                    "asset": asset,
                    "ret_20d": ret_20d,
                    "vol_20d": vol_20d,
                    "fwd_ret_5d": fwd_ret_5d,
                    "fwd_ret_20d": fwd_ret_20d,
                    "market_cap": market_cap,
                    "amount_20d": amount_20d,
                }
            )

    df = pd.DataFrame(rows).sort_values(["date", "asset"]).reset_index(drop=True)
    return df


def write_feature_store_partitions(
    df: pd.DataFrame,
    feature_root: str | Path = "data/processed/feature_store/v1",
) -> Path:
    """Write synthetic features into partitioned parquet layout.

    Expected output layout:
    data/processed/feature_store/v1/
      trade_date=YYYY-MM-DD/data.parquet
    """

    root = Path(feature_root)
    root.mkdir(parents=True, exist_ok=True)

    for date, group in df.groupby("date"):
        part_dir = root / f"trade_date={pd.Timestamp(date).date()}"
        part_dir.mkdir(parents=True, exist_ok=True)
        group.to_parquet(part_dir / "data.parquet", index=False)

    return root


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic feature store for smoke testing."
    )
    parser.add_argument(
        "--feature-root",
        default="data/processed/feature_store/v1",
        help="Output feature store root.",
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Synthetic start date.",
    )
    parser.add_argument(
        "--periods",
        type=int,
        default=40,
        help="Number of business dates to generate.",
    )
    parser.add_argument(
        "--n-assets",
        type=int,
        default=5,
        help="Number of assets to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed.",
    )
    args = parser.parse_args()

    df = generate_synthetic_feature_frame(
        start_date=args.start_date,
        periods=args.periods,
        n_assets=args.n_assets,
        seed=args.seed,
    )
    root = write_feature_store_partitions(df, feature_root=args.feature_root)

    print("Synthetic feature store written.")
    print(f"feature_root: {root}")
    print(f"rows: {len(df)}")
    print(f"date range: {df['date'].min()} -> {df['date'].max()}")
    print(f"assets: {df['asset'].nunique()}")
    print("columns:", list(df.columns))


if __name__ == "__main__":
    main()