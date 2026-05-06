"""Build and save market index benchmark daily return curves via AkShare."""

from __future__ import annotations

import argparse
from pathlib import Path

from qsys.rebalance.index_benchmarks import load_default_market_benchmark_curves


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build market index benchmark curves")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--sleep", type=float, default=3.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    curves = load_default_market_benchmark_curves(args.start_date, args.end_date, retries=args.retries, sleep=args.sleep)

    curves["CSI300"].to_csv(out / "csi300_daily_returns.csv", index=False)
    curves["CSI500"].to_csv(out / "csi500_daily_returns.csv", index=False)
    curves["SHANGHAI_COMPOSITE"].to_csv(out / "shanghai_composite_daily_returns.csv", index=False)


if __name__ == "__main__":
    main()
