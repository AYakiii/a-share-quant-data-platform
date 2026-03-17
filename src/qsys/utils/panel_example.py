"""Minimal CLI example for the daily panel reader."""

from __future__ import annotations

import argparse

from qsys.data.panel.daily_panel import load_daily_panel


def main() -> None:
    parser = argparse.ArgumentParser(description="Load normalized daily panel data")
    parser.add_argument("--dataset-root", default="data/standardized/market/daily_bars")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--columns", nargs="*", default=None)
    parser.add_argument("--head", type=int, default=5)
    args = parser.parse_args()

    panel = load_daily_panel(
        dataset_root=args.dataset_root,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
        columns=args.columns,
    )
    print(panel.head(args.head))
    print(f"rows={len(panel)}")


if __name__ == "__main__":
    main()
