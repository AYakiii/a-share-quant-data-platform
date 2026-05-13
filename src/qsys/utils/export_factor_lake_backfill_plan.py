from __future__ import annotations

import argparse

from qsys.data.factor_lake.backfill_plan import export_backfill_plan_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Raw Factor Lake backfill plan CSV")
    parser.add_argument("--output-root", default="outputs/factor_lake_registry")
    args = parser.parse_args()
    out = export_backfill_plan_csv(args.output_root)
    print(out)


if __name__ == "__main__":
    main()
