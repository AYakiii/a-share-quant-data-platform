from __future__ import annotations

import argparse

from qsys.data.factor_lake.registry import export_registry_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Factor Lake source capability registry to CSV")
    parser.add_argument("--output-root", default=".")
    args = parser.parse_args()
    out = export_registry_csv(args.output_root)
    print(out)


if __name__ == "__main__":
    main()
