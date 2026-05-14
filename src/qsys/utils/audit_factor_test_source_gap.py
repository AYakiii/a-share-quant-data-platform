from __future__ import annotations

import argparse

from qsys.data.factor_lake.source_gap_audit import (
    build_source_gap_audit,
    write_source_gap_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit factor_test viable API coverage gaps")
    parser.add_argument("--viable-sources-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--raw-ingest-catalog-csv", default=None)
    parser.add_argument("--raw-source-health-matrix-csv", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_source_gap_audit(
        viable_sources_csv=args.viable_sources_csv,
        raw_ingest_catalog_csv=args.raw_ingest_catalog_csv,
        raw_source_health_matrix_csv=args.raw_source_health_matrix_csv,
    )
    gap_path, plan_path = write_source_gap_outputs(result=result, output_root=args.output_root)
    print(f"Wrote gap matrix: {gap_path}")
    print(f"Wrote expansion plan: {plan_path}")


if __name__ == "__main__":
    main()
