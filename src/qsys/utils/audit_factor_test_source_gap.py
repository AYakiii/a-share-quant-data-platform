from __future__ import annotations

import argparse
import json

from qsys.data.factor_lake.source_gap_audit import run_source_gap_audit


def main() -> None:
    p = argparse.ArgumentParser(description="Audit Factor_test viable sources vs current raw coverage")
    p.add_argument("--viable-sources-csv", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--raw-ingest-catalog-csv", required=False, default=None)
    p.add_argument("--raw-source-health-matrix-csv", required=False, default=None)
    args = p.parse_args()

    out = run_source_gap_audit(args.viable_sources_csv, args.output_root, args.raw_ingest_catalog_csv, args.raw_source_health_matrix_csv)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
