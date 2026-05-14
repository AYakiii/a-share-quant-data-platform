from __future__ import annotations

import argparse
import json

from qsys.data.factor_lake.coverage_audit import run_coverage_audit


def main() -> None:
    p = argparse.ArgumentParser(description="Audit raw coverage catalog and generate health/wave plans")
    p.add_argument("--input-root", required=True)
    p.add_argument("--output-root", required=True)
    args = p.parse_args()

    out = run_coverage_audit(args.input_root, args.output_root)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
