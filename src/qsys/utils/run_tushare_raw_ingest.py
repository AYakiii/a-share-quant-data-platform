"""Tushare Raw ingest CLI skeleton for dry-run validation only."""
from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.sources.tushare_acquisition import manifest_json, run_tushare_raw_ingest_dry_run
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a Tushare Raw ingest dry run; no historical API pull is performed.")
    parser.add_argument("--symbols-file", required=True)
    parser.add_argument("--universe-name", required=True)
    parser.add_argument("--expected-symbol-count", required=True, type=int)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--provider", default="tushare")
    parser.add_argument("--storage-schema-version", default="v1")
    parser.add_argument("--dry-run", action="store_true", help="Required in M0; no formal Tushare pull is implemented.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.dry_run:
        raise RuntimeError("M0 Tushare entrypoint supports --dry-run only; formal API pulls are intentionally disabled.")
    cfg = TushareRawIngestConfig(
        symbols_file=Path(args.symbols_file),
        universe_name=args.universe_name,
        expected_symbol_count=args.expected_symbol_count,
        start_date=args.start_date,
        end_date=args.end_date,
        output_root=Path(args.output_root),
        provider=args.provider,
        storage_schema_version=args.storage_schema_version,
    )
    print(manifest_json(run_tushare_raw_ingest_dry_run(cfg)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
