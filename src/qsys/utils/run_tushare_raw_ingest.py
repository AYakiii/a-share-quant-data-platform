"""CLI for local-only Tushare Raw acquisition."""
from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.sources.tushare_acquisition import manifest_json, run_tushare_raw_ingest, run_tushare_raw_ingest_dry_run
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig


def _csv_arg(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    """Build the Tushare local raw acquisition argument parser."""
    parser = argparse.ArgumentParser(description="Run local-only Tushare Raw acquisition; never writes Google Drive or promotes data.")
    parser.add_argument("--start-date", required=True, help="Inclusive start trade date, YYYYMMDD.")
    parser.add_argument("--end-date", required=True, help="Inclusive end trade date, YYYYMMDD.")
    parser.add_argument("--api-names", default="", help="Comma-separated Tushare APIs, e.g. daily,daily_basic.")
    parser.add_argument("--families", default="", help="Comma-separated source families selected through the source registry.")
    parser.add_argument("--symbols-file", required=True, help="Provider-neutral canonical Universe symbols file.")
    parser.add_argument("--universe-name", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--max-workers", type=int, default=1, help="Reserved concurrency control; M1-A executes conservatively.")
    parser.add_argument("--request-sleep", type=float, default=0.3)
    parser.add_argument("--request-jitter", type=float, default=0.0)
    parser.add_argument("--retry", type=int, default=2)
    parser.add_argument("--dry-run", "--plan-only", dest="dry_run", action="store_true", help="Generate the plan and artifacts without calling Tushare.")
    parser.add_argument("--resume", action="store_true", help="Skip complete existing partitions containing data.parquet and metadata.json.")
    parser.add_argument("--expected-symbol-count", type=int, default=None, help="Optional compatibility guard for older M0 smoke commands.")
    return parser


def config_from_args(args: argparse.Namespace) -> TushareRawIngestConfig:
    """Convert parsed CLI args into an acquisition config."""
    return TushareRawIngestConfig(
        symbols_file=Path(args.symbols_file),
        universe_name=args.universe_name,
        expected_symbol_count=args.expected_symbol_count,
        start_date=args.start_date,
        end_date=args.end_date,
        output_root=Path(args.output_root),
        dataset_version=args.dataset_version,
        api_names=_csv_arg(args.api_names),
        families=_csv_arg(args.families),
        max_workers=args.max_workers,
        request_sleep=args.request_sleep,
        request_jitter=args.request_jitter,
        retry=args.retry,
        dry_run=args.dry_run,
        resume=args.resume,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = config_from_args(args)
    manifest = run_tushare_raw_ingest_dry_run(cfg, require_token=False) if cfg.dry_run else run_tushare_raw_ingest(cfg)
    print(manifest_json(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
