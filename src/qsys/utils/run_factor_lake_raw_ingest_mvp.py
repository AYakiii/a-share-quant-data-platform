from __future__ import annotations

import argparse
import json

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_mvp


def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataset-centered Raw Ingest MVP")
    parser.add_argument("--output-root", default="outputs/factor_lake_raw_ingest_mvp")
    parser.add_argument("--metastore-path", default="outputs/factor_lake_raw_ingest_mvp/metastore.sqlite")
    parser.add_argument("--datasets", default="daily_bar_raw,index_bar_raw,margin_detail_raw")
    parser.add_argument("--symbols", default="000001")
    parser.add_argument("--index-symbols", default="000300")
    parser.add_argument("--trade-dates", default="20240329")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20240331")
    parser.add_argument("--request-sleep", type=float, default=0.0)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--daily-api-preference", default="stock_zh_a_daily")
    args = parser.parse_args()

    out = run_raw_ingest_mvp(
        datasets=_split_csv(args.datasets),
        root=args.output_root,
        metastore_path=args.metastore_path,
        symbols=_split_csv(args.symbols),
        index_symbols=_split_csv(args.index_symbols),
        trade_dates=_split_csv(args.trade_dates),
        start_date=args.start_date,
        end_date=args.end_date,
        continue_on_error=args.continue_on_error,
        request_sleep=args.request_sleep,
        daily_api_preference=args.daily_api_preference,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
