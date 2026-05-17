from __future__ import annotations

import argparse
import json
import akshare as ak

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_official


def _split_csv(v: str) -> list[str]:
    return [x.strip() for x in v.split(",") if x.strip()]


def main() -> None:
    p = argparse.ArgumentParser(description="Official Stage-1 dataset-centered Raw Data Lake ingest runner")
    p.add_argument("--output-root", default="outputs/factor_lake_raw")
    p.add_argument("--families", default="market_price,index_market,margin_leverage,financial_fundamental,industry_concept,event_ownership,corporate_action,disclosure_ir,trading_attention")
    p.add_argument("--start-date", default="20100101")
    p.add_argument("--end-date", default="20101231")
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--request-sleep", type=float, default=0.0)
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument("--include-disabled", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--symbols", default="")
    p.add_argument("--index-symbols", default="")
    p.add_argument("--trade-dates", default="")
    p.add_argument("--report-dates", default="")
    p.add_argument("--industry-names", default="")
    p.add_argument("--concept-names", default="")
    p.add_argument("--api-names", default="")
    args = p.parse_args()

    out = run_raw_ingest_official(
        output_root=args.output_root,
        families=_split_csv(args.families),
        symbols=_split_csv(args.symbols),
        index_symbols=_split_csv(args.index_symbols),
        trade_dates=_split_csv(args.trade_dates),
        report_dates=_split_csv(args.report_dates),
        industry_names=_split_csv(args.industry_names),
        concept_names=_split_csv(args.concept_names),
        selected_api_names=_split_csv(args.api_names),
        start_date=args.start_date,
        end_date=args.end_date,
        max_workers=args.max_workers,
        request_sleep=args.request_sleep,
        continue_on_error=args.continue_on_error,
        include_disabled=args.include_disabled,
        resume=args.resume,
        ak_module=ak,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
