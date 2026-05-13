from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.backfill_execute import execute_backfill_tasks
from qsys.data.factor_lake.backfill_tasks import RawBackfillTask
from qsys.data.factor_lake.local_api import read_raw_partition


def build_p0_smoke_tasks() -> list[RawBackfillTask]:
    return [
        RawBackfillTask(
            task_id="p0_daily_bar_000001_2010",
            dataset_name="daily_bar_raw",
            source_family="market_price",
            api_name="stock_zh_a_hist",
            priority=1,
            partition='{"symbol":"000001","year":"2010"}',
            fetch_params='{"symbol":"000001","start_date":"20100101","end_date":"20100110"}',
            output_partition='{"symbol":"000001","year":"2010"}',
            status="planned",
            planned_start_date="2010-01-01",
            planned_end_date="2010-01-10",
            notes="P0 smoke tiny daily bar",
        ),
        RawBackfillTask(
            task_id="p0_index_bar_000300_2010",
            dataset_name="index_bar_raw",
            source_family="index_market",
            api_name="stock_zh_index_hist_csindex",
            priority=1,
            partition='{"index_symbol":"000300","year":"2010"}',
            fetch_params='{"symbol":"000300","start_date":"20100101","end_date":"20100110"}',
            output_partition='{"index_symbol":"000300","year":"2010"}',
            status="planned",
            planned_start_date="2010-01-01",
            planned_end_date="2010-01-10",
            notes="P0 smoke tiny index bar",
        ),
        RawBackfillTask(
            task_id="p1_margin_sse_20100104",
            dataset_name="margin_detail_raw",
            source_family="margin_leverage",
            api_name="stock_margin_detail_sse",
            priority=2,
            partition='{"exchange":"sse","trade_date":"20100104"}',
            fetch_params='{"date":"20100104"}',
            output_partition='{"exchange":"sse","trade_date":"20100104"}',
            status="planned",
            planned_start_date="2010-01-04",
            planned_end_date="2010-01-04",
            notes="P1 smoke SSE margin",
        ),
        RawBackfillTask(
            task_id="p1_margin_szse_20100104",
            dataset_name="margin_detail_raw",
            source_family="margin_leverage",
            api_name="stock_margin_detail_szse",
            priority=2,
            partition='{"exchange":"szse","trade_date":"20100104"}',
            fetch_params='{"date":"20100104"}',
            output_partition='{"exchange":"szse","trade_date":"20100104"}',
            status="planned",
            planned_start_date="2010-01-04",
            planned_end_date="2010-01-04",
            notes="P1 smoke SZSE margin",
        ),
    ]


def run_p0_smoke(output_root: str, metastore_path: str, execute: bool, max_tasks: int | None, request_sleep: float = 0.0) -> dict:
    dry_run = not execute
    if execute and max_tasks is None:
        raise ValueError("--execute requires --max-tasks for safe tiny execution.")

    tasks = build_p0_smoke_tasks()
    result = execute_backfill_tasks(tasks, output_root=output_root, metastore_path=metastore_path, max_tasks=max_tasks, dry_run=dry_run, continue_on_error=True, request_sleep=request_sleep)

    readback_ok = False
    readback_error = ""
    if execute:
        for r in result["results"]:
            if r["status"] in {"success", "empty"}:
                try:
                    t = next(x for x in tasks if x.task_id == r["task_id"])
                    df = read_raw_partition(output_root, t.dataset_name, t.api_name, json.loads(t.partition))
                    readback_ok = isinstance(df, pd.DataFrame)
                    break
                except Exception as exc:  # noqa: BLE001
                    readback_error = str(exc)

    out_dir = Path(output_root) / "outputs" / "factor_lake_smoke"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "mode": "dry_run" if dry_run else "execute",
        "task_count": result["task_count"],
        "result_count": result["result_count"],
        "summary": result["summary"],
        "readback_ok": readback_ok,
        "readback_error": readback_error,
        "metastore_path": metastore_path,
        "output_root": output_root,
    }
    (out_dir / "p0_smoke_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(result["results"]).to_csv(out_dir / "p0_smoke_summary.csv", index=False, encoding="utf-8-sig")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run P0/P1 tiny Raw Factor Lake smoke")
    parser.add_argument("--output-root", default="outputs/factor_lake_backfill")
    parser.add_argument("--metastore-path", default="outputs/factor_lake_backfill/metastore.sqlite")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--request-sleep", type=float, default=0.0)
    parser.add_argument("--max-tasks", type=int)
    args = parser.parse_args()

    execute = args.execute and not args.dry_run
    summary = run_p0_smoke(args.output_root, args.metastore_path, execute=execute, max_tasks=args.max_tasks, request_sleep=args.request_sleep)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
