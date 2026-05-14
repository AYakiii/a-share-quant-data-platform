from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from qsys.data.factor_lake.backfill_plan import RawBackfillPlanItem, generate_default_backfill_plan


@dataclass(frozen=True)
class RawBackfillTask:
    task_id: str
    dataset_name: str
    source_family: str
    api_name: str
    priority: int
    partition: str
    fetch_params: str
    output_partition: str
    status: str
    planned_start_date: str
    planned_end_date: str
    notes: str


def _priority_label(priority: int) -> str:
    return {1: "P0", 2: "P1", 3: "P2"}.get(priority, f"P{priority}")


def _default_partition_and_params(plan: RawBackfillPlanItem) -> tuple[dict, dict]:
    # conservative dry-run defaults; no massive universe expansion
    if plan.dataset_name == "daily_bar_raw":
        p = {"symbol": "000001", "year": "2010"}
        q = {"symbol": "000001", "start_date": "20100101", "end_date": "20101231"}
    elif plan.dataset_name == "index_bar_raw":
        p = {"index_symbol": "000300", "year": "2010"}
        q = {"symbol": "000300", "start_date": "20100101", "end_date": "20101231"}
    elif plan.dataset_name == "margin_detail_raw":
        p = {"exchange": "sse", "trade_date": "20100104"}
        q = {"date": "20100104"}
    else:
        p = {"planning_bucket": "default"}
        q = {"start_date": plan.backfill_start_date, "end_date": plan.backfill_end_date}
    return p, q


def generate_tasks_from_plan_item(plan: RawBackfillPlanItem) -> list[RawBackfillTask]:
    partition, params = _default_partition_and_params(plan)
    task_id = f"{plan.dataset_name}__{plan.api_name}__{_priority_label(plan.priority)}"
    return [
        RawBackfillTask(
            task_id=task_id,
            dataset_name=plan.dataset_name,
            source_family=plan.source_family,
            api_name=plan.api_name,
            priority=plan.priority,
            partition=json.dumps(partition, ensure_ascii=False, sort_keys=True),
            fetch_params=json.dumps(params, ensure_ascii=False, sort_keys=True),
            output_partition=json.dumps(partition, ensure_ascii=False, sort_keys=True),
            status="planned",
            planned_start_date=plan.backfill_start_date,
            planned_end_date=plan.backfill_end_date,
            notes=plan.notes,
        )
    ]


def generate_tasks_from_default_backfill_plan() -> list[RawBackfillTask]:
    tasks: list[RawBackfillTask] = []
    for item in generate_default_backfill_plan():
        tasks.extend(generate_tasks_from_plan_item(item))
    return tasks


def filter_tasks(tasks: Iterable[RawBackfillTask], priority: str | None = None, source_family: str | None = None, dataset_name: str | None = None, max_tasks: int | None = None) -> list[RawBackfillTask]:
    out: list[RawBackfillTask] = []
    for t in tasks:
        if priority and _priority_label(t.priority) != priority:
            continue
        if source_family and t.source_family != source_family:
            continue
        if dataset_name and t.dataset_name != dataset_name:
            continue
        out.append(t)
    if max_tasks is not None:
        return out[:max_tasks]
    return out


def tasks_to_frame(tasks: list[RawBackfillTask]) -> pd.DataFrame:
    return pd.DataFrame([asdict(t) for t in tasks])


def dry_run_summary(tasks: list[RawBackfillTask]) -> pd.DataFrame:
    df = tasks_to_frame(tasks)
    if df.empty:
        return pd.DataFrame(columns=["source_family", "dataset_name", "priority", "task_count"])
    g = df.groupby(["source_family", "dataset_name", "priority"], as_index=False).size()
    return g.rename(columns={"size": "task_count"})


def export_backfill_tasks_csv(output_root: str | Path = ".", tasks: list[RawBackfillTask] | None = None) -> tuple[Path, Path]:
    out_dir = Path(output_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    task_list = tasks or generate_tasks_from_default_backfill_plan()
    tasks_path = out_dir / "raw_backfill_tasks.csv"
    summary_path = out_dir / "raw_backfill_task_summary.csv"
    tasks_to_frame(task_list).to_csv(tasks_path, index=False, encoding="utf-8-sig")
    dry_run_summary(task_list).to_csv(summary_path, index=False, encoding="utf-8-sig")
    return tasks_path, summary_path
