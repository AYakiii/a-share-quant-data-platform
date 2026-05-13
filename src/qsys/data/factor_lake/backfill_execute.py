from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.backfill_tasks import RawBackfillTask
from qsys.data.factor_lake.io import write_raw_partition
from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.raw_ingest import DEFAULT_ADAPTERS


@dataclass
class RawBackfillTaskResult:
    task_id: str
    dataset_name: str
    source_family: str
    api_name: str
    status: str
    rows: int
    output_path: str
    metadata_path: str
    error_type: str
    error_message: str
    started_at: str
    finished_at: str
    elapsed_sec: float


def _ensure_task_result_table(db_path: str | Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists backfill_task_result(
                task_id text primary key,
                dataset_name text,
                source_family text,
                api_name text,
                status text,
                rows integer,
                output_path text,
                metadata_path text,
                error_type text,
                error_message text,
                started_at text,
                finished_at text,
                elapsed_sec real
            )
            """
        )


def _is_completed(db_path: str | Path, task_id: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("select status from backfill_task_result where task_id=?", (task_id,)).fetchone()
    return bool(row and row[0] == "success")


def _record_result(db_path: str | Path, result: RawBackfillTaskResult) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "insert or replace into backfill_task_result(task_id,dataset_name,source_family,api_name,status,rows,output_path,metadata_path,error_type,error_message,started_at,finished_at,elapsed_sec) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                result.task_id,
                result.dataset_name,
                result.source_family,
                result.api_name,
                result.status,
                result.rows,
                result.output_path,
                result.metadata_path,
                result.error_type,
                result.error_message,
                result.started_at,
                result.finished_at,
                result.elapsed_sec,
            ),
        )


def execute_backfill_task(task: RawBackfillTask, output_root: str, metastore_path: str, adapter_override: dict | None = None, dry_run: bool = True) -> RawBackfillTaskResult:
    db_path = Path(metastore_path)
    _ensure_task_result_table(db_path)
    start = datetime.now(UTC)
    t0 = time.perf_counter()

    if _is_completed(db_path, task.task_id):
        end = datetime.now(UTC)
        return RawBackfillTaskResult(task.task_id, task.dataset_name, task.source_family, task.api_name, "skipped_completed", 0, "", "", "", "", start.isoformat(), end.isoformat(), time.perf_counter() - t0)

    if dry_run:
        end = datetime.now(UTC)
        result = RawBackfillTaskResult(task.task_id, task.dataset_name, task.source_family, task.api_name, "dry_run", 0, "", "", "", "", start.isoformat(), end.isoformat(), time.perf_counter() - t0)
        _record_result(db_path, result)
        return result

    partition = json.loads(task.partition)
    fetch_params = json.loads(task.fetch_params)
    adapters = adapter_override or DEFAULT_ADAPTERS
    out_path = meta_path = ""
    status = "failed"
    rows = 0
    err_t = err_m = ""

    try:
        adapter = adapters[task.api_name]
        ret = adapter(**fetch_params)
        raw = ret.raw if hasattr(ret, "raw") else ret
        if not isinstance(raw, pd.DataFrame):
            raw = pd.DataFrame(raw)
        rows = len(raw)
        status = "empty" if raw.empty else "success"
        metadata = {
            "task_id": task.task_id,
            "dataset": task.dataset_name,
            "api_name": task.api_name,
            "source_family": task.source_family,
            "partition": partition,
            "row_count": rows,
            "col_count": len(raw.columns),
            "status": status,
            "ingested_at": datetime.now(UTC).isoformat(),
        }
        data_path, metadata_path = write_raw_partition(output_root, task.source_family, task.api_name, partition, raw, metadata)
        out_path, meta_path = str(data_path), str(metadata_path)
        ms = FactorLakeMetastore(db_path)
        ms.execute(
            "insert or replace into raw_dataset_inventory(dataset, source_family, api_name, partition_json, data_path, metadata_path, row_count, col_count) values (?, ?, ?, ?, ?, ?, ?, ?)",
            (task.dataset_name, task.source_family, task.api_name, json.dumps(partition, sort_keys=True), out_path, meta_path, rows, len(raw.columns)),
        )
    except Exception as exc:  # noqa: BLE001
        err_t = type(exc).__name__
        err_m = str(exc)
        status = "failed"

    end = datetime.now(UTC)
    result = RawBackfillTaskResult(task.task_id, task.dataset_name, task.source_family, task.api_name, status, rows, out_path, meta_path, err_t, err_m, start.isoformat(), end.isoformat(), time.perf_counter() - t0)
    _record_result(db_path, result)
    return result


def execute_backfill_tasks(tasks: list[RawBackfillTask], output_root: str, metastore_path: str, max_tasks: int | None = None, dry_run: bool = True, continue_on_error: bool = True, request_sleep: float = 0.0, adapter_override: dict | None = None) -> dict:
    selected = tasks[:max_tasks] if max_tasks is not None else tasks
    results: list[RawBackfillTaskResult] = []
    for t in selected:
        res = execute_backfill_task(t, output_root, metastore_path, adapter_override=adapter_override, dry_run=dry_run)
        results.append(res)
        if res.status == "failed" and not continue_on_error:
            break
        if request_sleep > 0:
            time.sleep(request_sleep)

    df = pd.DataFrame([asdict(r) for r in results]) if results else pd.DataFrame(columns=list(RawBackfillTaskResult.__annotations__.keys()))
    summary = df.groupby(["source_family", "dataset_name", "status"], as_index=False).size() if not df.empty else pd.DataFrame(columns=["source_family", "dataset_name", "status", "size"])
    return {"task_count": len(selected), "result_count": len(results), "results": [asdict(r) for r in results], "summary": summary.rename(columns={"size": "count"}).to_dict(orient="records")}
