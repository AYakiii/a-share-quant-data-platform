from __future__ import annotations

import sqlite3

import pandas as pd

from qsys.data.factor_lake.backfill_execute import execute_backfill_tasks
from qsys.data.factor_lake.backfill_tasks import generate_tasks_from_default_backfill_plan


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_dry_run_does_not_call_adapter(tmp_path):
    called = {"n": 0}

    def bad_adapter(**kwargs):
        called["n"] += 1
        raise RuntimeError("should not be called")

    tasks = generate_tasks_from_default_backfill_plan()[:1]
    out = execute_backfill_tasks(tasks, str(tmp_path), str(tmp_path / "meta.sqlite"), dry_run=True, adapter_override={tasks[0].api_name: bad_adapter})
    assert out["result_count"] == 1
    assert called["n"] == 0
    assert out["results"][0]["status"] == "dry_run"


def test_real_execution_and_failure_record(tmp_path):
    tasks = generate_tasks_from_default_backfill_plan()[:2]

    def ok_stock(**kwargs):
        return _Result(pd.DataFrame({"a": [1]}))

    def fail_index(**kwargs):
        raise ValueError("boom")

    adapters = {
        tasks[0].api_name: ok_stock,
        tasks[1].api_name: fail_index,
    }
    out = execute_backfill_tasks(tasks, str(tmp_path), str(tmp_path / "meta.sqlite"), dry_run=False, adapter_override=adapters)
    statuses = [x["status"] for x in out["results"]]
    assert "success" in statuses
    assert "failed" in statuses

    with sqlite3.connect(tmp_path / "meta.sqlite") as conn:
        n = conn.execute("select count(*) from backfill_task_result").fetchone()[0]
    assert n == 2


def test_max_tasks_limit(tmp_path):
    tasks = generate_tasks_from_default_backfill_plan()
    out = execute_backfill_tasks(tasks, str(tmp_path), str(tmp_path / "meta.sqlite"), max_tasks=1, dry_run=True)
    assert out["task_count"] == 1
