from __future__ import annotations

import json

import pandas as pd

from qsys.utils.run_factor_lake_p0_smoke import build_p0_smoke_tasks, run_p0_smoke


def test_build_p0_smoke_tasks_shape():
    tasks = build_p0_smoke_tasks()
    assert len(tasks) == 4
    assert {t.dataset_name for t in tasks} >= {"daily_bar_raw", "index_bar_raw", "margin_detail_raw"}


def test_run_p0_smoke_dry_run_writes_summary(monkeypatch, tmp_path):
    def fake_exec(tasks, output_root, metastore_path, **kwargs):
        return {
            "task_count": 2,
            "result_count": 2,
            "results": [{"task_id": tasks[0].task_id, "status": "dry_run"}, {"task_id": tasks[1].task_id, "status": "dry_run"}],
            "summary": [{"source_family": "market_price", "dataset_name": "daily_bar_raw", "status": "dry_run", "count": 1}],
        }

    monkeypatch.setattr("qsys.utils.run_factor_lake_p0_smoke.execute_backfill_tasks", fake_exec)
    out = run_p0_smoke(str(tmp_path), str(tmp_path / "meta.sqlite"), execute=False, max_tasks=2)
    assert out["mode"] == "dry_run"
    assert (tmp_path / "outputs" / "factor_lake_smoke" / "p0_smoke_summary.json").exists()
    assert (tmp_path / "outputs" / "factor_lake_smoke" / "p0_smoke_summary.csv").exists()


def test_run_p0_smoke_execute_readback(monkeypatch, tmp_path):
    tasks = build_p0_smoke_tasks()

    def fake_exec(tasks, output_root, metastore_path, **kwargs):
        return {
            "task_count": 1,
            "result_count": 1,
            "results": [{"task_id": tasks[0].task_id, "status": "success"}],
            "summary": [],
        }

    def fake_readback(root, dataset, api_name, partition):
        return pd.DataFrame({"x": [1]})

    monkeypatch.setattr("qsys.utils.run_factor_lake_p0_smoke.execute_backfill_tasks", fake_exec)
    monkeypatch.setattr("qsys.utils.run_factor_lake_p0_smoke.read_raw_partition", fake_readback)

    out = run_p0_smoke(str(tmp_path), str(tmp_path / "meta.sqlite"), execute=True, max_tasks=1)
    assert out["readback_ok"] is True
