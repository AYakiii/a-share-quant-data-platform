from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor as RealThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake import raw_ingest
from qsys.data.factor_lake.io import raw_partition_path
from qsys.data.factor_lake.raw_ingest import _build_hybrid_batches, run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame | None = None):
        self.raw = raw if raw is not None else pd.DataFrame({"value": [1]})


def _task_id(task: tuple[str, str, dict[str, str]]) -> str:
    family, api_name, params = task
    return json.dumps({"family": family, "api": api_name, "params": params}, sort_keys=True, ensure_ascii=False)


def test_hybrid_plan_completeness_and_non_symbol_uniqueness():
    tasks = [
        ("corporate_action", "stock_fhps_em", {}),
        ("financial_fundamental", "stock_financial_analysis_indicator", {"symbol": "000001"}),
        ("financial_fundamental", "stock_financial_analysis_indicator", {"symbol": "000002"}),
        ("financial_fundamental", "stock_financial_analysis_indicator", {"symbol": "000003"}),
    ]
    batches = _build_hybrid_batches(tasks, symbol_batch_size=2)
    planned = [task for batch in batches for task in batch["tasks"]]
    assert [_task_id(task) for task in planned].count(_task_id(tasks[0])) == 1
    assert sorted(_task_id(task) for task in planned) == sorted(_task_id(task) for task in tasks)


def test_symbol_chunk_split_and_flat_compatibility():
    tasks = [("financial_fundamental", "stock_financial_analysis_indicator", {"symbol": f"00000{i}"}) for i in range(5)]
    hybrid = _build_hybrid_batches(tasks, symbol_batch_size=2)
    symbol_chunks = [batch for batch in hybrid if batch["batch_scope"] == "symbol_chunk"]
    assert [len(batch["symbols"]) for batch in symbol_chunks] == [2, 2, 1]
    assert [len(batch["tasks"]) for batch in symbol_chunks] == [2, 2, 1]

    flat = _build_hybrid_batches(tasks, symbol_batch_size=0)
    assert len(flat) == 1
    assert flat[0]["batch_scope"] == "flat"
    assert flat[0]["tasks"] == tasks


def test_non_symbol_task_executes_once_not_once_per_symbol_chunk(tmp_path):
    calls: list[str] = []

    def stock_fhps_em():
        calls.append("non_symbol")
        return _Result()

    def stock_financial_analysis_indicator(symbol: str):
        calls.append(symbol)
        return _Result()

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["corporate_action", "financial_fundamental"],
        selected_api_names=["stock_fhps_em", "stock_financial_analysis_indicator"],
        symbols=["000001", "000002", "000003", "000004", "000005"],
        adapter_map={"stock_fhps_em": stock_fhps_em, "stock_financial_analysis_indicator": stock_financial_analysis_indicator},
        max_workers=1,
        symbol_batch_size=2,
        include_disabled=True,
    )
    assert calls.count("non_symbol") == 1
    assert len([row for row in out["rows"] if row["api_name"] == "stock_financial_analysis_indicator"]) == 5


def test_bounded_inflight_window_and_validation(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="max_inflight_tasks"):
        run_raw_coverage_ingest(
            output_root=str(tmp_path / "bad"),
            families=["financial_fundamental"],
            selected_api_names=["stock_financial_analysis_indicator"],
            symbols=["000001"],
            adapter_map={"stock_financial_analysis_indicator": lambda symbol: _Result()},
            max_workers=3,
            max_inflight_tasks=2,
        )

    counts = {"unfinished": 0, "max_unfinished": 0}
    lock = threading.Lock()

    class TrackingExecutor(RealThreadPoolExecutor):
        def submit(self, *args, **kwargs):
            fut = super().submit(*args, **kwargs)
            with lock:
                counts["unfinished"] += 1
                counts["max_unfinished"] = max(counts["max_unfinished"], counts["unfinished"])

            def done(_future):
                with lock:
                    counts["unfinished"] -= 1

            fut.add_done_callback(done)
            return fut

    monkeypatch.setattr(raw_ingest, "ThreadPoolExecutor", TrackingExecutor)

    def slow_dividend(symbol: str):
        time.sleep(0.02)
        return _Result()

    run_raw_coverage_ingest(
        output_root=str(tmp_path / "ok"),
        families=["financial_fundamental"],
        selected_api_names=["stock_financial_analysis_indicator"],
        symbols=[f"00000{i}" for i in range(8)],
        adapter_map={"stock_financial_analysis_indicator": slow_dividend},
        max_workers=3,
        max_inflight_tasks=4,
        symbol_batch_size=0,
    )
    assert counts["max_unfinished"] <= 4


def test_checkpoint_artifacts_and_heartbeat_fields(tmp_path):
    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        selected_api_names=["stock_financial_analysis_indicator"],
        symbols=["000001", "000002", "000003"],
        adapter_map={"stock_financial_analysis_indicator": lambda symbol: _Result()},
        max_workers=1,
        symbol_batch_size=2,
        heartbeat_sec=0,
    )
    op = tmp_path / "_operation_review"
    plan = pd.read_csv(op / "hybrid_batch_plan.csv")
    report = pd.read_csv(op / "hybrid_batch_report.csv")
    checkpoint = json.loads((op / "hybrid_batch_checkpoint.json").read_text(encoding="utf-8"))
    required = set(raw_ingest.BATCH_ARTIFACT_COLUMNS)
    assert required.issubset(plan.columns)
    assert required.issubset(report.columns)
    assert "fingerprint" in checkpoint
    assert "fingerprint_payload" in checkpoint

    live = json.loads((op / "live_progress.json").read_text(encoding="utf-8"))
    for field in ["event", "lane", "total_tasks", "completed_tasks", "success_tasks"]:
        assert field in live
    for field in ["current_batch_id", "current_batch_scope", "completed_batches", "total_batches", "current_batch_task_count", "current_batch_completed_tasks"]:
        assert field in live


def test_resume_skips_completed_and_reruns_failed_or_incomplete_batches(tmp_path):
    calls: list[str] = []

    def dividend(symbol: str):
        calls.append(symbol)
        return _Result()

    kwargs = dict(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        selected_api_names=["stock_financial_analysis_indicator"],
        symbols=["000001", "000002", "000003"],
        adapter_map={"stock_financial_analysis_indicator": dividend},
        max_workers=1,
        symbol_batch_size=2,
        include_disabled=True,
    )
    run_raw_coverage_ingest(**kwargs)
    calls.clear()
    run_raw_coverage_ingest(**kwargs, resume=True)
    assert calls == []

    checkpoint_path = tmp_path / "_operation_review" / "hybrid_batch_checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["batches"][0]["status"] = "completed"
    checkpoint["batches"][1]["status"] = "failed"
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    run_raw_coverage_ingest(**kwargs, resume=True)
    assert calls == []

    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        run_raw_coverage_ingest(**{**kwargs, "symbol_batch_size": 1}, resume=True)


def test_existing_partition_skip_and_partial_partition_state(tmp_path):
    calls = {"fhps": 0}

    def stock_fhps_em():
        calls["fhps"] += 1
        return _Result()

    partition = {"api_name": "stock_fhps_em"}
    out_dir = raw_partition_path(tmp_path / "exists", "corporate_action", "stock_fhps_em", partition)
    pd.DataFrame({"x": [1]}).to_parquet(out_dir / "data.parquet", index=False)
    (out_dir / "metadata.json").write_text("{}", encoding="utf-8")
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "exists"),
        families=["corporate_action"],
        selected_api_names=["stock_fhps_em"],
        adapter_map={"stock_fhps_em": stock_fhps_em},
        max_workers=1,
    )
    assert calls["fhps"] == 0
    assert out["rows"][0]["status"] == "already_exists"

    partial_dir = raw_partition_path(tmp_path / "partial", "corporate_action", "stock_fhps_em", partition)
    (partial_dir / "metadata.json").write_text("{}", encoding="utf-8")
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "partial"),
        families=["corporate_action"],
        selected_api_names=["stock_fhps_em"],
        adapter_map={"stock_fhps_em": stock_fhps_em},
        max_workers=1,
    )
    assert calls["fhps"] == 0
    assert out["rows"][0]["status"] == "failed"
    assert out["rows"][0]["error_type"] == "partial_partition_state"
