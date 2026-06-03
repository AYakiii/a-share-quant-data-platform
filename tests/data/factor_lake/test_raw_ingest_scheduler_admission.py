from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor as RealThreadPoolExecutor
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake import raw_ingest
from qsys.data.factor_lake.io import raw_partition_path
from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame | None = None):
        self.raw = raw if raw is not None else pd.DataFrame({"value": [1]})


def test_szse_trade_date_admission_cap_does_not_block_healthy_sources(tmp_path: Path) -> None:
    lock = threading.Lock()
    szse_running = 0
    max_szse_running = 0
    start_order: list[str] = []

    def stock_margin_detail_szse(date: str) -> _Result:
        nonlocal szse_running, max_szse_running
        with lock:
            szse_running += 1
            max_szse_running = max(max_szse_running, szse_running)
            start_order.append(f"szse:{date}")
        time.sleep(0.05)
        with lock:
            szse_running -= 1
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    def stock_financial_analysis_indicator(symbol: str) -> _Result:
        with lock:
            start_order.append(f"healthy:{symbol}")
        return _Result(pd.DataFrame({"symbol": [symbol], "value": [1]}))

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["margin_leverage", "financial_fundamental"],
        selected_api_names=["stock_margin_detail_szse", "stock_financial_analysis_indicator"],
        trade_dates=["20240102", "20240103", "20240104", "20240105"],
        symbols=["000001", "000002"],
        adapter_map={
            "stock_margin_detail_szse": stock_margin_detail_szse,
            "stock_financial_analysis_indicator": stock_financial_analysis_indicator,
        },
        max_workers=4,
        max_inflight_tasks=4,
        symbol_batch_size=0,
        include_disabled=True,
    )

    assert max_szse_running <= 2
    assert any(item.startswith("healthy:") for item in start_order[:4])
    assert start_order.index("healthy:000001") < start_order.index("szse:20240104")
    assert {row["status"] for row in out["rows"]} == {"success"}

    observation = pd.read_csv(tmp_path / "_operation_review" / "source_concurrency_observation.csv")
    szse = observation[(observation["source_key"] == "szse") & (observation["workload_class"] == "trade_date_scoped")].iloc[0]
    assert int(szse["peak_inflight_tasks"]) <= 2
    assert int(szse["blocked_by_admission_control"]) > 0


def test_scheduler_admission_preserves_global_inflight_window(tmp_path: Path, monkeypatch) -> None:
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

    def stock_margin_detail_szse(date: str) -> _Result:
        time.sleep(0.03)
        return _Result(pd.DataFrame({"date": [date]}))

    def stock_financial_analysis_indicator(symbol: str) -> _Result:
        time.sleep(0.03)
        return _Result(pd.DataFrame({"symbol": [symbol]}))

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["margin_leverage", "financial_fundamental"],
        selected_api_names=["stock_margin_detail_szse", "stock_financial_analysis_indicator"],
        trade_dates=["20240102", "20240103", "20240104", "20240105"],
        symbols=["000001", "000002", "000003", "000004"],
        adapter_map={
            "stock_margin_detail_szse": stock_margin_detail_szse,
            "stock_financial_analysis_indicator": stock_financial_analysis_indicator,
        },
        max_workers=3,
        max_inflight_tasks=4,
        symbol_batch_size=0,
        include_disabled=True,
    )

    assert counts["max_unfinished"] <= 4


def test_admission_control_preserves_resume_fingerprint_and_existing_partition_skip(tmp_path: Path) -> None:
    calls: list[str] = []
    partition = {"date": "20240102"}
    out_dir = raw_partition_path(tmp_path, "margin_leverage", "stock_margin_detail_szse", partition)
    pd.DataFrame({"date": ["20240102"], "value": [1]}).to_parquet(out_dir / "data.parquet", index=False)
    (out_dir / "metadata.json").write_text("{}", encoding="utf-8")

    def stock_margin_detail_szse(date: str) -> _Result:
        calls.append(date)
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    kwargs = dict(
        output_root=str(tmp_path),
        families=["margin_leverage"],
        selected_api_names=["stock_margin_detail_szse"],
        trade_dates=["20240102", "20240103"],
        adapter_map={"stock_margin_detail_szse": stock_margin_detail_szse},
        max_workers=2,
        max_inflight_tasks=2,
        symbol_batch_size=0,
    )
    first = run_raw_coverage_ingest(**kwargs)
    assert calls == ["20240103"]
    assert {row["status"] for row in first["rows"]} == {"already_exists", "success"}

    checkpoint_path = tmp_path / "_operation_review" / "hybrid_batches" / "raw" / "hybrid_batch_checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert "SOURCE_WORKLOAD_ADMISSION_OVERRIDES" not in json.dumps(checkpoint["fingerprint_payload"])
    first_fingerprint = checkpoint["fingerprint"]

    calls.clear()
    run_raw_coverage_ingest(**kwargs, resume=True)
    resumed_checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert calls == []
    assert resumed_checkpoint["fingerprint"] == first_fingerprint
