from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake import raw_ingest
from qsys.data.factor_lake.io import raw_partition_path
from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


def _df(**values: object) -> pd.DataFrame:
    return pd.DataFrame({key: [value] for key, value in values.items()})


def test_hybrid_plan_completeness_non_symbol_once_and_symbol_chunks(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def stock_yjyg_em(date: str) -> pd.DataFrame:
        calls.append(("stock_yjyg_em", date))
        return _df(date=date)

    def stock_balance_sheet_by_report_em(symbol: str) -> pd.DataFrame:
        calls.append(("stock_balance_sheet_by_report_em", symbol))
        return _df(symbol=symbol)

    symbols = ["000001", "000002", "000003", "000004", "000005"]
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=symbols,
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em", "stock_balance_sheet_by_report_em"],
        adapter_map={
            "stock_yjyg_em": stock_yjyg_em,
            "stock_balance_sheet_by_report_em": stock_balance_sheet_by_report_em,
        },
        max_workers=1,
        symbol_batch_size=2,
    )

    assert len(out["rows"]) == 6
    assert calls.count(("stock_yjyg_em", "20240331")) == 1
    assert [call for call in calls if call[0] == "stock_balance_sheet_by_report_em"] == [
        ("stock_balance_sheet_by_report_em", "SZ000001"),
        ("stock_balance_sheet_by_report_em", "SZ000002"),
        ("stock_balance_sheet_by_report_em", "SZ000003"),
        ("stock_balance_sheet_by_report_em", "SZ000004"),
        ("stock_balance_sheet_by_report_em", "SZ000005"),
    ]

    report = pd.read_csv(tmp_path / "_operation_review" / "hybrid_batch_report.csv")
    assert report["batch_scope"].tolist() == ["non_symbol", "symbol_chunk", "symbol_chunk", "symbol_chunk"]
    assert report["task_count"].tolist() == [1, 2, 2, 1]
    assert report["symbol_start_index"].tolist() == [-1, 0, 2, 4]
    assert report["symbol_end_index"].tolist() == [-1, 1, 3, 4]
    assert int(report["task_count"].sum()) == 6


def test_flat_compatibility_uses_single_flat_batch(tmp_path: Path) -> None:
    def stock_balance_sheet_by_report_em(symbol: str) -> pd.DataFrame:
        return _df(symbol=symbol)

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001", "000002"],
        selected_api_names=["stock_balance_sheet_by_report_em"],
        adapter_map={"stock_balance_sheet_by_report_em": stock_balance_sheet_by_report_em},
        max_workers=1,
        symbol_batch_size=0,
    )

    report = pd.read_csv(tmp_path / "_operation_review" / "hybrid_batch_report.csv")
    assert report["batch_scope"].tolist() == ["flat"]
    assert report["task_count"].tolist() == [2]


def test_bounded_inflight_window_and_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    observed_max_pending = 0
    lock = threading.Lock()

    real_executor = raw_ingest.ThreadPoolExecutor

    pending_submitted = 0

    class TrackingExecutor(real_executor):  # type: ignore[misc]
        def submit(self, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
            nonlocal observed_max_pending, pending_submitted
            with lock:
                pending_submitted += 1
                observed_max_pending = max(observed_max_pending, pending_submitted)
            future = super().submit(*args, **kwargs)

            def _mark_done(_future: object) -> None:
                nonlocal pending_submitted
                with lock:
                    pending_submitted -= 1

            future.add_done_callback(_mark_done)
            return future

    monkeypatch.setattr(raw_ingest, "ThreadPoolExecutor", TrackingExecutor)

    def stock_balance_sheet_by_report_em(symbol: str) -> pd.DataFrame:
        time.sleep(0.02)
        return _df(symbol=symbol)

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001", "000002", "000003", "000004", "000005"],
        selected_api_names=["stock_balance_sheet_by_report_em"],
        adapter_map={"stock_balance_sheet_by_report_em": stock_balance_sheet_by_report_em},
        max_workers=2,
        max_inflight_tasks=3,
        symbol_batch_size=0,
    )
    assert observed_max_pending <= 3

    with pytest.raises(ValueError, match="max_inflight_tasks"):
        run_raw_coverage_ingest(
            output_root=str(tmp_path / "bad"),
            families=["financial_fundamental"],
            symbols=["000001"],
            selected_api_names=["stock_balance_sheet_by_report_em"],
            adapter_map={"stock_balance_sheet_by_report_em": stock_balance_sheet_by_report_em},
            max_workers=2,
            max_inflight_tasks=1,
        )


def test_batch_checkpoint_resume_and_fingerprint_protection(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def first_stock_yjyg_em(date: str) -> pd.DataFrame:
        calls.append(("stock_yjyg_em", date))
        return _df(date=date)

    def first_balance(symbol: str) -> pd.DataFrame:
        calls.append(("balance", symbol))
        if symbol == "SZ000001":
            raise ConnectionError("boom")
        return _df(symbol=symbol)

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001", "000002"],
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em", "stock_balance_sheet_by_report_em"],
        adapter_map={"stock_yjyg_em": first_stock_yjyg_em, "stock_balance_sheet_by_report_em": first_balance},
        max_workers=1,
        symbol_batch_size=2,
        continue_on_error=True,
    )
    assert ("stock_yjyg_em", "20240331") in calls

    checkpoint_path = tmp_path / "_operation_review" / "hybrid_batch_checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["plan_fingerprint"]
    assert 0 in checkpoint["completed_batches"]
    assert (tmp_path / "_operation_review" / "hybrid_batch_plan.csv").exists()
    assert (tmp_path / "_operation_review" / "hybrid_batch_report.csv").exists()

    calls.clear()

    def recovery_balance(symbol: str) -> pd.DataFrame:
        calls.append(("balance", symbol))
        return _df(symbol=symbol)

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001", "000002"],
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em", "stock_balance_sheet_by_report_em"],
        adapter_map={"stock_yjyg_em": first_stock_yjyg_em, "stock_balance_sheet_by_report_em": recovery_balance},
        max_workers=1,
        symbol_batch_size=2,
        resume=True,
        continue_on_error=True,
    )
    assert all(call[0] != "stock_yjyg_em" for call in calls)
    assert ("balance", "SZ000001") in calls

    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        run_raw_coverage_ingest(
            output_root=str(tmp_path),
            families=["financial_fundamental"],
            symbols=["000001", "000002", "000003"],
            report_dates=["20240331"],
            selected_api_names=["stock_yjyg_em", "stock_balance_sheet_by_report_em"],
            adapter_map={"stock_yjyg_em": first_stock_yjyg_em, "stock_balance_sheet_by_report_em": recovery_balance},
            max_workers=1,
            symbol_batch_size=2,
            resume=True,
        )


def test_pre_request_existing_partition_skip_and_partial_state(tmp_path: Path) -> None:
    calls: list[str] = []
    partition = {"date": "20240331"}
    existing_dir = raw_partition_path(tmp_path / "exists", "financial_fundamental", "stock_yjyg_em", partition)
    pd.DataFrame({"x": [1]}).to_parquet(existing_dir / "data.parquet", index=False)
    (existing_dir / "metadata.json").write_text("{}", encoding="utf-8")

    def adapter(date: str) -> pd.DataFrame:
        calls.append(date)
        return _df(date=date)

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "exists"),
        families=["financial_fundamental"],
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": adapter},
        max_workers=1,
    )
    assert calls == []
    assert out["rows"][0]["status"] == "already_exists"

    partial_dir = raw_partition_path(tmp_path / "partial", "financial_fundamental", "stock_yjyg_em", partition)
    pd.DataFrame({"x": [1]}).to_parquet(partial_dir / "data.parquet", index=False)
    partial = run_raw_coverage_ingest(
        output_root=str(tmp_path / "partial"),
        families=["financial_fundamental"],
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": adapter},
        max_workers=1,
    )
    assert calls == []
    assert partial["rows"][0]["status"] == "failed"
    assert partial["rows"][0]["error_type"] == "partial_partition_state"


def test_heartbeat_payload_contains_batch_progress_fields(tmp_path: Path) -> None:
    def stock_yjyg_em(date: str) -> pd.DataFrame:
        return _df(date=date)

    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": stock_yjyg_em},
        max_workers=1,
        heartbeat_sec=0,
        symbol_batch_size=2,
    )
    payload = json.loads((tmp_path / "_operation_review" / "live_progress.json").read_text(encoding="utf-8"))
    for existing_field in ["total_tasks", "completed_tasks", "success_tasks", "selected_apis"]:
        assert existing_field in payload
    for batch_field in [
        "current_batch_id",
        "current_batch_scope",
        "completed_batches",
        "total_batches",
        "current_batch_task_count",
        "current_batch_completed_tasks",
    ]:
        assert batch_field in payload
