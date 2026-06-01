from __future__ import annotations

import builtins
import json
import time
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.raw_ingest import (
    COVERAGE_API_SPECS,
    _build_raw_partition,
    _build_task_key,
    _params_for_mode,
    run_raw_coverage_ingest,
)


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_heartbeat_prints_periodically_with_flush_and_persists_live_progress(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, bool | None]] = []
    real_print = builtins.print

    def spy_print(*args, **kwargs):  # noqa: ANN002, ANN003
        text = " ".join(str(arg) for arg in args)
        calls.append((text, kwargs.get("flush")))
        real_print(*args, **kwargs)

    def slow_adapter(date: str) -> _Result:  # noqa: ARG001
        time.sleep(0.08)
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    monkeypatch.setattr(builtins, "print", spy_print)
    run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331", "20240630"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": slow_adapter},
        max_workers=2,
        heartbeat_sec=0.02,
        lane_name="main",
    )

    heartbeat_calls = [(text, flush) for text, flush in calls if text.startswith("[heartbeat]")]
    assert len(heartbeat_calls) >= 3
    assert all(flush is True for _, flush in heartbeat_calls)
    required_fields = [
        "lane=main",
        "elapsed_sec=",
        "total_tasks=2",
        "completed_tasks=",
        "pending_or_running_tasks=",
        "completion_pct=",
        "success_tasks=",
        "empty_tasks=",
        "failed_tasks=",
        "timeout_tasks=",
        "skipped_tasks=",
        "already_exists_tasks=",
        "pending_adapter_tasks=",
        "throughput_tasks_per_min=",
    ]
    assert all(field in heartbeat_calls[0][0] for field in required_fields)
    assert any("completed_tasks=0" in text for text, _ in heartbeat_calls)
    assert any("completed_tasks=2" in text for text, _ in heartbeat_calls)

    live_progress = json.loads((tmp_path / "_operation_review" / "live_progress.json").read_text(encoding="utf-8"))
    assert live_progress["event"] == "lane_completion"
    assert live_progress["lane"] == "main"
    assert live_progress["completed_tasks"] == 2
    assert live_progress["selected_apis"] == ["stock_yjyg_em"]
    events = [json.loads(line) for line in (tmp_path / "_operation_review" / "progress_events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert events[0]["event"] == "lane_start"
    assert events[-1]["event"] == "lane_completion"
    assert any(event["event"] == "heartbeat" for event in events)


def test_stock_history_dividend_detail_is_one_task_per_symbol_with_stable_keys() -> None:
    spec = next(row for row in COVERAGE_API_SPECS["corporate_action"] if row["api_name"] == "stock_history_dividend_detail")
    assert spec["param_mode"] == "symbol_only"
    symbols = ["000001", "600000"]
    params = _params_for_mode(spec["param_mode"], symbols, [], ["20240331", "20240630"], [], [], [], "20240101", "20241231")
    assert params == [{"symbol": "000001"}, {"symbol": "600000"}]
    assert len(params) == len(symbols)
    assert len(params) != len(symbols) * 2

    partitions = [_build_raw_partition("corporate_action", "stock_history_dividend_detail", p, p) for p in params]
    keys_first = [_build_task_key("corporate_action", "stock_history_dividend_detail", p) for p in params]
    keys_second = [_build_task_key("corporate_action", "stock_history_dividend_detail", p) for p in params]
    assert partitions == [{"symbol": "000001"}, {"symbol": "600000"}]
    assert keys_first == keys_second
    assert len(set(keys_first)) == len(symbols)


def test_stock_history_dividend_detail_stock_universe_v1_count_is_846() -> None:
    symbols = [line.strip() for line in Path("stock_universe_v1_symbols.txt").read_text(encoding="utf-8").splitlines() if line.strip()]
    spec = next(row for row in COVERAGE_API_SPECS["corporate_action"] if row["api_name"] == "stock_history_dividend_detail")
    params = _params_for_mode(spec["param_mode"], symbols, [], ["20240331", "20240630"], [], [], [], "20240101", "20241231")
    assert len(symbols) == 846
    assert len(params) == 846
