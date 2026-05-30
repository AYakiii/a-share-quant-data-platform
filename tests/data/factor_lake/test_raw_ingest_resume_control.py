from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake import raw_ingest
from qsys.data.factor_lake.raw_ingest import API_POLICY_METADATA, run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_partition_aware_resume_preserves_audit_and_recovers_queue(tmp_path: Path) -> None:
    calls: list[str] = []

    def first_adapter(date: str) -> _Result:
        calls.append(date)
        if date == "20240630":
            raise ConnectionError("RemoteDisconnected")
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    first = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331", "20240630"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": first_adapter},
        max_workers=1,
        continue_on_error=True,
    )
    assert calls == ["20240331", "20240630"]
    first_catalog = pd.read_csv(first["catalog_path"])
    assert set(first_catalog["status"]) == {"success", "failed"}
    assert "task_key_json" in first_catalog.columns
    assert first_catalog["task_key_json"].nunique() == 2

    events_path = tmp_path / "_operation_review" / "task_events.jsonl"
    attempts_path = tmp_path / "_operation_review" / "task_attempts.csv"
    first_event_count = len(events_path.read_text(encoding="utf-8").splitlines())
    first_attempts = pd.read_csv(attempts_path)
    assert first_event_count == 2
    assert len(first_attempts) == 2
    assert first_attempts["run_id"].notna().all()
    assert "task_key_json" in first_attempts.columns

    calls.clear()

    def recovery_adapter(date: str) -> _Result:
        calls.append(date)
        return _Result(pd.DataFrame({"date": [date], "value": [2]}))

    second = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331", "20240630"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": recovery_adapter},
        max_workers=1,
        continue_on_error=True,
        resume=True,
    )
    assert calls == ["20240630"]

    merged = pd.read_csv(second["catalog_path"])
    assert len(merged) == 2
    assert set(merged["status"]) == {"success"}
    assert merged["task_key_json"].nunique() == 2
    assert set(merged["partition_json"].map(lambda raw: json.loads(raw)["date"])) == {"20240331", "20240630"}

    recovery = pd.read_csv(tmp_path / "_operation_review" / "recovery_tasks.csv")
    assert recovery.empty
    expected_recovery_columns = {
        "source_family",
        "api_name",
        "requested_api_name",
        "original_symbol",
        "akshare_symbol",
        "status",
        "error_type",
        "error_message",
        "partition_json",
        "params_json",
        "task_key_json",
    }
    assert expected_recovery_columns <= set(recovery.columns)

    assert len(events_path.read_text(encoding="utf-8").splitlines()) == first_event_count + 1
    attempts = pd.read_csv(attempts_path)
    assert len(attempts) == len(first_attempts) + 1
    assert attempts["run_id"].notna().all()
    assert attempts["task_key_json"].notna().all()


def test_heavy_sources_are_manual_selected_only_and_policy_is_truthful(tmp_path: Path) -> None:
    class _FakeJgdyResponse:
        def json(self) -> dict:
            return {
                "result": {
                    "pages": 1,
                    "data": [
                        {
                            "SECURITY_CODE": "000001",
                            "SECURITY_NAME_ABBR": "平安银行",
                            "NOTICE_DATE": "2024-12-31",
                            "RECEIVE_START_DATE": "2024-12-31",
                        }
                    ],
                }
            }

    def stock_jgdy_detail_page_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        return _FakeJgdyResponse()

    def stock_gdfx_holding_analyse_em(date: str) -> _Result:  # noqa: ARG001
        return _Result(pd.DataFrame({"x": [2]}))

    default = run_raw_coverage_ingest(
        output_root=str(tmp_path / "default"),
        families=["disclosure_ir", "event_ownership"],
        symbols=["000001"],
        report_dates=["20241231"],
        adapter_map={
            "__stock_jgdy_detail_em_request_get__": stock_jgdy_detail_page_get,
            "__stock_jgdy_detail_em_config__": {"retry_attempts": 1, "retry_sleep_sec": 0.0, "request_sleep_sec": 0.0},
            "stock_gdfx_holding_analyse_em": stock_gdfx_holding_analyse_em,
        },
        max_workers=1,
    )
    heavy_default = [
        row
        for row in default["rows"]
        if (row["source_family"], row["api_name"]) in {
            ("disclosure_ir", "stock_jgdy_detail_em"),
            ("event_ownership", "stock_gdfx_holding_analyse_em"),
        }
    ]
    assert len(heavy_default) == 2
    assert {row["status"] for row in heavy_default} == {"skipped"}
    assert {row["error_type"] for row in heavy_default} == {"default_disabled"}

    selected = run_raw_coverage_ingest(
        output_root=str(tmp_path / "selected"),
        families=["disclosure_ir", "event_ownership"],
        report_dates=["20241231"],
        selected_api_names=["stock_jgdy_detail_em", "stock_gdfx_holding_analyse_em", "stock_gdfx_free_holding_analyse_em"],
        adapter_map={
            "__stock_jgdy_detail_em_request_get__": stock_jgdy_detail_page_get,
            "__stock_jgdy_detail_em_config__": {"retry_attempts": 1, "retry_sleep_sec": 0.0, "request_sleep_sec": 0.0},
            "stock_gdfx_holding_analyse_em": stock_gdfx_holding_analyse_em,
            "stock_gdfx_free_holding_analyse_em": lambda date: _Result(pd.DataFrame({"x": [3]})),
        },
        max_workers=1,
    )
    status_by_api = {row["api_name"]: row["status"] for row in selected["rows"]}
    error_by_api = {row["api_name"]: row["error_type"] for row in selected["rows"]}
    assert status_by_api["stock_jgdy_detail_em"] == "success"
    assert status_by_api["stock_gdfx_holding_analyse_em"] == "success"
    assert status_by_api["stock_gdfx_free_holding_analyse_em"] == "skipped"
    assert error_by_api["stock_gdfx_free_holding_analyse_em"] == "default_disabled"

    holding_policy = API_POLICY_METADATA[("event_ownership", "stock_gdfx_holding_analyse_em")]
    assert holding_policy["enabled"] is False
    assert holding_policy["default_enabled"] is False
    assert holding_policy["manual_review_required"] is True
    assert holding_policy["importance"] == "high"
    assert holding_policy["acquisition_mode"] == "long_recovery_run"
    assert holding_policy["disabled_category"] == "recovered_heavy_source"
    assert "123,880" in str(holding_policy["disabled_reason"])
    assert "857" in str(holding_policy["disabled_reason"])



def test_single_worker_subprocess_heartbeat_and_timeout(capsys, tmp_path: Path) -> None:
    def slow_adapter(date: str) -> _Result:  # noqa: ARG001
        time.sleep(0.5)
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240930"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": slow_adapter},
        max_workers=1,
        task_timeout_sec=0.2,
        heartbeat_sec=0.05,
    )

    captured = capsys.readouterr().out
    assert "[heartbeat]" in captured
    assert "source_family=financial_fundamental" in captured
    assert "api_name=stock_yjyg_em" in captured
    assert "pending_or_running_tasks=1" in captured
    [row] = out["rows"]
    assert row["status"] == "timeout"
    assert row["error_type"] == "TimeoutError"


class _NoPayloadQueue:
    def empty(self) -> bool:
        return True


class _NoPayloadProcess:
    def __init__(self, target, args):  # noqa: ANN001, ARG002
        self._alive = False

    def start(self) -> None:
        self._alive = False

    def join(self, timeout=None) -> None:  # noqa: ANN001, ARG002
        self._alive = False

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self._alive = False


class _NoPayloadContext:
    def Queue(self) -> _NoPayloadQueue:  # noqa: N802
        return _NoPayloadQueue()

    def Process(self, target, args):  # noqa: ANN001, N802
        return _NoPayloadProcess(target, args)


def test_subprocess_no_result_enters_recovery_and_resume_recovers(monkeypatch, tmp_path: Path) -> None:
    with monkeypatch.context() as m:
        m.setattr(raw_ingest.mp, "get_context", lambda method: _NoPayloadContext())
        first = run_raw_coverage_ingest(
            output_root=str(tmp_path),
            families=["financial_fundamental"],
            report_dates=["20240930"],
            selected_api_names=["stock_yjyg_em"],
            adapter_map={"stock_yjyg_em": lambda date: _Result(pd.DataFrame({"date": [date]}))},
            max_workers=1,
            task_timeout_sec=1.0,
        )

    catalog = pd.read_csv(first["catalog_path"])
    assert len(catalog) == 1
    row = catalog.iloc[0]
    assert row["status"] == "failed"
    assert row["error_type"] == "NoResult"
    assert row["error_message"] == "subprocess exited without queue payload"
    assert isinstance(row["task_key_json"], str) and row["task_key_json"]
    assert json.loads(row["partition_json"]) == {"date": "20240930"}
    assert json.loads(row["params_json"]) == {"date": "20240930"}

    recovery = pd.read_csv(tmp_path / "_operation_review" / "recovery_tasks.csv")
    assert len(recovery) == 1
    assert recovery.iloc[0]["error_type"] == "NoResult"
    assert recovery.iloc[0]["task_key_json"] == row["task_key_json"]

    calls: list[str] = []

    def recovery_adapter(date: str) -> _Result:
        calls.append(date)
        return _Result(pd.DataFrame({"date": [date], "value": [1]}))

    second = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240930"],
        selected_api_names=["stock_yjyg_em"],
        adapter_map={"stock_yjyg_em": recovery_adapter},
        max_workers=1,
        resume=True,
    )

    assert calls == ["20240930"]
    merged = pd.read_csv(second["catalog_path"])
    assert len(merged) == 1
    assert set(merged["status"]) == {"success"}
    assert merged.iloc[0]["task_key_json"] == row["task_key_json"]
    recovered_queue = pd.read_csv(tmp_path / "_operation_review" / "recovery_tasks.csv")
    assert recovered_queue.empty
