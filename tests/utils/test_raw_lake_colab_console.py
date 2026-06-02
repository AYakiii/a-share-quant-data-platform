from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest

from qsys.utils import raw_lake_colab_console as console
from qsys.utils.raw_lake_colab_console import RawLakeConsoleConfig


def _namespace(tmp_path: Path, **overrides):
    ns = {
        "PROJECT_ROOT": tmp_path / "project",
        "SYMBOLS_FILE": tmp_path / "symbols.txt",
        "OUTPUT_ROOT": tmp_path / "out",
        "START_DATE": "20200101",
        "END_DATE": "20200131",
        "REPORT_DATES": "20200331",
        "LANES": "main",
        "MAX_WORKERS": 2,
        "HEAVY_MAX_WORKERS": 1,
        "LONG_RUN_MAX_WORKERS": 1,
        "DEFERRED_MAX_WORKERS": 1,
        "HEARTBEAT_SEC": 30,
        "SYMBOL_BATCH_SIZE": 10,
        "MAX_INFLIGHT_TASKS": 12,
        "TASK_TIMEOUT_SEC": 120,
        "MANUAL_SELECTED_TASK_TIMEOUT_SEC": 180,
        "HEAVY_TASK_TIMEOUT_SEC": 300,
        "LONG_RUN_TASK_TIMEOUT_SEC": 600,
        "DEFERRED_TASK_TIMEOUT_SEC": 300,
        "REQUEST_SLEEP": 0.1,
        "HEAVY_REQUEST_SLEEP": 0.2,
        "LONG_RUN_REQUEST_SLEEP": 0.5,
        "TASK_RETRY_ATTEMPTS": 2,
        "TASK_RETRY_SLEEP_SEC": 0,
        "TASK_RETRY_BACKOFF": 1,
        "TASK_RETRY_JITTER_SEC": 0,
        "ONLY_FAMILIES": "",
        "EXCLUDE_FAMILIES": "",
        "ONLY_APIS": "",
        "EXCLUDE_APIS": "",
        "RESUME": False,
        "RESET_OUTPUT": False,
        "REFRESH_UNIVERSE": False,
        "DRY_RUN": False,
    }
    ns.update(overrides)
    return ns


def test_config_reads_existing_notebook_variables_and_lanes_pass_through(tmp_path):
    config = RawLakeConsoleConfig.from_namespace(_namespace(tmp_path, LANES="main,heavy"))
    assert config.output_root == tmp_path / "out"
    assert config.lanes == "main,heavy"
    assert config.max_inflight_tasks == 12


def test_missing_required_variable_raises_clear_error(tmp_path):
    ns = _namespace(tmp_path)
    ns.pop("MAX_WORKERS")
    with pytest.raises(KeyError, match="MAX_WORKERS"):
        RawLakeConsoleConfig.from_namespace(ns)


def test_resume_and_reset_output_conflict_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="RESUME and RESET_OUTPUT"):
        RawLakeConsoleConfig.from_namespace(_namespace(tmp_path, RESUME=True, RESET_OUTPUT=True))


def test_drive_like_output_root_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="Drive-like"):
        RawLakeConsoleConfig.from_namespace(_namespace(tmp_path, OUTPUT_ROOT="/content/drive/MyDrive/raw"))


def test_manual_selected_is_never_silently_appended(tmp_path):
    config = RawLakeConsoleConfig.from_namespace(_namespace(tmp_path, LANES="main"))
    cmd = console.build_preheat_command(config)
    assert cmd[cmd.index("--lanes") + 1] == "main"
    assert "manual_selected" not in cmd[cmd.index("--lanes") + 1]


def test_optional_cli_filters_omitted_when_empty(tmp_path):
    config = RawLakeConsoleConfig.from_namespace(_namespace(tmp_path))
    cmd = console.build_preheat_command(config)
    assert "--only-families" not in cmd
    assert "--exclude-families" not in cmd
    assert "--only-apis" not in cmd
    assert "--exclude-apis" not in cmd


def test_boolean_flags_emitted_only_when_enabled(tmp_path):
    disabled = console.build_preheat_command(RawLakeConsoleConfig.from_namespace(_namespace(tmp_path)))
    assert "--resume" not in disabled
    assert "--refresh-universe" not in disabled
    assert "--dry-run" not in disabled

    enabled = console.build_preheat_command(
        RawLakeConsoleConfig.from_namespace(_namespace(tmp_path, RESUME=True, REFRESH_UNIVERSE=True, DRY_RUN=True))
    )
    assert "--resume" in enabled
    assert "--refresh-universe" in enabled
    assert "--dry-run" in enabled


def test_optional_cli_filters_appended_when_non_empty(tmp_path):
    config = RawLakeConsoleConfig.from_namespace(
        _namespace(tmp_path, ONLY_FAMILIES="market_price", EXCLUDE_FAMILIES="x", ONLY_APIS="a", EXCLUDE_APIS="b")
    )
    cmd = console.build_preheat_command(config)
    assert cmd[cmd.index("--only-families") + 1] == "market_price"
    assert cmd[cmd.index("--exclude-families") + 1] == "x"
    assert cmd[cmd.index("--only-apis") + 1] == "a"
    assert cmd[cmd.index("--exclude-apis") + 1] == "b"


def test_safe_json_reader_tolerates_missing_and_partial_file(tmp_path):
    path = tmp_path / "live_progress.json"
    assert console.read_json_safe(path) is None
    path.write_text('{"event":', encoding="utf-8")
    assert console.read_json_safe(path) is None
    path.write_text('{"event": "heartbeat"}', encoding="utf-8")
    assert console.read_json_safe(path) == {"event": "heartbeat"}


def test_duration_and_progress_formatters():
    assert console.format_duration(None) == "-"
    assert console.format_duration(65) == "1m 05s"
    assert console.format_duration(3661) == "1h 01m 01s"
    assert console.progress_bar(5, 10, width=10) == "[█████░░░░░]  50.00%"
    assert console.progress_bar("bad", 10, width=4) == "[░░░░]   0.00%"


def test_dashboard_rendering_includes_expected_fields_and_log_path(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(console, "clear_output", lambda wait=True: None)
    log_path = tmp_path / "raw_lake_preheat_console.log"
    payload = {
        "lane": "main",
        "event": "heartbeat",
        "completed_tasks": 3,
        "total_tasks": 10,
        "pending_or_running_tasks": 7,
        "success_tasks": 1,
        "empty_tasks": 2,
        "failed_tasks": 0,
        "timeout_tasks": 0,
        "already_exists_tasks": 1,
        "skipped_tasks": 0,
        "pending_adapter_tasks": 0,
        "current_batch_id": 0,
        "total_batches": 2,
        "current_batch_scope": "flat",
        "current_batch_completed_tasks": 3,
        "current_batch_task_count": 10,
        "completed_batches": 0,
        "elapsed_sec": 65,
        "throughput_tasks_per_min": 12.5,
        "eta_sec": 30,
    }
    console.render_dashboard(payload, "RUNNING", log_path)
    out = capsys.readouterr().out
    for expected in ["RAW LAKE PREHEAT DASHBOARD", "state:", "lane:", "event:", "completed:", "remaining:", "success:", "batch:", "scope:", "elapsed:", "throughput:", "eta:", str(log_path)]:
        assert expected in out


def test_dashboard_waiting_first_heartbeat_state_and_log_path(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(console, "clear_output", lambda wait=True: None)
    log_path = tmp_path / "log.txt"
    console.render_dashboard(None, "RUNNING", log_path)
    out = capsys.readouterr().out
    assert "preflight / waiting for first heartbeat" in out
    assert str(log_path) in out


class _FinishedProcess:
    returncode = 1

    def poll(self):
        return self.returncode


def test_non_zero_subprocess_result_prints_bounded_log_tail(tmp_path, capsys, monkeypatch):
    config = RawLakeConsoleConfig.from_namespace(_namespace(tmp_path))
    (config.project_root).mkdir(parents=True)
    op_root = config.output_root / "_operation_review"
    op_root.mkdir(parents=True)

    def fake_popen(*args, **kwargs):  # noqa: ARG001
        log_file = kwargs["stdout"]
        for i in range(200):
            log_file.write(f"line {i}\n")
        log_file.flush()
        return _FinishedProcess()

    monkeypatch.setattr(console.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(console, "clear_output", lambda wait=True: None)
    with pytest.raises(subprocess.CalledProcessError):
        console.run_preheat_with_compact_dashboard(config, refresh_sec=0, pre_run_pause_sec=0)
    out = capsys.readouterr().out
    assert "=== LOG TAIL ===" in out
    assert "line 199" in out
    assert "line 0" not in out


def test_concise_review_reads_source_concurrency_observation(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(console, "display", lambda value: None)
    root = tmp_path / "out"
    op = root / "_operation_review"
    (op / "hybrid_batches" / "main").mkdir(parents=True)
    pd.DataFrame({"task_key_json": ["a"], "partition_json": ["p"], "status": ["success"]}).to_csv(root / "raw_ingest_catalog.csv", index=False)
    pd.DataFrame({"status": []}).to_csv(op / "recovery_tasks.csv", index=False)
    pd.DataFrame({"status": ["completed"]}).to_csv(op / "hybrid_batches" / "main" / "hybrid_batch_report.csv", index=False)
    (op / "hybrid_batches" / "main" / "hybrid_batch_checkpoint.json").write_text('{"fingerprint_payload":{"lane_name":"main"}}', encoding="utf-8")
    pd.DataFrame(
        {
            "source_key": ["eastmoney"],
            "workload_class": ["symbol_scoped"],
            "peak_inflight_tasks": [2],
            "latency_sample_count": [1],
            "median_task_elapsed_sec": [0.5],
            "p90_task_elapsed_sec": [0.9],
        }
    ).to_csv(op / "source_concurrency_observation.csv", index=False)
    result = console.review_preheat_output(root, lane_name="main")
    out = capsys.readouterr().out
    assert "clean batches / total:" in out
    assert "completed / total:" not in out
    assert result["catalog_rows"] == 1
    assert result["completed_batches"] == 1
    assert not result["source_concurrency_observation"].empty


def test_concise_review_rejects_duplicated_logical_partitions(tmp_path, monkeypatch):
    monkeypatch.setattr(console, "display", lambda value: None)
    root = tmp_path / "out"
    root.mkdir()
    pd.DataFrame({"task_key_json": ["a", "a"], "partition_json": ["p", "p"], "status": ["success", "success"]}).to_csv(
        root / "raw_ingest_catalog.csv", index=False
    )
    with pytest.raises(AssertionError, match="duplicated"):
        console.review_preheat_output(root)


def test_failure_audit_writes_only_audit_csv_files(tmp_path, monkeypatch):
    monkeypatch.setattr(console, "display", lambda value: None)
    root = tmp_path / "out"
    root.mkdir()
    pd.DataFrame(
        {
            "source_family": ["fam", "fam"],
            "api_name": ["api", "api"],
            "status": ["failed", "success"],
            "rows": [0, 1],
            "error_type": ["ValueError", ""],
            "error_message": ["bad", ""],
            "original_symbol": ["000046", "000002"],
            "akshare_symbol": ["SZ000046", "SZ000002"],
            "params_json": ["{}", "{}"],
            "partition_json": ["{}", "{}"],
            "task_key_json": ["a", "b"],
            "elapsed_sec": [1.0, 2.0],
            "output_path": ["", "x"],
        }
    ).to_csv(root / "raw_ingest_catalog.csv", index=False)
    displayed = []
    monkeypatch.setattr(console, "display", lambda value: displayed.append(value))
    paths = console.audit_preheat_failures(root, display_limit=1)
    audit_root = root / "_operation_review" / "failure_audit"
    failure_detail = pd.read_csv(audit_root / "failure_detail.csv", dtype={"original_symbol": str, "akshare_symbol": str})
    api_summary = pd.read_csv(audit_root / "failure_summary_by_api.csv")
    displayed_detail = next(value for value in displayed if isinstance(value, pd.DataFrame) and "original_symbol" in value.columns)
    assert displayed_detail.iloc[0]["original_symbol"] == "000046"
    assert displayed_detail.iloc[0]["akshare_symbol"] == "SZ000046"
    assert failure_detail.iloc[0]["original_symbol"] == "000046"
    assert failure_detail.iloc[0]["akshare_symbol"] == "SZ000046"
    assert float(api_summary.loc[0, "rows"]) == 1.0
    assert set(p.name for p in audit_root.iterdir()) == {
        "failure_summary_by_api.csv",
        "failure_detail.csv",
        "failure_summary_by_error_type.csv",
        "high_failure_api_shortlist.csv",
    }
    assert all(path.parent == audit_root for path in paths.values())


def test_clear_local_output_requires_confirm_and_rejects_drive_like_paths(tmp_path):
    with pytest.raises(ValueError, match="confirm=True"):
        console.clear_local_output(tmp_path / "out", confirm=False)
    with pytest.raises(ValueError, match="Drive-like"):
        console.clear_local_output("/content/drive/MyDrive/raw", confirm=True)


def test_clear_local_output_deletes_only_explicit_output_root(tmp_path):
    root = tmp_path / "out"
    sibling = tmp_path / "sibling"
    root.mkdir()
    sibling.mkdir()
    (root / "x.txt").write_text("x", encoding="utf-8")
    console.clear_local_output(root, confirm=True)
    assert not root.exists()
    assert sibling.exists()
