from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_commands import build_audit_command, build_ingest_command, build_promote_command
from qsys.workflows.tushare_dwh4_executor import (
    CommandExecutionBlocked,
    CRITICAL_RESTORE_REQUIRED_STATUS,
    FileProfile,
    RunnerResult,
    VerifiedReplacementSpec,
    append_command_execution_record,
    build_subprocess_env,
    execute_command_with_runner,
    execute_plan_with_runner,
    execute_audit_with_runner,
    execute_promotion_with_review_gate,
    execute_run_to_prepare_with_ingest_gate,
    execute_verified_replacement,
    run_command_subprocess,
)
from qsys.workflows.tushare_dwh4_drive_inventory import file_sha256
from qsys.workflows.tushare_dwh4_orchestrator import build_run_to_prepare_plan
from qsys.workflows.tushare_dwh4_reviews import ReviewGateDecision, ReviewGateIssue
from qsys.workflows.tushare_dwh4_task import task_from_dict


def _registry_row(api_name: str) -> dict[str, object]:
    return {
        "source_family": "fam",
        "api_name": api_name,
        "fields": ["ts_code", "trade_date"],
        "query_mode": "by_trade_date",
        "calendar_mode": "trading_days",
        "partition_key": "trade_date",
        "primary_key": ["ts_code", "trade_date"],
        "universe_filter_mode": "ts_code",
        "empty_result_allowed": False,
        "compact_bucket": "year_from_trade_date",
        "status": "approved",
        "production_enabled": True,
    }


def _write_registry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sources": [
                    _registry_row(api)
                    for api in ("daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange")
                ]
            }
        ),
        encoding="utf-8",
    )


def _payload(tmp_path: Path) -> dict[str, object]:
    execution_repo = tmp_path / "execution_repo"
    (execution_repo / "src").mkdir(parents=True, exist_ok=True)
    _write_registry(execution_repo / "configs" / "tushare" / "source_registry.yaml")
    symbols_file = execution_repo / "stock_universe_v1_symbols.txt"
    symbols_file.write_text("000001\n000002\n", encoding="utf-8")
    return {
        "workflow_name": "tushare_test",
        "workflow_mode": "dwh4_dual_entry_single_core",
        "execution_repo": str(execution_repo),
        "ops_workspace": str(tmp_path / "ops"),
        "provider": "tushare",
        "symbols_file": str(symbols_file),
        "universe_name": "stock_universe_v1",
        "expected_symbol_count": 2,
        "dataset_version": "v1_csi500_2021_2025_union",
        "start_date": "20220101",
        "end_date": "20260601",
        "api_names": ["daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange"],
        "allow_candidate_sources": False,
        "work_name": "tushare_test",
        "output_root": str(tmp_path / "outputs" / "tushare_test"),
        "drive_dwh_root": str(tmp_path / "drive" / "a_share_quant_data"),
        "promotion_name": "tushare_test_compact",
        "execution": {
            "max_workers": 16,
            "request_sleep": 0.2,
            "request_jitter": 0.08,
            "retry": 4,
            "heartbeat_sec": 10,
            "resume": True,
        },
        "repo_policy": {
            "sync_policy": "manual_or_current",
            "require_clean_worktree": False,
            "record_git_commit": True,
        },
        "auto_review_policy": {
            "require_ingest_rough_check_pass": True,
            "require_all_api_rough_check_pass": True,
            "require_zero_bad_status_partitions": True,
            "require_zero_failed_partitions": True,
            "require_zero_disallowed_empty_partitions": True,
            "require_zero_duplicate_partitions": True,
            "require_zero_missing_data_files": True,
            "require_zero_missing_metadata_files": True,
            "require_zero_missing_required_fields": True,
            "require_compact_qa_all_ok": True,
            "require_zero_failed_backlog": True,
            "require_ready_for_promotion": True,
            "block_non_identical_drive_collision": True,
        },
        "promotion_policy": {
            "auto_prepare": True,
            "auto_promote": False,
            "require_final_human_confirmation": True,
            "allow_reviewed_bucket_kinds": ["snapshot"],
        },
        "human_intervention_policy": {
            "only_token_and_final_promotion": True,
            "do_not_pause_for_scope_review": True,
            "do_not_pause_for_ingest_review": True,
            "do_not_pause_for_compact_review": True,
            "on_any_review_failure": "stop_and_write_report",
        },
    }


def _task(tmp_path: Path):
    return task_from_dict(_payload(tmp_path))


def _add_dwh41_policies(payload: dict[str, object]) -> dict[str, object]:
    dataset_version = str(payload["dataset_version"])
    payload["drive_inventory_policy"] = {
        "enabled": True,
        "scan_raw_tushare": True,
        "read_parquet_metadata": True,
        "compute_sha256": True,
        "fail_on_unreadable_existing_asset": True,
    }
    payload["incremental_policy"] = {
        "enabled": True,
        "mode": "drive_aware_incremental",
        "target_end_date_policy": "latest_open_trading_day",
        "as_of_date": "today",
        "data_lag_trading_days": 1,
        "open_year_policy": {
            "enabled": True,
            "replace_current_year_bucket": True,
            "freeze_closed_years": True,
            "overlap_trading_days": 3,
            "clip_overlap_to_open_year": True,
            "block_on_non_identical_key_conflict": True,
            "allow_identical_overlap_collapse": True,
        },
        "stable_latest_policy": {
            "enabled": True,
            "range_apis": ["trade_cal", "namechange"],
            "snapshot_apis": ["stock_basic"],
            "range_bucket": "window=latest",
            "snapshot_bucket": "snapshot=latest",
        },
        "active_manifest_policy": {
            "enabled": True,
            "write_active_manifest": True,
            "active_manifest_path": f"catalog/active/tushare/{dataset_version}/dwh4_tushare_active_manifest.json",
        },
    }
    payload["drive_mutation_policy"] = {
        "allow_delete": False,
        "allow_verified_replace": True,
        "require_final_confirmation_for_replace": True,
        "generate_delete_request_only": True,
        "backup_old_drive_assets_locally_before_replace": True,
    }
    return payload


def _dwh41_task(tmp_path: Path):
    return task_from_dict(_add_dwh41_policies(_payload(tmp_path)))


def _write_profiled_file(path: Path, *, rows: int, columns: tuple[str, ...], marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"rows={rows}\ncolumns={','.join(columns)}\nmarker={marker}\n", encoding="utf-8")


def _text_file_profile(path: Path) -> FileProfile:
    payload: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            payload[key] = value
    columns = tuple(column for column in payload.get("columns", "").split(",") if column)
    return FileProfile(rows=int(payload["rows"]), columns=columns, sha256=file_sha256(path))


class FakeRunner:
    def __init__(self, return_codes: tuple[int, ...] = (0,)) -> None:
        self.return_codes = list(return_codes)
        self.calls: list[dict[str, object]] = []

    def __call__(self, command, *, env):
        self.calls.append({"stage": command.stage, "argv": command.argv, "cwd": command.cwd, "env": dict(env)})
        return_code = self.return_codes.pop(0) if self.return_codes else 0
        return RunnerResult(return_code=return_code, stdout=f"stdout:{command.stage}", stderr=f"stderr:{command.stage}")


class SequenceClock:
    def __init__(self) -> None:
        self.values = [
            "2026-06-28T00:00:00Z",
            "2026-06-28T00:00:01Z",
            "2026-06-28T00:00:02Z",
            "2026-06-28T00:00:03Z",
            "2026-06-28T00:00:04Z",
            "2026-06-28T00:00:05Z",
        ]
        self.index = 0

    def __call__(self) -> str:
        value = self.values[self.index]
        self.index += 1
        return value


def _ingest_gate(status: str) -> ReviewGateDecision:
    issues = () if status == "PASS" else (ReviewGateIssue(code="FAKE_GATE_FAIL", field="fake", message="fake failure"),)
    return ReviewGateDecision(stage="review-ingest", status=status, checked=("fake",), issues=issues, metadata={})


def _package_gate(stage: str, status: str) -> ReviewGateDecision:
    issues = () if status == "PASS" else (ReviewGateIssue(code=f"{stage.upper()}_FAIL", field="fake", message="fake failure"),)
    return ReviewGateDecision(stage=stage, status=status, checked=("fake",), issues=issues, metadata={})


def test_build_subprocess_env_merges_updates_without_mutating_base(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    base_env = {"TUSHARE_TOKEN": "secret-token-value", "PYTHONPATH": "old", "EXISTING": "1"}
    env = build_subprocess_env(command, base_env=base_env)
    assert env["TUSHARE_TOKEN"] == "secret-token-value"
    assert env["PYTHONPATH"] == str(task.execution_repo / "src")
    assert env["EXISTING"] == "1"
    assert base_env == {"TUSHARE_TOKEN": "secret-token-value", "PYTHONPATH": "old", "EXISTING": "1"}


def test_run_command_subprocess_passes_command_to_subprocess_run(tmp_path: Path, monkeypatch) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    calls = []

    class Completed:
        returncode = 3
        stdout = "captured stdout"
        stderr = "captured stderr"

    def fake_run(argv, *, cwd, env, capture_output, text, check):
        calls.append({"argv": argv, "cwd": cwd, "env": dict(env), "capture_output": capture_output, "text": text, "check": check})
        return Completed()

    monkeypatch.setattr("qsys.workflows.tushare_dwh4_executor.subprocess.run", fake_run)
    result = run_command_subprocess(command, env={"TUSHARE_TOKEN": "secret-token-value", "PYTHONPATH": "old"})
    assert result.return_code == 3
    assert result.stdout == "captured stdout"
    assert result.stderr == "captured stderr"
    assert calls[0]["argv"] == list(command.argv)
    assert calls[0]["cwd"] == str(task.execution_repo)
    assert calls[0]["env"]["TUSHARE_TOKEN"] == "secret-token-value"
    assert calls[0]["capture_output"] is True
    assert calls[0]["text"] is True
    assert calls[0]["check"] is False


def test_execute_command_blocks_unallowed_stage_before_runner(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    runner = FakeRunner()
    try:
        execute_command_with_runner(
            command,
            runner=runner,
            allowed_stages=("prepare",),
            token_present=True,
            base_env={"TUSHARE_TOKEN": "secret-token-value"},
            clock=SequenceClock(),
        )
    except CommandExecutionBlocked as exc:
        assert "allowed_stages" in str(exc)
    else:
        raise AssertionError("expected disallowed stage to be blocked")
    assert runner.calls == []


def test_execute_command_with_fake_runner_records_token_free_result(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    runner = FakeRunner()
    result = execute_command_with_runner(
        command,
        runner=runner,
        allowed_stages=("ingest",),
        token_present=True,
        base_env={"TUSHARE_TOKEN": "secret-token-value", "PYTHONPATH": "old"},
        clock=SequenceClock(),
    )
    assert result.stage == "ingest"
    assert result.return_code == 0
    assert result.stdout == "stdout:ingest"
    assert runner.calls[0]["stage"] == "ingest"
    assert runner.calls[0]["cwd"] == task.execution_repo
    assert runner.calls[0]["argv"] == command.argv
    assert runner.calls[0]["env"]["TUSHARE_TOKEN"] == "secret-token-value"
    assert runner.calls[0]["env"]["PYTHONPATH"] == str(task.execution_repo / "src")
    assert result.record["stage"] == "ingest"
    assert result.record["token_present"] is True
    assert result.record["started_at"] == "2026-06-28T00:00:00Z"
    assert result.record["finished_at"] == "2026-06-28T00:00:01Z"
    assert "env" not in result.record
    dumped = json.dumps(result.record)
    assert "TUSHARE_TOKEN" not in dumped
    assert "secret-token-value" not in dumped


def test_execute_plan_refuses_blocked_token_plan(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={})
    runner = FakeRunner()
    try:
        execute_plan_with_runner(plan, runner=runner, base_env={}, clock=SequenceClock())
    except CommandExecutionBlocked as exc:
        assert "not executable" in str(exc)
    else:
        raise AssertionError("expected blocked plan to fail before execution")
    assert runner.calls == []


def test_execute_plan_refuses_design_only_plan_without_runtime_token(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={}, require_runtime_token=False)
    runner = FakeRunner()
    try:
        execute_plan_with_runner(plan, runner=runner, base_env={}, clock=SequenceClock())
    except CommandExecutionBlocked as exc:
        assert "runtime token" in str(exc)
    else:
        raise AssertionError("expected tokenless executable plan to fail before execution")
    assert runner.calls == []


def test_execute_plan_stops_on_nonzero_return_code(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(7, 0))
    results = execute_plan_with_runner(
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
    )
    assert [result.stage for result in results] == ["ingest"]
    assert results[0].return_code == 7
    assert [call["stage"] for call in runner.calls] == ["ingest"]


def test_execute_plan_run_to_prepare_uses_only_ingest_and_prepare(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 0))
    results = execute_plan_with_runner(
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
    )
    assert [result.stage for result in results] == ["ingest", "prepare"]
    assert [call["stage"] for call in runner.calls] == ["ingest", "prepare"]
    assert "promote" not in [call["stage"] for call in runner.calls]


def test_gated_run_to_prepare_runs_prepare_only_after_ingest_gate_pass(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 0))
    review_calls = []

    def review(current_task):
        review_calls.append(current_task.workflow_name)
        return _ingest_gate("PASS")
    package_calls = []

    def compact_review(current_task, *, package_root=None):
        package_calls.append(("compact", current_task.workflow_name, package_root))
        return _package_gate("review-compact", "PASS")

    def promotion_review(current_task, *, package_root=None):
        package_calls.append(("promotion", current_task.workflow_name, package_root))
        return _package_gate("review-promotion", "PASS")

    package_root = tmp_path / "pkg"

    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=review,
        package_root=package_root,
        compact_review=compact_review,
        promotion_review=promotion_review,
    )
    assert [result.stage for result in execution.results] == ["ingest", "prepare"]
    assert [decision.stage for decision in execution.decisions] == ["review-ingest", "review-compact", "review-promotion"]
    assert [decision.status for decision in execution.decisions] == ["PASS", "PASS", "PASS"]
    assert execution.blocked_stage is None
    assert execution.blocked_reason is None
    assert execution.run_to_prepare_complete is True
    assert review_calls == ["tushare_test"]
    assert package_calls == [("compact", "tushare_test", package_root), ("promotion", "tushare_test", package_root)]
    assert [call["stage"] for call in runner.calls] == ["ingest", "prepare"]


def test_gated_run_to_prepare_stops_prepare_when_ingest_gate_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 0))
    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=lambda _task: _ingest_gate("FAIL"),
    )
    assert [result.stage for result in execution.results] == ["ingest"]
    assert [decision.status for decision in execution.decisions] == ["FAIL"]
    assert execution.blocked_stage == "prepare"
    assert execution.blocked_reason == "review-ingest"
    assert execution.run_to_prepare_complete is False
    assert [call["stage"] for call in runner.calls] == ["ingest"]


def test_gated_run_to_prepare_skips_review_when_ingest_command_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(9, 0))
    review_calls = []
    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=lambda _task: review_calls.append("called") or _ingest_gate("PASS"),
    )
    assert [result.stage for result in execution.results] == ["ingest"]
    assert execution.decisions == ()
    assert execution.blocked_stage == "review-ingest"
    assert execution.blocked_reason == "ingest_return_code"
    assert execution.run_to_prepare_complete is False
    assert review_calls == []
    assert [call["stage"] for call in runner.calls] == ["ingest"]


def test_gated_run_to_prepare_stops_when_prepare_command_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 8))
    package_calls = []
    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=lambda _task: _ingest_gate("PASS"),
        compact_review=lambda _task, *, package_root=None: package_calls.append("compact") or _package_gate("review-compact", "PASS"),
        promotion_review=lambda _task, *, package_root=None: package_calls.append("promotion") or _package_gate("review-promotion", "PASS"),
    )
    assert [result.stage for result in execution.results] == ["ingest", "prepare"]
    assert [decision.stage for decision in execution.decisions] == ["review-ingest"]
    assert execution.blocked_stage == "review-compact"
    assert execution.blocked_reason == "prepare_return_code"
    assert execution.run_to_prepare_complete is False
    assert package_calls == []


def test_gated_run_to_prepare_stops_when_compact_gate_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 0))
    promotion_calls = []
    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=lambda _task: _ingest_gate("PASS"),
        compact_review=lambda _task, *, package_root=None: _package_gate("review-compact", "FAIL"),
        promotion_review=lambda _task, *, package_root=None: promotion_calls.append("called") or _package_gate("review-promotion", "PASS"),
    )
    assert [result.stage for result in execution.results] == ["ingest", "prepare"]
    assert [decision.stage for decision in execution.decisions] == ["review-ingest", "review-compact"]
    assert execution.blocked_stage == "review-compact"
    assert execution.blocked_reason == "review-compact"
    assert execution.run_to_prepare_complete is False
    assert promotion_calls == []


def test_gated_run_to_prepare_stops_when_promotion_gate_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-token-value"})
    runner = FakeRunner(return_codes=(0, 0))
    execution = execute_run_to_prepare_with_ingest_gate(
        task,
        plan,
        runner=runner,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        ingest_review=lambda _task: _ingest_gate("PASS"),
        compact_review=lambda _task, *, package_root=None: _package_gate("review-compact", "PASS"),
        promotion_review=lambda _task, *, package_root=None: _package_gate("review-promotion", "FAIL"),
    )
    assert [decision.stage for decision in execution.decisions] == ["review-ingest", "review-compact", "review-promotion"]
    assert execution.blocked_stage == "review-promotion"
    assert execution.blocked_reason == "review-promotion"
    assert execution.run_to_prepare_complete is False


def test_execute_promotion_requires_exact_confirmation_before_review(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner()
    review_calls = []
    try:
        execute_promotion_with_review_gate(
            task,
            runner=runner,
            confirm_promotion="wrong",
            base_env={},
            promotion_review=lambda _task, *, package_root=None: review_calls.append("called") or _package_gate("review-promotion", "PASS"),
        )
    except CommandExecutionBlocked as exc:
        assert "confirm-promotion" in str(exc)
    else:
        raise AssertionError("expected mismatched promotion confirmation to fail")
    assert runner.calls == []
    assert review_calls == []


def test_execute_promotion_blocks_when_readiness_review_fails(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner()
    execution = execute_promotion_with_review_gate(
        task,
        runner=runner,
        confirm_promotion=task.promotion_name,
        base_env={},
        promotion_review=lambda _task, *, package_root=None: _package_gate("review-promotion", "FAIL"),
    )
    assert execution.result is None
    assert execution.decision.status == "FAIL"
    assert execution.blocked_stage == "promote"
    assert execution.blocked_reason == "review-promotion"
    assert execution.promotion_executed is False
    assert runner.calls == []


def test_execute_promotion_runs_promote_after_confirmation_and_review_pass(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner(return_codes=(0,))
    execution = execute_promotion_with_review_gate(
        task,
        runner=runner,
        confirm_promotion=task.promotion_name,
        package_root=tmp_path / "pkg",
        python_executable="python-test",
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
        promotion_review=lambda _task, *, package_root=None: ReviewGateDecision(
            stage="review-promotion",
            status="PASS",
            checked=("fake",),
            metadata={"review_required_bucket_kinds": ["snapshot"], "package_root": str(package_root)},
        ),
        token_present=True,
    )
    assert execution.result is not None
    assert execution.result.stage == "promote"
    assert execution.result.return_code == 0
    assert execution.blocked_stage is None
    assert execution.promotion_executed is True
    assert [call["stage"] for call in runner.calls] == ["promote"]
    assert runner.calls[0]["argv"][0] == "python-test"
    assert "--confirm-promotion" in runner.calls[0]["argv"]
    assert execution.result.record["token_present"] is True


def test_execute_promotion_blocks_delete_request_only_even_when_review_passes(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner()
    execution = execute_promotion_with_review_gate(
        task,
        runner=runner,
        confirm_promotion=task.promotion_name,
        base_env={},
        promotion_review=lambda _task, *, package_root=None: ReviewGateDecision(
            stage="review-promotion",
            status="PASS",
            checked=("fake",),
            metadata={
                "promotion_action_counts": {"delete_request_only": 1},
                "promotion_actions_present": ["delete_request_only"],
                "delete_request_generated": True,
            },
        ),
    )
    assert execution.result is None
    assert execution.blocked_stage == "promote"
    assert execution.blocked_reason == "delete_request_only"
    assert execution.promotion_executed is False
    assert runner.calls == []


def test_execute_audit_runs_read_only_command_without_runtime_token(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner(return_codes=(0,))
    execution = execute_audit_with_runner(
        task,
        runner=runner,
        python_executable="python-test",
        base_env={},
        clock=SequenceClock(),
        token_present=False,
    )
    assert execution.result.stage == "audit"
    assert execution.result.return_code == 0
    assert execution.blocked_stage is None
    assert execution.blocked_reason is None
    assert execution.audit_executed is True
    assert [call["stage"] for call in runner.calls] == ["audit"]
    assert runner.calls[0]["argv"][0] == "python-test"
    assert runner.calls[0]["argv"][4] == "audit"
    assert "TUSHARE_TOKEN" not in runner.calls[0]["env"]
    assert execution.result.record["token_present"] is False


def test_execute_audit_marks_nonzero_return_code_failed(tmp_path: Path) -> None:
    task = _task(tmp_path)
    runner = FakeRunner(return_codes=(5,))
    execution = execute_audit_with_runner(
        task,
        runner=runner,
        base_env={},
        clock=SequenceClock(),
    )
    assert execution.result.stage == "audit"
    assert execution.result.return_code == 5
    assert execution.blocked_stage == "audit"
    assert execution.blocked_reason == "audit_return_code"
    assert execution.audit_executed is True


def test_promote_command_is_not_allowed_by_default_executor_stages(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_promote_command(task, confirm_promotion=task.promotion_name)
    runner = FakeRunner()
    try:
        execute_command_with_runner(
            command,
            runner=runner,
            allowed_stages=("ingest", "prepare"),
            token_present=True,
            base_env={"TUSHARE_TOKEN": "secret-token-value"},
            clock=SequenceClock(),
        )
    except CommandExecutionBlocked as exc:
        assert "promote" in str(exc)
    else:
        raise AssertionError("expected promote to be blocked")
    assert runner.calls == []


def test_verified_replacement_creates_local_backup_and_replaces_drive_file(tmp_path: Path) -> None:
    task = _dwh41_task(tmp_path)
    relative = "raw/tushare/fam/daily_basic/v1_csi500_2021_2025_union/year=2026/data.parquet"
    drive_path = task.drive_dwh_root / Path(*relative.split("/"))
    candidate_path = tmp_path / "candidate" / "data.parquet"
    columns = ("ts_code", "trade_date")
    _write_profiled_file(drive_path, rows=1, columns=columns, marker="old")
    _write_profiled_file(candidate_path, rows=2, columns=columns, marker="candidate")
    expected_sha = file_sha256(candidate_path)

    execution = execute_verified_replacement(
        task,
        VerifiedReplacementSpec(
            action="replace_verified_incremental",
            drive_relative_path=relative,
            candidate_path=candidate_path,
            expected_rows=2,
            expected_columns=columns,
            expected_sha256=expected_sha,
            reason="verified open-year replacement",
        ),
        run_id="run_001",
        confirm_promotion=task.promotion_name,
        file_profiler=_text_file_profile,
        clock=SequenceClock(),
    )

    assert execution.passed is True
    assert execution.status == "PASS"
    assert execution.drive_write_executed is True
    assert execution.drive_delete_executed is False
    assert execution.auto_rollback_executed is False
    assert execution.critical_restore_path is None
    assert drive_path.read_text(encoding="utf-8") == candidate_path.read_text(encoding="utf-8")
    assert execution.backup_path.read_text(encoding="utf-8").endswith("marker=old\n")
    metadata = json.loads(execution.backup_metadata_path.read_text(encoding="utf-8"))
    assert metadata["drive_path"] == str(drive_path.resolve())
    assert metadata["backup_path"] == str(execution.backup_path)
    assert metadata["old_rows"] == 1
    assert metadata["backup_local_only"] is True
    assert metadata["drive_delete_executed"] is False
    dumped = json.dumps(execution.payload())
    assert "secret-token-value" not in dumped


def test_verified_replacement_blocks_unapproved_action_before_backup_or_write(tmp_path: Path) -> None:
    task = _dwh41_task(tmp_path)
    relative = "raw/tushare/fam/daily_basic/v1_csi500_2021_2025_union/year=2026/data.parquet"
    drive_path = task.drive_dwh_root / Path(*relative.split("/"))
    candidate_path = tmp_path / "candidate" / "data.parquet"
    columns = ("ts_code", "trade_date")
    _write_profiled_file(drive_path, rows=1, columns=columns, marker="old")
    _write_profiled_file(candidate_path, rows=2, columns=columns, marker="candidate")

    try:
        execute_verified_replacement(
            task,
            VerifiedReplacementSpec(
                action="copy_new",
                drive_relative_path=relative,
                candidate_path=candidate_path,
                expected_rows=2,
                expected_columns=columns,
                expected_sha256=file_sha256(candidate_path),
            ),
            run_id="run_001",
            confirm_promotion=task.promotion_name,
            file_profiler=_text_file_profile,
        )
    except CommandExecutionBlocked as exc:
        assert "replacement action" in str(exc)
    else:
        raise AssertionError("expected unapproved replacement action to be blocked")
    assert drive_path.read_text(encoding="utf-8").endswith("marker=old\n")
    assert not (task.ops_workspace / "runs" / task.workflow_name / "run_001" / "drive_backups").exists()


def test_verified_replacement_blocks_candidate_mismatch_before_drive_write(tmp_path: Path) -> None:
    task = _dwh41_task(tmp_path)
    relative = "raw/tushare/fam/daily_basic/v1_csi500_2021_2025_union/year=2026/data.parquet"
    drive_path = task.drive_dwh_root / Path(*relative.split("/"))
    candidate_path = tmp_path / "candidate" / "data.parquet"
    columns = ("ts_code", "trade_date")
    _write_profiled_file(drive_path, rows=1, columns=columns, marker="old")
    _write_profiled_file(candidate_path, rows=2, columns=columns, marker="candidate")

    try:
        execute_verified_replacement(
            task,
            VerifiedReplacementSpec(
                action="replace_verified_incremental",
                drive_relative_path=relative,
                candidate_path=candidate_path,
                expected_rows=3,
                expected_columns=columns,
                expected_sha256=file_sha256(candidate_path),
            ),
            run_id="run_001",
            confirm_promotion=task.promotion_name,
            file_profiler=_text_file_profile,
        )
    except CommandExecutionBlocked as exc:
        assert "candidate verification failed" in str(exc)
    else:
        raise AssertionError("expected candidate mismatch to be blocked")
    assert drive_path.read_text(encoding="utf-8").endswith("marker=old\n")
    assert not (task.ops_workspace / "runs" / task.workflow_name / "run_001" / "drive_backups").exists()


def test_verified_replacement_writes_restore_required_on_post_write_failure_without_rollback(tmp_path: Path) -> None:
    task = _dwh41_task(tmp_path)
    relative = "raw/tushare/fam/daily_basic/v1_csi500_2021_2025_union/year=2026/data.parquet"
    drive_path = task.drive_dwh_root / Path(*relative.split("/"))
    candidate_path = tmp_path / "candidate" / "data.parquet"
    columns = ("ts_code", "trade_date")
    _write_profiled_file(drive_path, rows=1, columns=columns, marker="old")
    _write_profiled_file(candidate_path, rows=2, columns=columns, marker="candidate")
    expected_sha = file_sha256(candidate_path)
    calls: dict[str, int] = {"drive": 0}

    def flaky_post_write_profile(path: Path) -> FileProfile:
        if path.resolve() == drive_path.resolve():
            calls["drive"] += 1
            if calls["drive"] == 1:
                return _text_file_profile(path)
            return FileProfile(rows=999, columns=columns, sha256=expected_sha)
        return _text_file_profile(path)

    execution = execute_verified_replacement(
        task,
        VerifiedReplacementSpec(
            action="replace_verified_latest",
            drive_relative_path=relative,
            candidate_path=candidate_path,
            expected_rows=2,
            expected_columns=columns,
            expected_sha256=expected_sha,
        ),
        run_id="run_001",
        confirm_promotion=task.promotion_name,
        file_profiler=flaky_post_write_profile,
        clock=SequenceClock(),
    )

    assert execution.status == CRITICAL_RESTORE_REQUIRED_STATUS
    assert execution.passed is False
    assert execution.drive_write_executed is True
    assert execution.drive_delete_executed is False
    assert execution.auto_rollback_executed is False
    assert execution.reason == "post_write_verification_failed:rows"
    assert execution.critical_restore_path is not None
    restore = execution.critical_restore_path.read_text(encoding="utf-8")
    assert "CRITICAL_RESTORE_REQUIRED" in restore
    assert "No automatic rollback was performed." in restore
    assert "failed_checks: rows" in restore
    assert execution.backup_path.read_text(encoding="utf-8").endswith("marker=old\n")
    assert drive_path.read_text(encoding="utf-8") == candidate_path.read_text(encoding="utf-8")


def test_append_command_execution_record_writes_token_free_jsonl(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    result = execute_command_with_runner(
        command,
        runner=FakeRunner(),
        allowed_stages=("ingest",),
        token_present=True,
        base_env={"TUSHARE_TOKEN": "secret-token-value"},
        clock=SequenceClock(),
    )
    path = append_command_execution_record(tmp_path / "commands_executed.jsonl", result.record)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    loaded = json.loads(lines[0])
    assert loaded["stage"] == "ingest"
    assert loaded["return_code"] == 0
    assert "stdout" not in loaded
    assert "stderr" not in loaded
    assert "env" not in loaded
    assert "TUSHARE_TOKEN" not in lines[0]
    assert "secret-token-value" not in lines[0]
