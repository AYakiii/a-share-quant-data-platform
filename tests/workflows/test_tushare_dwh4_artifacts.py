from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_artifacts import (
    write_audit_execution_artifacts,
    write_plan_only_artifacts,
    write_promotion_execution_artifacts,
    write_run_to_prepare_execution_artifacts,
)
from qsys.workflows.tushare_dwh4_commands import build_audit_command, build_promote_command
from qsys.workflows.tushare_dwh4_executor import AuditExecution, PromotionExecution
from qsys.workflows.tushare_dwh4_executor import execute_command_with_runner, RunnerResult
from qsys.workflows.tushare_dwh4_orchestrator import build_run_to_prepare_plan, collect_run_to_prepare_review_decisions
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
        json.dumps({"sources": [_registry_row(api) for api in ("daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange")]}),
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


def test_write_plan_only_artifacts_creates_expected_files(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    decisions = collect_run_to_prepare_review_decisions(task, package_root=tmp_path / "missing_pkg")
    written = write_plan_only_artifacts(task, plan, decisions)
    assert set(written) == {"workflow_state", "planned_commands", "gate_decisions", "agent_report"}
    for path in written.values():
        assert path.exists()
    assert not plan.artifact_paths["commands_executed"].exists()

    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    planned = json.loads(written["planned_commands"].read_text(encoding="utf-8"))
    gates = json.loads(written["gate_decisions"].read_text(encoding="utf-8"))
    report = written["agent_report"].read_text(encoding="utf-8")
    assert state["status"] == "READY_TO_RUN_TO_PREPARE"
    assert state["planned_command_stages"] == ["ingest", "prepare"]
    assert [row["stage"] for row in planned] == ["ingest", "prepare"]
    assert set(gates) == {"review-ingest", "review-compact", "review-promotion"}
    assert "Subprocess executed: no" in report
    dumped = json.dumps({"state": state, "planned": planned, "gates": gates, "report": report})
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_write_plan_only_artifacts_records_blocked_token_without_commands(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={})
    written = write_plan_only_artifacts(task, plan)
    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    planned = json.loads(written["planned_commands"].read_text(encoding="utf-8"))
    assert state["status"] == "BLOCKED_TOKEN"
    assert state["token_present"] is False
    assert planned == []


def _runner_result(return_code: int = 0):
    return lambda _command, *, env: RunnerResult(return_code=return_code, stdout="secret-value", stderr="")


def _clock() -> str:
    return "2026-06-28T00:00:00Z"


def _execution_result(command, *, stage: str = "ingest", return_code: int = 0):
    return execute_command_with_runner(
        command,
        runner=_runner_result(return_code),
        allowed_stages=(stage,),
        token_present=True,
        base_env={"TUSHARE_TOKEN": "secret-value"},
        clock=_clock,
    )


def test_write_execution_artifacts_records_gate_block_without_secrets(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    result = _execution_result(plan.commands[0], stage="ingest")
    decision = ReviewGateDecision(
        stage="review-ingest",
        status="FAIL",
        checked=("fake",),
        issues=(ReviewGateIssue(code="FAKE_GATE_FAIL", field="fake", message="fake failure"),),
        metadata={},
    )
    written = write_run_to_prepare_execution_artifacts(
        task,
        plan,
        (decision,),
        (result,),
        blocked_stage="prepare",
        blocked_reason="review-ingest",
    )
    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    gates = json.loads(written["gate_decisions"].read_text(encoding="utf-8"))
    report = written["agent_report"].read_text(encoding="utf-8")
    lines = written["commands_executed"].read_text(encoding="utf-8").splitlines()
    assert state["blocked_stage"] == "prepare"
    assert state["blocked_reason"] == "review-ingest"
    assert state["gate_decision_stages"] == ["review-ingest"]
    assert state["run_to_prepare_complete"] is False
    assert gates["review-ingest"]["status"] == "FAIL"
    assert "blocked_stage: prepare" in report
    assert state["final_promotion_review_ready"] is False
    assert "final_promotion_review" not in written
    dumped = json.dumps({"state": state, "gates": gates, "lines": lines, "report": report})
    assert "secret-value" not in dumped


def test_write_execution_artifacts_writes_final_promotion_review_on_complete_run(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    results = (
        _execution_result(plan.commands[0], stage="ingest"),
        _execution_result(plan.commands[1], stage="prepare"),
    )
    decisions = (
        ReviewGateDecision(stage="review-ingest", status="PASS", checked=("fake",), metadata={}),
        ReviewGateDecision(stage="review-compact", status="PASS", checked=("fake",), metadata={"package_root": str(tmp_path / "pkg")}),
        ReviewGateDecision(
            stage="review-promotion",
            status="PASS",
            checked=("fake",),
            metadata={
                "package_root": str(tmp_path / "pkg"),
                "planned_copy_new_count": 1,
                "planned_skip_identical_count": 0,
                "planned_block_non_identical_count": 0,
                "review_required_bucket_kinds": ["snapshot"],
            },
        ),
    )
    written = write_run_to_prepare_execution_artifacts(
        task,
        plan,
        decisions,
        results,
        package_root=tmp_path / "pkg",
        python_executable="python-test",
    )
    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    review = written["final_promotion_review"].read_text(encoding="utf-8")
    assert state["run_to_prepare_complete"] is True
    assert state["final_promotion_review_ready"] is True
    assert state["final_promotion_decision"] == "READY FOR PROMOTION"
    assert state["promotion_action_counts"] == {"copy_new": 1}
    assert state["promotion_actions_present"] == ["copy_new"]
    assert state["promotion_command_planned"] is True
    assert state["promotion_command"]["stage"] == "promote"
    assert "--confirm-promotion" in state["promotion_command"]["argv"]
    assert task.promotion_name in state["promotion_command"]["argv"]
    assert "--allow-reviewed-bucket-kinds" in state["promotion_command"]["argv"]
    assert "python-test" == state["promotion_command"]["argv"][0]
    assert "DWH4 Final Promotion Review" in review
    assert "## 0. Decision Summary" in review
    assert "## 0.1 Promotion Action Summary" in review
    assert "**Decision:** READY FOR PROMOTION" in review
    assert "| copy_new | 1 | Drive target missing; copy candidate after confirmation |" in review
    assert "| Drive inventory read | NOT_IMPLEMENTED_IN_I1 |" in review
    assert "| Drive delete executed | NO |" in review
    assert "## 8. Replacement Summary" in review
    assert "## 9. Stable Latest Summary" in review
    assert "## 12. Active Manifest Summary" in review
    assert f"exact_confirmation: {task.promotion_name}" in review
    assert "Promotion executed: no" in review
    dumped = json.dumps(state) + review
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_final_promotion_review_blocks_command_when_delete_request_exists(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    results = (
        _execution_result(plan.commands[0], stage="ingest"),
        _execution_result(plan.commands[1], stage="prepare"),
    )
    decisions = (
        ReviewGateDecision(stage="review-ingest", status="PASS", checked=("fake",), metadata={}),
        ReviewGateDecision(stage="review-compact", status="PASS", checked=("fake",), metadata={"package_root": str(tmp_path / "pkg")}),
        ReviewGateDecision(
            stage="review-promotion",
            status="PASS",
            checked=("fake",),
            metadata={
                "package_root": str(tmp_path / "pkg"),
                "planned_copy_new_count": 1,
                "planned_skip_identical_count": 0,
                "planned_block_non_identical_count": 0,
                "promotion_action_counts": {"copy_new": 1, "delete_request_only": 1},
                "promotion_actions_present": ["copy_new", "delete_request_only"],
                "review_required_bucket_kinds": ["snapshot"],
                "delete_request_generated": True,
            },
        ),
    )
    written = write_run_to_prepare_execution_artifacts(
        task,
        plan,
        decisions,
        results,
        package_root=tmp_path / "pkg",
        python_executable="python-test",
    )
    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    review = written["final_promotion_review"].read_text(encoding="utf-8")
    assert state["final_promotion_review_ready"] is True
    assert state["final_promotion_decision"] == "NEEDS HUMAN DELETE REVIEW"
    assert state["promotion_action_counts"] == {"copy_new": 1, "delete_request_only": 1}
    assert state["promotion_actions_present"] == ["copy_new", "delete_request_only"]
    assert state["promotion_command_planned"] is False
    assert state["promotion_command"] is None
    assert "**Decision:** NEEDS HUMAN DELETE REVIEW" in review
    assert "| delete_request_only | 1 | No delete; requires separate delete review |" in review
    assert "| Drive delete requested | YES |" in review
    assert "promotion confirmation is not delete confirmation" in review
    assert "Promotion command is not planned" in review


def test_final_promotion_review_blocks_command_when_conflict_count_remains(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    results = (
        _execution_result(plan.commands[0], stage="ingest"),
        _execution_result(plan.commands[1], stage="prepare"),
    )
    decisions = (
        ReviewGateDecision(stage="review-ingest", status="PASS", checked=("fake",), metadata={}),
        ReviewGateDecision(stage="review-compact", status="PASS", checked=("fake",), metadata={"package_root": str(tmp_path / "pkg")}),
        ReviewGateDecision(
            stage="review-promotion",
            status="PASS",
            checked=("fake",),
            metadata={
                "package_root": str(tmp_path / "pkg"),
                "planned_copy_new_count": 0,
                "planned_skip_identical_count": 0,
                "planned_block_non_identical_count": 2,
                "collision_rows": 2,
                "review_required_bucket_kinds": ["snapshot"],
            },
        ),
    )
    written = write_run_to_prepare_execution_artifacts(
        task,
        plan,
        decisions,
        results,
        package_root=tmp_path / "pkg",
        python_executable="python-test",
    )
    state = json.loads(written["workflow_state"].read_text(encoding="utf-8"))
    review = written["final_promotion_review"].read_text(encoding="utf-8")
    assert state["final_promotion_decision"] == "BLOCKED"
    assert state["promotion_command_planned"] is False
    assert state["final_promotion_blocking_reasons"] == ["non-identical Drive collisions remain blocked"]
    assert "| Drive collision block | 2 |" in review
    assert "Resolve blocking review findings before promotion" in review


def test_write_promotion_execution_artifacts_records_block_without_command(tmp_path: Path) -> None:
    task = _task(tmp_path)
    execution = PromotionExecution(
        decision=ReviewGateDecision(
            stage="review-promotion",
            status="FAIL",
            checked=("fake",),
            issues=(ReviewGateIssue(code="FAKE_FAIL", field="fake", message="fake failure"),),
            metadata={"package_root": str(tmp_path / "pkg")},
        ),
        result=None,
        blocked_stage="promote",
        blocked_reason="review-promotion",
        promotion_executed=False,
    )
    written = write_promotion_execution_artifacts(task, run_id="run_001", execution=execution)
    state = json.loads(written["promotion_execution_state"].read_text(encoding="utf-8"))
    report = written["promotion_execution_report"].read_text(encoding="utf-8")
    assert state["status"] == "PROMOTION_BLOCKED"
    assert state["promotion_executed"] is False
    assert state["drive_write_executed"] is False
    assert state["blocked_stage"] == "promote"
    assert "commands_executed" not in written
    assert "Promotion subprocess executed: false" in report


def test_write_promotion_execution_artifacts_appends_promote_record_without_secrets(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_promote_command(task, confirm_promotion=task.promotion_name, package_root=tmp_path / "pkg")
    result = execute_command_with_runner(
        command,
        runner=_runner_result(0),
        allowed_stages=("promote",),
        token_present=False,
        base_env={"TUSHARE_TOKEN": "secret-value"},
        clock=_clock,
    )
    execution = PromotionExecution(
        decision=ReviewGateDecision(stage="review-promotion", status="PASS", checked=("fake",), metadata={"package_root": str(tmp_path / "pkg")}),
        result=result,
        blocked_stage=None,
        blocked_reason=None,
        promotion_executed=True,
    )
    written = write_promotion_execution_artifacts(task, run_id="run_001", execution=execution)
    state = json.loads(written["promotion_execution_state"].read_text(encoding="utf-8"))
    lines = written["commands_executed"].read_text(encoding="utf-8").splitlines()
    report = written["promotion_execution_report"].read_text(encoding="utf-8")
    assert state["status"] == "PROMOTION_EXECUTED"
    assert state["promotion_executed"] is True
    assert state["drive_write_executed"] is True
    assert len(lines) == 1
    assert json.loads(lines[0])["stage"] == "promote"
    dumped = json.dumps(state) + "\n".join(lines) + report
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_write_audit_execution_artifacts_appends_audit_record_without_secrets(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_audit_command(task)
    result = execute_command_with_runner(
        command,
        runner=_runner_result(0),
        allowed_stages=("audit",),
        token_present=False,
        base_env={"TUSHARE_TOKEN": "secret-value"},
        clock=_clock,
    )
    execution = AuditExecution(
        result=result,
        blocked_stage=None,
        blocked_reason=None,
        audit_executed=True,
    )
    written = write_audit_execution_artifacts(task, run_id="run_001", execution=execution)
    state = json.loads(written["audit_execution_state"].read_text(encoding="utf-8"))
    lines = written["commands_executed"].read_text(encoding="utf-8").splitlines()
    report = written["audit_execution_report"].read_text(encoding="utf-8")
    assert state["status"] == "AUDIT_EXECUTED"
    assert state["audit_executed"] is True
    assert state["drive_read_executed"] is True
    assert state["drive_write_executed"] is False
    assert len(lines) == 1
    assert json.loads(lines[0])["stage"] == "audit"
    assert "DWH4 Audit Execution Report" in report
    assert "Drive write executed: false" in report
    dumped = json.dumps(state) + "\n".join(lines) + report
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_write_audit_execution_artifacts_records_failed_audit(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_audit_command(task)
    result = execute_command_with_runner(
        command,
        runner=_runner_result(6),
        allowed_stages=("audit",),
        token_present=False,
        base_env={},
        clock=_clock,
    )
    execution = AuditExecution(
        result=result,
        blocked_stage="audit",
        blocked_reason="audit_return_code",
        audit_executed=True,
    )
    written = write_audit_execution_artifacts(task, run_id="run_001", execution=execution)
    state = json.loads(written["audit_execution_state"].read_text(encoding="utf-8"))
    report = written["audit_execution_report"].read_text(encoding="utf-8")
    assert state["status"] == "AUDIT_FAILED"
    assert state["blocked_stage"] == "audit"
    assert "blocked_reason: audit_return_code" in report
