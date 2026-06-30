from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_orchestrator import (
    BLOCKED_TOKEN,
    BLOCKED_VALIDATION,
    DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE,
    READY_TO_RUN_TO_PREPARE,
    build_run_to_prepare_plan,
    collect_run_to_prepare_review_decisions,
    gate_decisions_payload,
    run_to_prepare_stage_sequence,
    planned_commands_payload,
    workflow_state_payload,
)
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


def _task(tmp_path: Path):
    return task_from_dict(_payload(tmp_path))


def test_plan_ready_when_validation_passes_and_token_present(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    assert plan.status == READY_TO_RUN_TO_PREPARE
    assert plan.ready is True
    assert plan.token_present is True
    assert [stage.stage for stage in plan.stages] == ["validate", "ingest", "review-ingest", "prepare", "review-compact", "review-promotion"]
    assert [stage.status for stage in plan.stages] == ["PASS", "PENDING", "PENDING", "PENDING", "PENDING", "PENDING"]
    assert [command.stage for command in plan.commands] == ["ingest", "prepare"]
    assert all("promote" not in command.argv for command in plan.commands)


def test_dwh41_run_to_prepare_v2_sequence_is_integrated_but_non_promoting(tmp_path: Path) -> None:
    task = task_from_dict(_add_dwh41_policies(_payload(tmp_path)))
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    state = workflow_state_payload(plan)
    assert run_to_prepare_stage_sequence(task) == DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE
    assert plan.stage_sequence == DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE
    assert [stage.stage for stage in plan.stages] == list(DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE)
    assert [stage.kind for stage in plan.stages] == [
        "in_process",
        "in_process",
        "in_process",
        "command",
        "review",
        "in_process",
        "command",
        "review",
        "review",
        "in_process",
    ]
    assert [command.stage for command in plan.commands] == ["ingest", "prepare"]
    assert all("promote" not in command.argv for command in plan.commands)
    assert state["run_to_prepare_v2"] is True
    assert state["stage_sequence"] == list(DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE)
    assert state["planned_command_stages"] == ["ingest", "prepare"]
    assert state["promotion_executed"] is False
    assert state["drive_write_executed"] is False
    assert state["drive_delete_executed"] is False


def test_plan_blocks_when_token_missing_by_default(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={})
    assert plan.status == BLOCKED_TOKEN
    assert plan.ready is False
    assert plan.commands == ()
    assert [issue.code for issue in plan.validation_issues] == ["TOKEN_NOT_PRESENT"]
    state = workflow_state_payload(plan)
    assert state["token_present"] is False
    assert state["planned_command_stages"] == []
    dumped = json.dumps(state)
    assert "secret-value" not in dumped


def test_plan_can_skip_runtime_token_check_for_design_only(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={}, require_runtime_token=False)
    assert plan.status == READY_TO_RUN_TO_PREPARE
    assert plan.token_present is False
    assert [command.stage for command in plan.commands] == ["ingest", "prepare"]


def test_plan_blocks_on_validation_errors(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["api_names"] = ["no_such_api"]
    task = task_from_dict(payload)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    assert plan.status == BLOCKED_VALIDATION
    assert plan.commands == ()
    assert "API_NAME_UNKNOWN" in {issue.code for issue in plan.validation_issues}
    assert [stage.status for stage in plan.stages] == ["FAIL", "NOT_PLANNED", "NOT_PLANNED", "NOT_PLANNED", "NOT_PLANNED", "NOT_PLANNED"]


def test_workflow_state_and_command_payloads_are_token_free(tmp_path: Path) -> None:
    task = _task(tmp_path)
    plan = build_run_to_prepare_plan(task, run_id="run_001", env={"TUSHARE_TOKEN": "secret-value"})
    state = workflow_state_payload(plan)
    commands = planned_commands_payload(plan)
    assert state["promotion_executed"] is False
    assert state["drive_write_executed"] is False
    assert state["artifact_paths"]["commands_executed"].endswith("commands_executed.jsonl")
    assert commands[0]["stage"] == "ingest"
    assert commands[0]["env_update_keys"] == ["PYTHONPATH"]
    dumped = json.dumps({"state": state, "commands": commands})
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_collect_review_decisions_is_read_only_and_reports_missing_artifacts(tmp_path: Path) -> None:
    task = _task(tmp_path)
    decisions = collect_run_to_prepare_review_decisions(task, package_root=tmp_path / "missing_pkg")
    assert [decision.stage for decision in decisions] == ["review-ingest", "review-compact", "review-promotion"]
    assert [decision.status for decision in decisions] == ["FAIL", "FAIL", "FAIL"]
    payload = gate_decisions_payload(decisions)
    assert set(payload) == {"review-ingest", "review-compact", "review-promotion"}
    assert payload["review-ingest"]["issues"][0]["code"] == "ARTIFACT_MISSING"
    assert not (tmp_path / "missing_pkg").exists()
