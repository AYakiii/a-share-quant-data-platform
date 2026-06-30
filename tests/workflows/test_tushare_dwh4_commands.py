from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_commands import (
    build_audit_command,
    build_ingest_command,
    build_prepare_command,
    build_promote_command,
    build_run_to_prepare_commands,
    command_execution_record,
    default_package_root,
    run_artifact_paths,
)
from qsys.workflows.tushare_dwh4_task import task_from_dict


def _payload(tmp_path: Path) -> dict[str, object]:
    execution_repo = tmp_path / "execution_repo"
    return {
        "workflow_name": "tushare_test",
        "workflow_mode": "dwh4_dual_entry_single_core",
        "execution_repo": str(execution_repo),
        "ops_workspace": str(tmp_path / "ops"),
        "provider": "tushare",
        "symbols_file": str(execution_repo / "stock_universe_v1_symbols.txt"),
        "universe_name": "stock_universe_v1",
        "expected_symbol_count": 846,
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


def _value_after(argv: tuple[str, ...], flag: str) -> str:
    return argv[argv.index(flag) + 1]


def test_ingest_command_uses_existing_runner_and_task_args(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_ingest_command(task)
    assert command.stage == "ingest"
    assert command.cwd == task.execution_repo
    assert command.argv[:4] == ("python", "-u", "-m", "qsys.utils.run_tushare_raw_ingest")
    assert _value_after(command.argv, "--symbols-file") == str(task.symbols_file)
    assert _value_after(command.argv, "--universe-name") == "stock_universe_v1"
    assert _value_after(command.argv, "--dataset-version") == "v1_csi500_2021_2025_union"
    assert _value_after(command.argv, "--start-date") == "20220101"
    assert _value_after(command.argv, "--end-date") == "20260601"
    assert _value_after(command.argv, "--api-names") == "daily_basic,stk_limit,suspend_d,trade_cal,stock_basic,namechange"
    assert _value_after(command.argv, "--expected-symbol-count") == "846"
    assert "--resume" in command.argv
    assert "--allow-candidate-sources" not in command.argv
    assert command.env_update_mapping()["PYTHONPATH"] == str(task.execution_repo / "src")


def test_candidate_source_flag_is_explicit(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["allow_candidate_sources"] = True
    task = task_from_dict(payload)
    assert "--allow-candidate-sources" in build_ingest_command(task).argv


def test_prepare_command_uses_existing_compact_prepare(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_prepare_command(task)
    assert command.stage == "prepare"
    assert command.argv[:5] == ("python", "-u", "-m", "qsys.utils.raw_lake_compact_cli", "prepare")
    assert _value_after(command.argv, "--provider") == "tushare"
    assert _value_after(command.argv, "--dataset-version") == task.dataset_version
    assert _value_after(command.argv, "--output-root") == str(task.output_root)
    assert _value_after(command.argv, "--drive-dwh-root") == str(task.drive_dwh_root)
    assert _value_after(command.argv, "--promotion-name") == task.promotion_name
    assert "--replace-local-package" in command.argv


def test_promote_command_requires_exact_confirmation(tmp_path: Path) -> None:
    task = _task(tmp_path)
    try:
        build_promote_command(task, confirm_promotion="wrong")
    except ValueError as exc:
        assert "confirm-promotion" in str(exc)
    else:
        raise AssertionError("expected mismatched confirmation to fail")


def test_promote_command_adds_authorized_reviewed_bucket_kinds(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_promote_command(
        task,
        confirm_promotion=task.promotion_name,
        required_reviewed_bucket_kinds=("snapshot",),
    )
    assert command.stage == "promote"
    assert command.argv[:5] == ("python", "-u", "-m", "qsys.utils.raw_lake_compact_cli", "promote")
    assert _value_after(command.argv, "--package-root") == str(default_package_root(task))
    assert _value_after(command.argv, "--confirm-promotion") == task.promotion_name
    assert _value_after(command.argv, "--allow-reviewed-bucket-kinds") == "snapshot"


def test_promote_command_blocks_unauthorized_reviewed_bucket_kind(tmp_path: Path) -> None:
    task = _task(tmp_path)
    try:
        build_promote_command(
            task,
            confirm_promotion=task.promotion_name,
            required_reviewed_bucket_kinds=("scope",),
        )
    except ValueError as exc:
        assert "not authorized" in str(exc)
    else:
        raise AssertionError("expected unauthorized reviewed bucket kind to fail")


def test_audit_command_is_read_only_cli_shape(tmp_path: Path) -> None:
    task = _task(tmp_path)
    command = build_audit_command(task)
    assert command.stage == "audit"
    assert command.argv[:5] == ("python", "-u", "-m", "qsys.utils.raw_lake_compact_cli", "audit")
    assert _value_after(command.argv, "--promotion-name") == task.promotion_name
    assert _value_after(command.argv, "--drive-dwh-root") == str(task.drive_dwh_root)


def test_run_to_prepare_never_emits_promote(tmp_path: Path) -> None:
    commands = build_run_to_prepare_commands(_task(tmp_path))
    assert [command.stage for command in commands] == ["ingest", "prepare"]
    assert all("promote" not in command.argv for command in commands)


def test_run_artifact_paths_are_under_ops_workspace(tmp_path: Path) -> None:
    task = _task(tmp_path)
    paths = run_artifact_paths(task, "run_001")
    assert paths["workflow_state"] == task.ops_workspace / "runs" / task.workflow_name / "run_001" / "workflow_state.json"
    assert paths["commands_executed"].name == "commands_executed.jsonl"
    assert paths["final_promotion_review"].name == "final_promotion_review.md"
    assert paths["drive_inventory"].name == "drive_inventory.csv"
    assert paths["incremental_plan"].name == "incremental_plan.csv"
    assert paths["incremental_merge_report"].name == "incremental_merge_report.csv"
    assert paths["candidate_active_manifest"].name == "candidate_active_manifest.json"
    assert paths["stable_latest_report"].name == "stable_latest_report.csv"
    assert paths["drive_delete_request"].name == "DRIVE_DELETE_REQUEST.md"
    assert paths["drive_delete_plan"].name == "drive_delete_plan.csv"
    assert paths["drive_delete_summary"].name == "drive_delete_summary.json"
    assert paths["audit_execution_state"].name == "audit_execution_state.json"
    assert paths["audit_execution_report"].name == "audit_execution_report.md"
    try:
        run_artifact_paths(task, "../bad")
    except ValueError:
        pass
    else:
        raise AssertionError("expected unsafe run_id to fail")


def test_command_execution_record_is_token_free(tmp_path: Path) -> None:
    command = build_ingest_command(_task(tmp_path))
    record = command_execution_record(
        command,
        started_at="2026-06-28T00:00:00Z",
        finished_at="2026-06-28T00:00:01Z",
        return_code=0,
        token_present=True,
    )
    assert record["stage"] == "ingest"
    assert record["return_code"] == 0
    assert record["token_present"] is True
    assert "env_updates" not in record
    assert "TUSHARE_TOKEN" not in json.dumps(record)
    assert "secret-token-value" not in json.dumps(record)
