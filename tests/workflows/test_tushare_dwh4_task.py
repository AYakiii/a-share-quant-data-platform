from __future__ import annotations

import json
from pathlib import Path

import pytest

from qsys.workflows.tushare_dwh4_task import (
    assert_dwh4_tushare_task_valid,
    load_dwh4_tushare_task,
    runtime_token_present,
    task_from_dict,
    validate_dwh4_tushare_task,
)


def _write_registry(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_family": "market_basic",
                        "api_name": "daily_basic",
                        "fields": ["ts_code", "trade_date", "total_mv"],
                        "query_mode": "by_trade_date",
                        "calendar_mode": "trading_days",
                        "partition_key": "trade_date",
                        "primary_key": ["ts_code", "trade_date"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": False,
                        "compact_bucket": "year_from_trade_date",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "market_limit",
                        "api_name": "stk_limit",
                        "fields": ["ts_code", "trade_date", "up_limit", "down_limit"],
                        "query_mode": "by_trade_date",
                        "calendar_mode": "trading_days",
                        "partition_key": "trade_date",
                        "primary_key": ["ts_code", "trade_date"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": False,
                        "compact_bucket": "year_from_trade_date",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "market_tradability",
                        "api_name": "suspend_d",
                        "fields": ["ts_code", "trade_date", "suspend_timing", "suspend_type"],
                        "query_mode": "by_trade_date",
                        "calendar_mode": "trading_days",
                        "partition_key": "trade_date",
                        "primary_key": ["ts_code", "trade_date", "suspend_timing", "suspend_type"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": True,
                        "compact_bucket": "year_from_trade_date",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "market_calendar",
                        "api_name": "trade_cal",
                        "fields": ["exchange", "cal_date", "is_open"],
                        "query_mode": "by_date_range",
                        "calendar_mode": "range_once",
                        "partition_key": "start_date",
                        "partition_keys": ["exchange", "start_date", "end_date"],
                        "range_start_param": "start_date",
                        "range_end_param": "end_date",
                        "param_grid": {"exchange": ["SSE", "SZSE"]},
                        "primary_key": ["exchange", "cal_date"],
                        "universe_filter_mode": "none",
                        "empty_result_allowed": False,
                        "compact_bucket": "window_from_range",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "security_master",
                        "api_name": "stock_basic",
                        "fields": ["ts_code", "symbol", "name", "list_status"],
                        "query_mode": "snapshot_by_param",
                        "calendar_mode": "snapshot",
                        "partition_key": "snapshot",
                        "partition_keys": ["snapshot", "list_status"],
                        "param_grid": {"list_status": ["L", "D", "P"]},
                        "primary_key": ["ts_code", "list_status"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": False,
                        "compact_bucket": "snapshot",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "security_master",
                        "api_name": "namechange",
                        "fields": ["ts_code", "name", "start_date", "end_date", "change_reason", "ann_date"],
                        "query_mode": "by_date_range",
                        "calendar_mode": "range_once",
                        "partition_key": "start_date",
                        "partition_keys": ["start_date", "end_date"],
                        "range_start_param": "start_date",
                        "range_end_param": "end_date",
                        "primary_key": ["ts_code", "name", "start_date", "end_date", "change_reason", "ann_date"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": True,
                        "compact_bucket": "window_from_range",
                        "status": "approved",
                        "production_enabled": True,
                    },
                    {
                        "source_family": "market_limit",
                        "api_name": "limit_list_d",
                        "fields": ["trade_date", "ts_code"],
                        "query_mode": "by_trade_date",
                        "calendar_mode": "trading_days",
                        "partition_key": "trade_date",
                        "primary_key": ["ts_code", "trade_date"],
                        "universe_filter_mode": "ts_code",
                        "empty_result_allowed": True,
                        "compact_bucket": "year_from_trade_date",
                        "status": "manual_review",
                        "production_enabled": False,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _base_payload(tmp_path: Path) -> dict[str, object]:
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
        "execution": {"max_workers": 16, "request_sleep": 0.2, "request_jitter": 0.08, "retry": 4, "heartbeat_sec": 10, "resume": True},
        "repo_policy": {"sync_policy": "manual_or_current", "require_clean_worktree": False, "record_git_commit": True},
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
        "promotion_policy": {"auto_prepare": True, "auto_promote": False, "require_final_human_confirmation": True, "allow_reviewed_bucket_kinds": ["snapshot"]},
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


def _codes(payload: dict[str, object]) -> set[str]:
    return {issue.code for issue in validate_dwh4_tushare_task(task_from_dict(payload))}


def test_load_external_task_json_and_validate(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    task_path = tmp_path / "task.json"
    task_path.write_text(json.dumps(payload), encoding="utf-8")
    task = load_dwh4_tushare_task(task_path)
    assert task.workflow_name == "tushare_test"
    assert task.api_names == ("daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange")
    assert validate_dwh4_tushare_task(task) == []
    assert_dwh4_tushare_task_valid(task)


def test_dwh41_policy_fields_load_and_validate(tmp_path: Path) -> None:
    payload = _add_dwh41_policies(_base_payload(tmp_path))
    task = task_from_dict(payload)
    assert task.drive_inventory_policy is not None
    assert task.drive_inventory_policy.enabled is True
    assert task.incremental_policy is not None
    assert task.incremental_policy.mode == "drive_aware_incremental"
    assert task.incremental_policy.open_year_policy.overlap_trading_days == 3
    assert task.incremental_policy.stable_latest_policy.range_apis == ("trade_cal", "namechange")
    assert task.incremental_policy.stable_latest_policy.snapshot_apis == ("stock_basic",)
    assert task.incremental_policy.active_manifest_policy.active_manifest_path.endswith("dwh4_tushare_active_manifest.json")
    assert task.drive_mutation_policy is not None
    assert task.drive_mutation_policy.allow_delete is False
    assert validate_dwh4_tushare_task(task) == []


def test_dwh41_policy_rejects_delete_bad_manifest_and_unknown_stable_api(tmp_path: Path) -> None:
    payload = _add_dwh41_policies(_base_payload(tmp_path))
    payload["drive_mutation_policy"]["allow_delete"] = True
    payload["incremental_policy"]["active_manifest_policy"]["active_manifest_path"] = "../bad.json"
    payload["incremental_policy"]["stable_latest_policy"]["range_apis"] = ["trade_cal", "no_such_api"]
    codes = _codes(payload)
    assert {"DWH41_DELETE_NOT_ALLOWED", "DWH41_ACTIVE_MANIFEST_PATH_INVALID", "DWH41_STABLE_LATEST_API_NOT_IN_TASK"} <= codes


def test_secret_like_keys_rejected_except_policy_allowlist(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    assert task_from_dict(payload).secret_like_key_paths == ()
    payload["tushare_token"] = "do-not-store"
    task = task_from_dict(payload)
    assert task.secret_like_key_paths == ("tushare_token",)
    assert "SECRET_LIKE_KEY_REJECTED" in {issue.code for issue in validate_dwh4_tushare_task(task)}


def test_rejects_drive_like_output_root(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["output_root"] = "C:/example/Google Drive/a_share_quant_data/output"
    issues = validate_dwh4_tushare_task(task_from_dict(payload))
    assert [issue.code for issue in issues] == ["OUTPUT_ROOT_DRIVE_LIKE"]


def test_rejects_bad_dates_and_unsafe_dataset_version(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["start_date"] = "20260632"
    payload["end_date"] = "20200101"
    payload["dataset_version"] = "../bad"
    codes = _codes(payload)
    assert "DATE_FORMAT_INVALID" in codes
    assert "DATASET_VERSION_INVALID" in codes
    payload = _base_payload(tmp_path)
    payload["start_date"] = "20260602"
    payload["end_date"] = "20260601"
    assert "DATE_RANGE_INVALID" in _codes(payload)


def test_symbol_count_mismatch_fails(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["expected_symbol_count"] = 3
    assert "EXPECTED_SYMBOL_COUNT_MISMATCH" in _codes(payload)


def test_missing_execution_src_fails(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    src = Path(str(payload["execution_repo"])) / "src"
    src.rmdir()
    issues = validate_dwh4_tushare_task(task_from_dict(payload))
    assert [issue.code for issue in issues] == ["EXECUTION_REPO_SRC_MISSING"]


def test_unknown_api_fails(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["api_names"] = ["no_such_api"]
    assert "API_NAME_UNKNOWN" in _codes(payload)


def test_candidate_api_requires_explicit_policy(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    payload["api_names"] = ["limit_list_d"]
    assert "API_NOT_PRODUCTION_ENABLED" in _codes(payload)
    payload["allow_candidate_sources"] = True
    assert "API_NOT_PRODUCTION_ENABLED" not in _codes(payload)


def test_runtime_token_presence_is_boolean_only() -> None:
    assert runtime_token_present({"TUSHARE_TOKEN": "abc"}) is True
    assert runtime_token_present({}) is False


def test_optional_drive_root_check(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path)
    task = task_from_dict(payload)
    assert "DRIVE_DWH_ROOT_MISSING" in {issue.code for issue in validate_dwh4_tushare_task(task, check_drive_root=True)}
    task.drive_dwh_root.mkdir(parents=True)
    assert "DRIVE_DWH_ROOT_MISSING" not in {issue.code for issue in validate_dwh4_tushare_task(task, check_drive_root=True)}


def test_runtime_token_warning_is_opt_in(tmp_path: Path) -> None:
    task = task_from_dict(_base_payload(tmp_path))
    assert "TOKEN_NOT_PRESENT" not in {issue.code for issue in validate_dwh4_tushare_task(task, env={})}
    issues = validate_dwh4_tushare_task(task, check_runtime_token=True, env={})
    assert [(issue.severity, issue.code) for issue in issues if issue.code == "TOKEN_NOT_PRESENT"] == [("WARNING", "TOKEN_NOT_PRESENT")]
