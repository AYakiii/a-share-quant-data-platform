from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_notebook import (
    load_dwh4_tushare_notebook_globals,
    load_dwh4_tushare_notebook_parameters,
)


def _write_registry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_family": "fam",
                        "api_name": api,
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
                    for api in ("daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange")
                ]
            }
        ),
        encoding="utf-8",
    )


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


def _task_json(tmp_path: Path, *, dwh41: bool = False) -> Path:
    execution_repo = tmp_path / "execution_repo"
    (execution_repo / "src").mkdir(parents=True, exist_ok=True)
    _write_registry(execution_repo / "configs" / "tushare" / "source_registry.yaml")
    symbols_file = execution_repo / "stock_universe_v1_symbols.txt"
    symbols_file.write_text("000001\n000002\n", encoding="utf-8")
    payload = {
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
    if dwh41:
        payload = _add_dwh41_policies(payload)
    task_path = tmp_path / "task.json"
    task_path.write_text(json.dumps(payload), encoding="utf-8")
    return task_path


def test_load_notebook_globals_from_shared_task_json(tmp_path: Path) -> None:
    task_path = _task_json(tmp_path)
    values = load_dwh4_tushare_notebook_globals(task_path, env={"TUSHARE_TOKEN": "secret-value"})
    execution_repo = tmp_path / "execution_repo"
    assert values["REPO_ROOT"] == execution_repo
    assert values["EXECUTION_REPO"] == execution_repo
    assert values["SYMBOLS_FILE"] == execution_repo / "stock_universe_v1_symbols.txt"
    assert values["UNIVERSE_NAME"] == "stock_universe_v1"
    assert values["EXPECTED_SYMBOL_COUNT"] == 2
    assert values["DATASET_VERSION"] == "v1_csi500_2021_2025_union"
    assert values["START_DATE"] == "20220101"
    assert values["END_DATE"] == "20260601"
    assert values["API_NAMES"] == "daily_basic,stk_limit,suspend_d,trade_cal,stock_basic,namechange"
    assert values["API_NAME_LIST"] == ["daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange"]
    assert values["OUTPUT_ROOT"] == tmp_path / "outputs" / "tushare_test"
    assert values["DRIVE_DWH_ROOT"] == tmp_path / "drive" / "a_share_quant_data"
    assert values["PROMOTION_NAME"] == "tushare_test_compact"
    assert values["COMPACT_PARENT"] == execution_repo / "outputs" / "raw_acquisition_compact"
    assert values["PACKAGE_ROOT"] == execution_repo / "outputs" / "raw_acquisition_compact" / "tushare_test_compact"
    assert values["MAX_WORKERS"] == 16
    assert values["REQUEST_SLEEP"] == 0.2
    assert values["REQUEST_JITTER"] == 0.08
    assert values["RETRY"] == 4
    assert values["HEARTBEAT_SEC"] == 10
    assert values["RESUME"] is True
    assert values["RUNTIME_TOKEN_PRESENT"] is True
    assert "secret-value" not in repr(values)


def test_notebook_parameter_summary_is_token_free(tmp_path: Path) -> None:
    task_path = _task_json(tmp_path)
    params = load_dwh4_tushare_notebook_parameters(task_path, env={"TUSHARE_TOKEN": "secret-value"})
    summary = params.as_summary()
    assert summary["workflow_name"] == "tushare_test"
    assert summary["api_count"] == 6
    assert summary["runtime_token_present"] is True
    assert "secret-value" not in json.dumps(summary)


def test_notebook_adapter_exposes_dwh41_policy_fields_without_secrets(tmp_path: Path) -> None:
    task_path = _task_json(tmp_path, dwh41=True)
    params = load_dwh4_tushare_notebook_parameters(task_path, env={"TUSHARE_TOKEN": "secret-value"})
    values = params.as_notebook_globals()
    summary = params.as_summary()
    manifest_path = "catalog/active/tushare/v1_csi500_2021_2025_union/dwh4_tushare_active_manifest.json"

    assert values["DWH41_INCREMENTAL_ENABLED"] is True
    assert values["DWH41_INCREMENTAL_MODE"] == "drive_aware_incremental"
    assert values["DWH41_TARGET_END_DATE_POLICY"] == "latest_open_trading_day"
    assert values["DWH41_ACTIVE_MANIFEST_PATH"] == manifest_path
    assert values["DWH41_ALLOW_DELETE"] is False
    assert values["DWH41_ALLOW_VERIFIED_REPLACE"] is True
    assert values["DWH41_INCREMENTAL_POLICY"]["stable_latest_range_apis"] == ["trade_cal", "namechange"]
    assert values["DWH41_INCREMENTAL_POLICY"]["stable_latest_snapshot_apis"] == ["stock_basic"]
    assert values["DWH41_INCREMENTAL_POLICY"]["open_year_overlap_trading_days"] == 3
    assert values["DWH41_DRIVE_POLICY"]["drive_inventory_compute_sha256"] is True
    assert values["DWH41_DRIVE_POLICY"]["generate_delete_request_only"] is True

    assert summary["dwh41_incremental_enabled"] is True
    assert summary["dwh41_incremental_mode"] == "drive_aware_incremental"
    assert summary["dwh41_active_manifest_path"] == manifest_path
    assert summary["dwh41_allow_delete"] is False
    assert summary["dwh41_allow_verified_replace"] is True
    dumped = json.dumps({"values": values, "summary": summary}, default=str)
    assert "secret-value" not in dumped
