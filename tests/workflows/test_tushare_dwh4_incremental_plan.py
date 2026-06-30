from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_drive_inventory import DriveInventoryAsset, DriveInventoryResult
from qsys.workflows.tushare_dwh4_incremental_plan import build_incremental_plan, write_incremental_plan_artifacts
from qsys.workflows.tushare_dwh4_task import task_from_dict


DATASET_VERSION = "v1_csi500_2021_2025_union"


def _registry_row(
    *,
    source_family: str,
    api_name: str,
    fields: list[str],
    query_mode: str,
    calendar_mode: str,
    partition_key: str,
    primary_key: list[str],
    compact_bucket: str,
    partition_keys: list[str] | None = None,
    range_start_param: str | None = None,
    range_end_param: str | None = None,
    param_grid: dict[str, list[str]] | None = None,
    universe_filter_mode: str = "ts_code",
    empty_result_allowed: bool = False,
) -> dict[str, object]:
    row: dict[str, object] = {
        "source_family": source_family,
        "api_name": api_name,
        "fields": fields,
        "query_mode": query_mode,
        "calendar_mode": calendar_mode,
        "partition_key": partition_key,
        "primary_key": primary_key,
        "universe_filter_mode": universe_filter_mode,
        "empty_result_allowed": empty_result_allowed,
        "compact_bucket": compact_bucket,
        "status": "approved",
        "production_enabled": True,
    }
    if partition_keys is not None:
        row["partition_keys"] = partition_keys
    if range_start_param is not None:
        row["range_start_param"] = range_start_param
    if range_end_param is not None:
        row["range_end_param"] = range_end_param
    if param_grid is not None:
        row["param_grid"] = param_grid
    return row


def _write_registry(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sources": [
                    _registry_row(
                        source_family="market_basic",
                        api_name="daily_basic",
                        fields=["ts_code", "trade_date", "total_mv"],
                        query_mode="by_trade_date",
                        calendar_mode="trading_days",
                        partition_key="trade_date",
                        primary_key=["ts_code", "trade_date"],
                        compact_bucket="year_from_trade_date",
                    ),
                    _registry_row(
                        source_family="market_calendar",
                        api_name="trade_cal",
                        fields=["exchange", "cal_date", "is_open"],
                        query_mode="by_date_range",
                        calendar_mode="range_once",
                        partition_key="start_date",
                        partition_keys=["exchange", "start_date", "end_date"],
                        range_start_param="start_date",
                        range_end_param="end_date",
                        param_grid={"exchange": ["SSE", "SZSE"]},
                        primary_key=["exchange", "cal_date"],
                        compact_bucket="window_from_range",
                        universe_filter_mode="none",
                    ),
                    _registry_row(
                        source_family="security_master",
                        api_name="stock_basic",
                        fields=["ts_code", "symbol", "name", "list_status"],
                        query_mode="snapshot_by_param",
                        calendar_mode="snapshot",
                        partition_key="snapshot",
                        partition_keys=["snapshot", "list_status"],
                        param_grid={"list_status": ["L", "D", "P"]},
                        primary_key=["ts_code", "list_status"],
                        compact_bucket="snapshot",
                    ),
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _payload(tmp_path: Path, *, api_names: list[str] | None = None) -> dict[str, object]:
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
        "dataset_version": DATASET_VERSION,
        "start_date": "20220101",
        "end_date": "20260630",
        "api_names": api_names or ["daily_basic", "trade_cal", "stock_basic"],
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
        "drive_inventory_policy": {
            "enabled": True,
            "scan_raw_tushare": True,
            "read_parquet_metadata": True,
            "compute_sha256": True,
            "fail_on_unreadable_existing_asset": True,
        },
        "incremental_policy": {
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
                "range_apis": ["trade_cal"],
                "snapshot_apis": ["stock_basic"],
                "range_bucket": "window=latest",
                "snapshot_bucket": "snapshot=latest",
            },
            "active_manifest_policy": {
                "enabled": True,
                "write_active_manifest": True,
                "active_manifest_path": f"catalog/active/tushare/{DATASET_VERSION}/dwh4_tushare_active_manifest.json",
            },
        },
        "drive_mutation_policy": {
            "allow_delete": False,
            "allow_verified_replace": True,
            "require_final_confirmation_for_replace": True,
            "generate_delete_request_only": True,
            "backup_old_drive_assets_locally_before_replace": True,
        },
    }


def _task(tmp_path: Path, *, api_names: list[str] | None = None):
    return task_from_dict(_payload(tmp_path, api_names=api_names))


def _asset(
    tmp_path: Path,
    *,
    source_family: str = "market_basic",
    api_name: str = "daily_basic",
    bucket_kind: str = "year",
    bucket_value: str = "2026",
    rows: int = 10,
    min_date: str = "20260101",
    max_date: str = "20260618",
    snapshot_date: str = "",
) -> DriveInventoryAsset:
    relative_path = f"raw/tushare/{source_family}/{api_name}/{DATASET_VERSION}/{bucket_kind}={bucket_value}/data.parquet"
    return DriveInventoryAsset(
        drive_dwh_root=str(tmp_path / "drive" / "a_share_quant_data"),
        path=str(tmp_path / "drive" / "a_share_quant_data" / Path(*relative_path.split("/"))),
        relative_path=relative_path,
        provider="tushare",
        source_family=source_family,
        api_name=api_name,
        dataset_version=DATASET_VERSION,
        bucket_kind=bucket_kind,
        bucket_value=bucket_value,
        partitions={bucket_kind: bucket_value},
        rows=rows,
        columns=("ts_code", "trade_date"),
        column_count=2,
        sha256="",
        size_bytes=123,
        date_column="trade_date",
        min_date=min_date,
        max_date=max_date,
        snapshot_date=snapshot_date,
        metadata_exists=False,
        metadata_keys=(),
        status="ok",
    )


def _inventory(tmp_path: Path, assets: tuple[DriveInventoryAsset, ...]) -> DriveInventoryResult:
    return DriveInventoryResult(
        drive_dwh_root=str(tmp_path / "drive" / "a_share_quant_data"),
        dataset_version=DATASET_VERSION,
        assets=assets,
        summary={"asset_count": len(assets)},
        active_manifest=None,
    )


def _row(result, api_name: str):
    return next(row for row in result.rows if row.api_name == api_name)


def test_by_trade_date_uses_drive_max_date_overlap_and_target_lag(tmp_path: Path) -> None:
    task = _task(tmp_path, api_names=["daily_basic"])
    result = build_incremental_plan(
        task,
        _inventory(tmp_path, (_asset(tmp_path, max_date="20260618"),)),
        latest_open_trading_day="20260626",
    )

    row = _row(result, "daily_basic")
    assert result.target_end_date == "20260625"
    assert row.fetch_start_date == "20260615"
    assert row.fetch_end_date == "20260625"
    assert row.target_bucket_kind == "year"
    assert row.target_bucket_value == "2026"
    assert row.existing_max_date == "20260618"
    assert row.existing_rows == 10
    assert row.planned_action == "replace_verified_incremental_candidate"
    assert result.summary["incremental_merge_executed"] is False
    assert result.summary["drive_write_executed"] is False
    assert result.summary["drive_delete_executed"] is False


def test_open_year_overlap_is_clipped_to_year_start(tmp_path: Path) -> None:
    task = _task(tmp_path, api_names=["daily_basic"])
    result = build_incremental_plan(
        task,
        _inventory(tmp_path, (_asset(tmp_path, min_date="20260101", max_date="20260102"),)),
        latest_open_trading_day="20260109",
    )

    row = _row(result, "daily_basic")
    assert result.target_end_date == "20260108"
    assert row.fetch_start_date == "20260101"
    assert row.fetch_end_date == "20260108"
    assert row.reason == "current open-year bucket found; overlap applied"


def test_no_drive_data_falls_back_to_task_start_date(tmp_path: Path) -> None:
    task = _task(tmp_path, api_names=["daily_basic"])
    result = build_incremental_plan(task, _inventory(tmp_path, ()), latest_open_trading_day="20260626")

    row = _row(result, "daily_basic")
    assert row.fetch_start_date == task.start_date
    assert row.fetch_end_date == "20260625"
    assert row.existing_relative_path == ""
    assert row.existing_max_date == ""
    assert row.planned_action == "copy_new_open_year"
    assert row.reason == "no current open-year Drive bucket"


def test_closed_year_assets_do_not_replace_open_year_full_history(tmp_path: Path) -> None:
    task = _task(tmp_path, api_names=["daily_basic"])
    result = build_incremental_plan(
        task,
        _inventory(tmp_path, (_asset(tmp_path, bucket_value="2025", min_date="20250101", max_date="20251231"),)),
        latest_open_trading_day="20260626",
    )

    row = _row(result, "daily_basic")
    assert row.fetch_start_date == "20260101"
    assert row.target_bucket_value == "2026"
    assert row.existing_relative_path == ""
    assert result.summary["closed_year_asset_count"] == 1


def test_stable_latest_range_and_snapshot_plans_use_latest_buckets(tmp_path: Path) -> None:
    task = _task(tmp_path)
    trade_cal = _asset(
        tmp_path,
        source_family="market_calendar",
        api_name="trade_cal",
        bucket_kind="window",
        bucket_value="latest",
        rows=2,
        min_date="20220101",
        max_date="20260610",
    )
    stock_basic = _asset(
        tmp_path,
        source_family="security_master",
        api_name="stock_basic",
        bucket_kind="snapshot",
        bucket_value="latest",
        rows=3,
        min_date="20260610",
        max_date="20260610",
        snapshot_date="20260610",
    )

    result = build_incremental_plan(
        task,
        _inventory(tmp_path, (_asset(tmp_path), trade_cal, stock_basic)),
        latest_open_trading_day="20260626",
    )

    range_row = _row(result, "trade_cal")
    assert range_row.plan_kind == "stable_latest_range"
    assert range_row.query_mode == "by_date_range"
    assert range_row.target_bucket_kind == "window"
    assert range_row.target_bucket_value == "latest"
    assert range_row.fetch_start_date == task.start_date
    assert range_row.fetch_end_date == "20260625"
    assert range_row.planned_action == "replace_verified_latest_candidate"

    snapshot_row = _row(result, "stock_basic")
    assert snapshot_row.plan_kind == "stable_latest_snapshot"
    assert snapshot_row.query_mode == "snapshot_by_param"
    assert snapshot_row.target_bucket_kind == "snapshot"
    assert snapshot_row.target_bucket_value == "latest"
    assert snapshot_row.fetch_start_date == "20260625"
    assert snapshot_row.fetch_end_date == "20260625"
    assert snapshot_row.existing_max_date == "20260610"
    assert result.summary["plan_kind_counts"] == {
        "by_trade_date_open_year": 1,
        "stable_latest_range": 1,
        "stable_latest_snapshot": 1,
    }


def test_write_incremental_plan_artifacts(tmp_path: Path) -> None:
    task = _task(tmp_path, api_names=["daily_basic"])
    result = build_incremental_plan(
        task,
        _inventory(tmp_path, (_asset(tmp_path, max_date="20260618"),)),
        latest_open_trading_day="20260626",
    )

    written = write_incremental_plan_artifacts(result, tmp_path / "artifacts")

    assert set(written) == {"incremental_plan", "incremental_plan_summary"}
    assert written["incremental_plan"].exists()
    assert written["incremental_plan_summary"].exists()
    csv_text = written["incremental_plan"].read_text(encoding="utf-8-sig")
    assert "daily_basic" in csv_text
    summary = json.loads(written["incremental_plan_summary"].read_text(encoding="utf-8"))
    assert summary["incremental_merge_executed"] is False
    assert summary["drive_write_executed"] is False
    assert summary["drive_delete_executed"] is False
