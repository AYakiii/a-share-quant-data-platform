from __future__ import annotations

import json
from pathlib import Path

import pytest

from qsys.workflows.tushare_dwh4_active_manifest import build_active_manifest, write_active_manifest_artifacts
from qsys.workflows.tushare_dwh4_drive_inventory import DriveInventoryAsset, DriveInventoryResult
from qsys.workflows.tushare_dwh4_incremental_merge import IncrementalMergeResult
from qsys.workflows.tushare_dwh4_incremental_plan import IncrementalPlanResult, IncrementalPlanRow
from qsys.workflows.tushare_dwh4_task import task_from_dict


DATASET_VERSION = "v1_csi500_2021_2025_union"


def _payload(tmp_path: Path) -> dict[str, object]:
    execution_repo = tmp_path / "execution_repo"
    (execution_repo / "src").mkdir(parents=True, exist_ok=True)
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
        "api_names": ["daily_basic", "trade_cal", "stock_basic"],
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


def _task(tmp_path: Path, *, bad_manifest_path: str | None = None):
    payload = _payload(tmp_path)
    if bad_manifest_path is not None:
        payload["incremental_policy"]["active_manifest_policy"]["active_manifest_path"] = bad_manifest_path
    return task_from_dict(payload)


def _relative(source_family: str, api_name: str, bucket: str) -> str:
    return f"raw/tushare/{source_family}/{api_name}/{DATASET_VERSION}/{bucket}/data.parquet"


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
    relative_path = _relative(source_family, api_name, f"{bucket_kind}={bucket_value}")
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
        sha256=f"sha-{api_name}-{bucket_kind}-{bucket_value}",
        size_bytes=123,
        date_column="trade_date",
        min_date=min_date,
        max_date=max_date,
        snapshot_date=snapshot_date,
        metadata_exists=True,
        metadata_keys=("rows",),
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


def _plan_row(
    *,
    api_name: str,
    source_family: str,
    plan_kind: str,
    bucket_kind: str,
    bucket_value: str,
    existing: bool = True,
    fetch_start: str = "20220101",
    fetch_end: str = "20260625",
) -> IncrementalPlanRow:
    relative_path = _relative(source_family, api_name, f"{bucket_kind}={bucket_value}")
    return IncrementalPlanRow(
        provider="tushare",
        source_family=source_family,
        api_name=api_name,
        dataset_version=DATASET_VERSION,
        plan_kind=plan_kind,
        query_mode="by_date_range" if plan_kind == "stable_latest_range" else "snapshot_by_param",
        fetch_start_date=fetch_start,
        fetch_end_date=fetch_end,
        target_bucket_kind=bucket_kind,
        target_bucket_value=bucket_value,
        target_relative_path=relative_path,
        existing_relative_path=relative_path if existing else "",
        existing_max_date="20260601" if existing else "",
        existing_rows=2 if existing else None,
        open_year="2026",
        planned_action="replace_verified_latest_candidate" if existing else "copy_new_latest",
        reason="test",
    )


def _plan(task, *rows: IncrementalPlanRow) -> IncrementalPlanResult:
    return IncrementalPlanResult(
        workflow_name=task.workflow_name,
        dataset_version=task.dataset_version,
        latest_open_trading_day="20260626",
        target_end_date="20260625",
        rows=tuple(rows),
        summary={"plan_row_count": len(rows)},
    )


def _merge_result(task) -> IncrementalMergeResult:
    relative_path = _relative("market_basic", "daily_basic", "year=2026")
    return IncrementalMergeResult(
        workflow_name=task.workflow_name,
        dataset_version=task.dataset_version,
        candidate_root="candidate",
        rows=(),
        summary={"pass_count": 1},
        candidate_active_manifest={
            "provider": "tushare",
            "dataset_version": DATASET_VERSION,
            "active_assets": [
                {
                    "provider": "tushare",
                    "source_family": "market_basic",
                    "api_name": "daily_basic",
                    "dataset_version": DATASET_VERSION,
                    "relative_path": relative_path,
                    "candidate_relative_path": relative_path,
                    "candidate_rows": 12,
                    "candidate_sha256": "candidate-sha",
                    "action": "replace_verified_incremental",
                }
            ],
            "superseded_assets_not_deleted": [],
        },
    )


def _active_by_api(result, api_name: str) -> dict[str, object]:
    return next(asset for asset in result.active_manifest["active_assets"] if asset["api_name"] == api_name)


def test_active_manifest_generates_active_assets_and_keeps_superseded_legacy_files(tmp_path: Path) -> None:
    task = _task(tmp_path)
    assets = (
        _asset(tmp_path, bucket_value="2025", min_date="20250101", max_date="20251231"),
        _asset(tmp_path, bucket_value="2026", min_date="20260101", max_date="20260618"),
        _asset(tmp_path, source_family="market_calendar", api_name="trade_cal", bucket_kind="window", bucket_value="20220101_20260601", min_date="20220101", max_date="20260601"),
        _asset(tmp_path, source_family="market_calendar", api_name="trade_cal", bucket_kind="window", bucket_value="latest", min_date="20220101", max_date="20260601"),
        _asset(tmp_path, source_family="security_master", api_name="stock_basic", bucket_kind="snapshot", bucket_value="20260601", min_date="20260601", max_date="20260601", snapshot_date="20260601"),
        _asset(tmp_path, source_family="security_master", api_name="stock_basic", bucket_kind="snapshot", bucket_value="latest", min_date="20260601", max_date="20260601", snapshot_date="20260601"),
    )
    trade_cal = _plan_row(api_name="trade_cal", source_family="market_calendar", plan_kind="stable_latest_range", bucket_kind="window", bucket_value="latest")
    stock_basic = _plan_row(api_name="stock_basic", source_family="security_master", plan_kind="stable_latest_snapshot", bucket_kind="snapshot", bucket_value="latest", fetch_start="20260625", fetch_end="20260625")

    result = build_active_manifest(
        task,
        _inventory(tmp_path, assets),
        _plan(task, trade_cal, stock_basic),
        merge_result=_merge_result(task),
        generated_at="2026-06-29T00:00:00Z",
    )

    active_paths = {asset["relative_path"] for asset in result.active_manifest["active_assets"]}
    assert _relative("market_basic", "daily_basic", "year=2025") in active_paths
    assert _relative("market_basic", "daily_basic", "year=2026") in active_paths
    assert _relative("market_calendar", "trade_cal", "window=latest") in active_paths
    assert _relative("security_master", "stock_basic", "snapshot=latest") in active_paths
    assert _relative("market_calendar", "trade_cal", "window=20220101_20260601") not in active_paths
    assert _relative("security_master", "stock_basic", "snapshot=20260601") not in active_paths

    assert _active_by_api(result, "daily_basic")["action"] == "keep_existing"
    daily_2026 = next(asset for asset in result.active_manifest["active_assets"] if asset["relative_path"].endswith("year=2026/data.parquet"))
    assert daily_2026["action"] == "replace_verified_incremental"
    assert _active_by_api(result, "trade_cal")["action"] == "replace_verified_latest"
    assert _active_by_api(result, "stock_basic")["action"] == "replace_verified_latest"

    superseded = result.active_manifest["superseded_assets_not_deleted"]
    assert {row["relative_path"] for row in superseded} == {
        _relative("market_calendar", "trade_cal", "window=20220101_20260601"),
        _relative("security_master", "stock_basic", "snapshot=20260601"),
    }
    assert all(row["deleted"] is False for row in superseded)
    assert result.summary["active_asset_count"] == 4
    assert result.summary["superseded_asset_count"] == 2
    assert result.summary["drive_write_executed"] is False
    assert result.summary["drive_delete_executed"] is False
    assert result.summary["old_window_snapshot_deleted"] is False
    assert result.active_manifest["active_manifest_written_to_drive"] is False


def test_active_manifest_supports_copy_new_latest_when_no_existing_latest(tmp_path: Path) -> None:
    task = _task(tmp_path)
    legacy = _asset(
        tmp_path,
        source_family="market_calendar",
        api_name="trade_cal",
        bucket_kind="window",
        bucket_value="20220101_20260601",
        min_date="20220101",
        max_date="20260601",
    )
    trade_cal = _plan_row(
        api_name="trade_cal",
        source_family="market_calendar",
        plan_kind="stable_latest_range",
        bucket_kind="window",
        bucket_value="latest",
        existing=False,
    )

    result = build_active_manifest(task, _inventory(tmp_path, (legacy,)), _plan(task, trade_cal), generated_at="2026-06-29T00:00:00Z")

    active = _active_by_api(result, "trade_cal")
    assert active["relative_path"] == _relative("market_calendar", "trade_cal", "window=latest")
    assert active["action"] == "copy_new_latest"
    assert result.active_manifest["superseded_assets_not_deleted"] == [
        {
            "relative_path": legacy.relative_path,
            "api_name": "trade_cal",
            "bucket": "window=20220101_20260601",
            "rows": 10,
            "coverage": "20220101-20260601",
            "reason": "superseded_by_window_latest",
            "deleted": False,
        }
    ]


def test_active_manifest_rejects_unsafe_manifest_path(tmp_path: Path) -> None:
    task = _task(tmp_path, bad_manifest_path="../bad.json")

    with pytest.raises(ValueError, match="safe relative POSIX path"):
        build_active_manifest(task, _inventory(tmp_path, ()), _plan(task), generated_at="2026-06-29T00:00:00Z")


def test_write_active_manifest_artifacts(tmp_path: Path) -> None:
    task = _task(tmp_path)
    trade_cal = _plan_row(api_name="trade_cal", source_family="market_calendar", plan_kind="stable_latest_range", bucket_kind="window", bucket_value="latest")
    result = build_active_manifest(task, _inventory(tmp_path, ()), _plan(task, trade_cal), generated_at="2026-06-29T00:00:00Z")

    written = write_active_manifest_artifacts(result, tmp_path / "artifacts")

    assert set(written) == {"candidate_active_manifest", "active_manifest_summary", "stable_latest_report"}
    for path in written.values():
        assert path.exists()
    manifest = json.loads(written["candidate_active_manifest"].read_text(encoding="utf-8"))
    summary = json.loads(written["active_manifest_summary"].read_text(encoding="utf-8"))
    report = written["stable_latest_report"].read_text(encoding="utf-8-sig")
    assert manifest["candidate_manifest_only"] is True
    assert manifest["active_manifest_written_to_drive"] is False
    assert summary["drive_write_executed"] is False
    assert summary["drive_delete_executed"] is False
    assert "trade_cal" in report
