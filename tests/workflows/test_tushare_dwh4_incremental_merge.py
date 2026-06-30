from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qsys.workflows import tushare_dwh4_incremental_merge as merge_module
from qsys.workflows.tushare_dwh4_incremental_merge import build_incremental_merge, write_incremental_merge_artifacts
from qsys.workflows.tushare_dwh4_incremental_plan import IncrementalPlanResult, IncrementalPlanRow
from qsys.workflows.tushare_dwh4_task import task_from_dict


DATASET_VERSION = "v1_csi500_2021_2025_union"
PARQUET_FRAMES: dict[Path, pd.DataFrame] = {}


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
                        "source_family": "market_calendar",
                        "api_name": "trade_cal",
                        "fields": ["exchange", "cal_date", "is_open"],
                        "query_mode": "by_date_range",
                        "calendar_mode": "range_once",
                        "partition_key": "start_date",
                        "partition_keys": ["exchange", "start_date", "end_date"],
                        "range_start_param": "start_date",
                        "range_end_param": "end_date",
                        "primary_key": ["exchange", "cal_date"],
                        "universe_filter_mode": "none",
                        "empty_result_allowed": False,
                        "compact_bucket": "window_from_range",
                        "status": "approved",
                        "production_enabled": True,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


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
        "dataset_version": DATASET_VERSION,
        "start_date": "20220101",
        "end_date": "20260630",
        "api_names": ["daily_basic", "trade_cal"],
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
                "snapshot_apis": [],
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


def _task(tmp_path: Path):
    return task_from_dict(_payload(tmp_path))


def _target_relative_path(api_name: str = "daily_basic", source_family: str = "market_basic", bucket: str = "year=2026") -> str:
    return f"raw/tushare/{source_family}/{api_name}/{DATASET_VERSION}/{bucket}/data.parquet"


def _plan_row(*, existing: bool = True, plan_kind: str = "by_trade_date_open_year") -> IncrementalPlanRow:
    relative_path = _target_relative_path()
    return IncrementalPlanRow(
        provider="tushare",
        source_family="market_basic",
        api_name="daily_basic",
        dataset_version=DATASET_VERSION,
        plan_kind=plan_kind,
        query_mode="by_trade_date",
        fetch_start_date="20260615",
        fetch_end_date="20260625",
        target_bucket_kind="year",
        target_bucket_value="2026",
        target_relative_path=relative_path,
        existing_relative_path=relative_path if existing else "",
        existing_max_date="20260618" if existing else "",
        existing_rows=3 if existing else None,
        open_year="2026",
        planned_action="replace_verified_incremental_candidate" if existing else "copy_new_open_year",
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


def _patch_parquet(monkeypatch) -> None:
    PARQUET_FRAMES.clear()

    def fake_read_parquet(path):
        return PARQUET_FRAMES[Path(path).resolve()].copy()

    def fake_to_parquet(self, path, index=False):
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        PARQUET_FRAMES[output.resolve()] = self.copy()
        output.write_bytes(("mock parquet " + output.as_posix()).encode("utf-8"))

    monkeypatch.setattr(merge_module.pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)


def _write_parquet_marker(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    PARQUET_FRAMES[path.resolve()] = frame.copy()
    path.write_bytes(("mock parquet " + path.as_posix()).encode("utf-8"))


def _drive_path(task, relative_path: str) -> Path:
    return task.drive_dwh_root / Path(*relative_path.split("/"))


def _local_path(root: Path, relative_path: str) -> Path:
    return root / Path(*relative_path.split("/"))


def _old_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
            "trade_date": ["20260102", "20260103", "20260104"],
            "total_mv": [10.0, 20.0, 30.0],
        }
    )


def test_merge_preserves_old_adds_new_and_collapses_identical_overlap(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    row = _plan_row(existing=True)
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    old_path = _drive_path(task, row.existing_relative_path)
    new_path = _local_path(local_incremental_root, row.target_relative_path)
    _write_parquet_marker(old_path, _old_frame())
    old_bytes = old_path.read_bytes()
    _write_parquet_marker(
        new_path,
        pd.DataFrame(
            {
                "ts_code": ["000002.SZ", "000004.SZ"],
                "trade_date": ["20260103", "20260625"],
                "total_mv": [20.0, 40.0],
            }
        ),
    )

    result = build_incremental_merge(task, _plan(task, row), local_incremental_root=local_incremental_root, candidate_root=candidate_root)

    report = result.rows[0]
    candidate_path = _local_path(candidate_root, row.target_relative_path)
    candidate_frame = PARQUET_FRAMES[candidate_path.resolve()]
    assert report.status == "PASS"
    assert report.merge_action == "replace_verified_incremental"
    assert report.old_rows == 3
    assert report.new_rows == 2
    assert report.candidate_rows == 4
    assert report.old_only_rows == 2
    assert report.new_only_rows == 1
    assert report.identical_overlap_rows == 1
    assert report.conflict_rows == 0
    assert report.candidate_written is True
    assert candidate_path.exists()
    assert old_path.read_bytes() == old_bytes
    assert candidate_frame.to_dict(orient="records") == [
        {"ts_code": "000001.SZ", "trade_date": "20260102", "total_mv": 10.0},
        {"ts_code": "000002.SZ", "trade_date": "20260103", "total_mv": 20.0},
        {"ts_code": "000003.SZ", "trade_date": "20260104", "total_mv": 30.0},
        {"ts_code": "000004.SZ", "trade_date": "20260625", "total_mv": 40.0},
    ]
    assert result.summary["drive_write_executed"] is False
    assert result.summary["drive_delete_executed"] is False
    assert result.summary["verified_replacement_executed"] is False


def test_non_identical_key_conflict_blocks_candidate_write(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    row = _plan_row(existing=True)
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    _write_parquet_marker(_drive_path(task, row.existing_relative_path), _old_frame())
    _write_parquet_marker(
        _local_path(local_incremental_root, row.target_relative_path),
        pd.DataFrame({"ts_code": ["000002.SZ"], "trade_date": ["20260103"], "total_mv": [21.0]}),
    )

    result = build_incremental_merge(task, _plan(task, row), local_incremental_root=local_incremental_root, candidate_root=candidate_root)

    report = result.rows[0]
    assert report.status == "BLOCKED"
    assert report.conflict_rows == 1
    assert report.identical_overlap_rows == 0
    assert report.candidate_written is False
    assert "non-identical rows" in report.reason
    assert not _local_path(candidate_root, row.target_relative_path).exists()
    assert result.summary["blocked_count"] == 1
    assert result.candidate_active_manifest["active_assets"] == []


def test_schema_mismatch_blocks_candidate_write(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    row = _plan_row(existing=True)
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    _write_parquet_marker(_drive_path(task, row.existing_relative_path), _old_frame())
    _write_parquet_marker(
        _local_path(local_incremental_root, row.target_relative_path),
        pd.DataFrame(
            {
                "ts_code": ["000004.SZ"],
                "trade_date": ["20260625"],
                "total_mv": [40.0],
                "extra": ["bad"],
            }
        ),
    )

    result = build_incremental_merge(task, _plan(task, row), local_incremental_root=local_incremental_root, candidate_root=candidate_root)

    report = result.rows[0]
    assert report.status == "BLOCKED"
    assert report.schema_mismatch is True
    assert report.candidate_written is False
    assert "schemas differ" in report.reason
    assert result.summary["schema_mismatch_count"] == 1


def test_duplicate_candidate_keys_block_candidate_write(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    row = _plan_row(existing=True)
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    _write_parquet_marker(_drive_path(task, row.existing_relative_path), _old_frame())
    _write_parquet_marker(
        _local_path(local_incremental_root, row.target_relative_path),
        pd.DataFrame(
            {
                "ts_code": ["000004.SZ", "000004.SZ"],
                "trade_date": ["20260625", "20260625"],
                "total_mv": [40.0, 40.0],
            }
        ),
    )

    result = build_incremental_merge(task, _plan(task, row), local_incremental_root=local_incremental_root, candidate_root=candidate_root)

    report = result.rows[0]
    assert report.status == "BLOCKED"
    assert report.duplicate_key_rows == 2
    assert report.candidate_written is False
    assert "duplicate primary-key" in report.reason
    assert result.summary["duplicate_key_rows"] == 2


def test_copy_new_open_year_when_no_old_drive_bucket(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    row = _plan_row(existing=False)
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    _write_parquet_marker(
        _local_path(local_incremental_root, row.target_relative_path),
        pd.DataFrame(
            {
                "ts_code": ["000004.SZ", "000005.SZ"],
                "trade_date": ["20260624", "20260625"],
                "total_mv": [40.0, 50.0],
            }
        ),
    )

    result = build_incremental_merge(task, _plan(task, row), local_incremental_root=local_incremental_root, candidate_root=candidate_root)

    report = result.rows[0]
    assert report.status == "PASS"
    assert report.merge_action == "copy_new_open_year"
    assert report.old_rows == 0
    assert report.new_rows == 2
    assert report.candidate_rows == 2
    assert report.new_only_rows == 2
    assert PARQUET_FRAMES[_local_path(candidate_root, row.target_relative_path).resolve()].shape == (2, 3)


def test_write_incremental_merge_artifacts_and_skip_non_i4_rows(tmp_path: Path, monkeypatch) -> None:
    _patch_parquet(monkeypatch)
    task = _task(tmp_path)
    merge_row = _plan_row(existing=False)
    skipped = _plan_row(existing=False, plan_kind="stable_latest_range")
    local_incremental_root = tmp_path / "local_incremental"
    candidate_root = tmp_path / "candidate"
    _write_parquet_marker(
        _local_path(local_incremental_root, merge_row.target_relative_path),
        pd.DataFrame({"ts_code": ["000004.SZ"], "trade_date": ["20260625"], "total_mv": [40.0]}),
    )

    result = build_incremental_merge(
        task,
        _plan(task, merge_row, skipped),
        local_incremental_root=local_incremental_root,
        candidate_root=candidate_root,
    )
    written = write_incremental_merge_artifacts(result, tmp_path / "artifacts")

    assert set(written) == {"incremental_merge_report", "incremental_merge_summary", "candidate_active_manifest"}
    assert written["incremental_merge_report"].exists()
    assert written["incremental_merge_summary"].exists()
    assert written["candidate_active_manifest"].exists()
    report_csv = written["incremental_merge_report"].read_text(encoding="utf-8-sig")
    assert "daily_basic" in report_csv
    assert "SKIPPED" in report_csv
    summary = json.loads(written["incremental_merge_summary"].read_text(encoding="utf-8"))
    manifest = json.loads(written["candidate_active_manifest"].read_text(encoding="utf-8"))
    assert summary["pass_count"] == 1
    assert summary["skipped_count"] == 1
    assert summary["drive_write_executed"] is False
    assert summary["drive_delete_executed"] is False
    assert manifest["candidate_manifest_only"] is True
    assert manifest["drive_write_executed"] is False
    assert manifest["drive_delete_executed"] is False
    assert manifest["active_assets"][0]["relative_path"] == merge_row.target_relative_path
