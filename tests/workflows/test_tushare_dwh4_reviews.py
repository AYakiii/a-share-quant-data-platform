from __future__ import annotations

import csv
import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_reviews import (
    review_compact_artifacts,
    review_ingest_artifacts,
    review_promotion_artifacts,
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


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fieldnames or list(rows[0].keys() if rows else [])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)


def _write_ingest_artifacts(task) -> Path:
    art = task.output_root / "artifacts" / "tushare_raw_acquisition"
    art.mkdir(parents=True, exist_ok=True)
    summary = {
        "provider": "tushare",
        "dataset_version": task.dataset_version,
        "start_date": task.start_date,
        "end_date": task.end_date,
        "api_names": list(task.api_names),
        "planned_partitions": 12,
        "status_counts": {"ok": 12},
        "abnormal_counts": {
            "bad_status_partitions": 0,
            "failed_partitions": 0,
            "disallowed_empty_partitions": 0,
            "duplicate_partitions": 0,
            "missing_data_files": 0,
            "missing_metadata_files": 0,
            "required_contract_fields_missing": 0,
        },
        "rough_check": "PASS",
    }
    (art / "operator_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    _write_csv(
        art / "operator_summary_by_api.csv",
        [{"api_name": api, "rough_check": "PASS"} for api in task.api_names],
    )
    (art / "tushare_acquisition_manifest.json").write_text(json.dumps({"request_date_count": 2}), encoding="utf-8")
    return art


def _package_root(tmp_path: Path) -> Path:
    return tmp_path / "pkg"


def _manifest(task, *, failed_backlog: int = 0, total_rows: int = 10, bucket_kind: str = "snapshot") -> dict[str, object]:
    return {
        "promotion_name": task.promotion_name,
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "total_rows": total_rows,
        "failed_backlog_task_count": failed_backlog,
        "compact_assets": [
            {
                "relative_path": "raw/tushare/fam/api/v1/snapshot=20260601/data.parquet",
                "source_family": "fam",
                "api_name": "api",
                "bucket_kind": bucket_kind,
                "bucket_value": "20260601",
                "rows": total_rows,
            }
        ]
        if total_rows > 0
        else [],
    }


def _write_compact_artifacts(task, pkg: Path, *, qa_ok: bool = True, failed_backlog: int = 0, total_rows: int = 10, bucket_kind: str = "snapshot") -> Path:
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "compact_manifest.json").write_text(json.dumps(_manifest(task, failed_backlog=failed_backlog, total_rows=total_rows, bucket_kind=bucket_kind)), encoding="utf-8")
    _write_csv(
        pkg / "compact_qa_report.csv",
        [{"relative_path": "raw/tushare/fam/api/v1/snapshot=20260601/data.parquet", "ok": qa_ok}],
    )
    _write_csv(pkg / "raw_asset_inventory.csv", [{"relative_path": "source.parquet"}])
    _write_csv(pkg / "compact_source_lineage.csv", [{"compact_relative_path": "raw/tushare/fam/api/v1/snapshot=20260601/data.parquet"}])
    (pkg / "_LOCAL_COMPACT_READY.txt").write_text("ready\n", encoding="utf-8")
    return pkg


def _write_promotion_artifacts(
    task,
    pkg: Path,
    *,
    ready: bool = True,
    action: str = "copy_new",
    planned_block: int = 0,
    provider: str | None = None,
    dataset_version: str | None = None,
    review_kinds: list[str] | None = None,
    bucket_kind: str = "snapshot",
) -> Path:
    _write_compact_artifacts(task, pkg, bucket_kind=bucket_kind)
    ready_payload = {
        "ready_for_promotion": ready,
        "promotion_name": task.promotion_name,
        "provider": provider or task.provider,
        "dataset_version": dataset_version or task.dataset_version,
        "planned_copy_new_count": 1 if action == "copy_new" else 0,
        "planned_skip_identical_count": 1 if action == "skip_identical" else 0,
        "planned_block_non_identical_count": planned_block,
        "review_required_bucket_kinds": review_kinds if review_kinds is not None else ["snapshot"],
    }
    (pkg / "READY_FOR_PROMOTION.json").write_text(json.dumps(ready_payload), encoding="utf-8")
    _write_csv(
        pkg / "drive_collision_plan.csv",
        [
            {
                "source_family": "fam",
                "api_name": "api",
                "bucket_kind": bucket_kind,
                "bucket_value": "20260601",
                "rows": 10,
                "relative_path": "raw/tushare/fam/api/v1/snapshot=20260601/data.parquet",
                "action": action,
            }
        ],
    )
    return pkg


def _codes(decision) -> set[str]:
    return {issue.code for issue in decision.issues}


def test_review_ingest_passes_on_clean_operator_artifacts(tmp_path: Path) -> None:
    task = _task(tmp_path)
    _write_ingest_artifacts(task)
    decision = review_ingest_artifacts(task)
    assert decision.status == "PASS"
    assert decision.passed is True
    assert decision.metadata["rough_check"] == "PASS"
    assert "operator_summary.rough_check" in decision.checked


def test_review_ingest_fails_on_rough_check_fail(tmp_path: Path) -> None:
    task = _task(tmp_path)
    art = _write_ingest_artifacts(task)
    summary = json.loads((art / "operator_summary.json").read_text(encoding="utf-8"))
    summary["rough_check"] = "FAIL"
    (art / "operator_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    decision = review_ingest_artifacts(task)
    assert decision.status == "FAIL"
    assert "INGEST_ROUGH_CHECK_NOT_PASS" in _codes(decision)


def test_review_ingest_fails_on_by_api_fail(tmp_path: Path) -> None:
    task = _task(tmp_path)
    art = _write_ingest_artifacts(task)
    _write_csv(art / "operator_summary_by_api.csv", [{"api_name": "daily_basic", "rough_check": "FAIL"}])
    decision = review_ingest_artifacts(task)
    assert "INGEST_BY_API_ROUGH_CHECK_NOT_PASS" in _codes(decision)


def test_review_ingest_fails_on_nonzero_abnormal_count(tmp_path: Path) -> None:
    task = _task(tmp_path)
    art = _write_ingest_artifacts(task)
    summary = json.loads((art / "operator_summary.json").read_text(encoding="utf-8"))
    summary["abnormal_counts"]["failed_partitions"] = 1
    (art / "operator_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    decision = review_ingest_artifacts(task)
    assert "INGEST_ABNORMAL_COUNT_NONZERO" in _codes(decision)


def test_review_ingest_reports_missing_artifacts(tmp_path: Path) -> None:
    task = _task(tmp_path)
    decision = review_ingest_artifacts(task)
    assert decision.status == "FAIL"
    assert _codes(decision) == {"ARTIFACT_MISSING"}


def test_review_compact_passes_on_clean_package(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path))
    decision = review_compact_artifacts(task, package_root=pkg)
    assert decision.status == "PASS"
    assert decision.metadata["compact_assets"] == 1
    assert "compact_qa_report.ok" in decision.checked


def test_review_compact_fails_on_qa_failure(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path), qa_ok=False)
    decision = review_compact_artifacts(task, package_root=pkg)
    assert "COMPACT_QA_NOT_OK" in _codes(decision)


def test_review_compact_fails_on_failed_backlog(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path), failed_backlog=2)
    decision = review_compact_artifacts(task, package_root=pkg)
    assert "COMPACT_FAILED_BACKLOG_NONZERO" in _codes(decision)


def test_review_compact_fails_on_empty_assets(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path), total_rows=0)
    decision = review_compact_artifacts(task, package_root=pkg)
    assert {"COMPACT_TOTAL_ROWS_INVALID", "COMPACT_ASSETS_EMPTY"}.issubset(_codes(decision))


def test_review_compact_reports_missing_artifacts(tmp_path: Path) -> None:
    decision = review_compact_artifacts(_task(tmp_path), package_root=_package_root(tmp_path))
    assert decision.status == "FAIL"
    assert _codes(decision) == {"ARTIFACT_MISSING"}


def test_review_promotion_passes_with_authorized_snapshot(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path))
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert decision.status == "PASS"
    assert decision.metadata["review_required_bucket_kinds"] == ["snapshot"]
    assert decision.metadata["authorized_bucket_kinds"] == ["snapshot"]
    assert decision.metadata["promotion_action_counts"] == {"copy_new": 1}
    assert decision.metadata["promotion_actions_present"] == ["copy_new"]


def test_review_promotion_supports_dwh41_action_vocabulary(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path))
    ready_payload = {
        "ready_for_promotion": True,
        "promotion_name": task.promotion_name,
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "review_required_bucket_kinds": ["snapshot"],
    }
    (pkg / "READY_FOR_PROMOTION.json").write_text(json.dumps(ready_payload), encoding="utf-8")
    _write_csv(
        pkg / "drive_collision_plan.csv",
        [
            {"relative_path": "a", "bucket_kind": "year", "action": "copy_new"},
            {"relative_path": "b", "bucket_kind": "year", "action": "skip_identical"},
            {"relative_path": "c", "bucket_kind": "year", "action": "replace_verified_incremental"},
            {"relative_path": "d", "bucket_kind": "window", "action": "replace_verified_latest"},
            {"relative_path": "e", "bucket_kind": "manifest", "action": "active_manifest_update"},
            {"relative_path": "f", "bucket_kind": "window", "action": "superseded_legacy_keep"},
            {"relative_path": "g", "bucket_kind": "legacy", "action": "delete_request_only"},
        ],
        fieldnames=["relative_path", "bucket_kind", "action"],
    )

    decision = review_promotion_artifacts(task, package_root=pkg)

    assert decision.status == "PASS"
    assert decision.metadata["promotion_action_counts"] == {
        "copy_new": 1,
        "skip_identical": 1,
        "replace_verified_incremental": 1,
        "replace_verified_latest": 1,
        "active_manifest_update": 1,
        "superseded_legacy_keep": 1,
        "delete_request_only": 1,
    }
    assert decision.metadata["promotion_actions_requiring_confirmation"] == [
        "copy_new",
        "replace_verified_incremental",
        "replace_verified_latest",
        "active_manifest_update",
    ]
    assert decision.metadata["promotion_no_mutation_actions"] == ["skip_identical", "superseded_legacy_keep"]
    assert decision.metadata["delete_request_generated"] is True


def test_review_promotion_rejects_unknown_action(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_compact_artifacts(task, _package_root(tmp_path))
    (pkg / "READY_FOR_PROMOTION.json").write_text(
        json.dumps(
            {
                "ready_for_promotion": True,
                "promotion_name": task.promotion_name,
                "provider": task.provider,
                "dataset_version": task.dataset_version,
                "review_required_bucket_kinds": ["snapshot"],
            }
        ),
        encoding="utf-8",
    )
    _write_csv(pkg / "drive_collision_plan.csv", [{"relative_path": "x", "bucket_kind": "snapshot", "action": "overwrite"}])

    decision = review_promotion_artifacts(task, package_root=pkg)

    assert "PROMOTION_ACTION_UNSUPPORTED" in _codes(decision)
    assert decision.metadata["unsupported_promotion_actions"] == ["overwrite"]


def test_review_promotion_fails_when_not_ready(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path), ready=False)
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert "PROMOTION_NOT_READY" in _codes(decision)


def test_review_promotion_fails_on_block_non_identical(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path), action="block_non_identical", planned_block=1)
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert {"PROMOTION_COLLISION_BLOCK_NON_IDENTICAL", "PROMOTION_BLOCK_COUNT_NONZERO"}.issubset(_codes(decision))


def test_review_promotion_fails_on_identity_mismatch(tmp_path: Path) -> None:
    task = _task(tmp_path)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path), provider="akshare", dataset_version="v2")
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert {"PROMOTION_PROVIDER_MISMATCH", "PROMOTION_DATASET_VERSION_MISMATCH"}.issubset(_codes(decision))


def test_review_promotion_blocks_unauthorized_review_bucket_kind(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["promotion_policy"] = {
        "auto_prepare": True,
        "auto_promote": False,
        "require_final_human_confirmation": True,
        "allow_reviewed_bucket_kinds": [],
    }
    task = task_from_dict(payload)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path))
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert "PROMOTION_REVIEWED_BUCKET_KIND_NOT_AUTHORIZED" in _codes(decision)


def test_review_promotion_uses_manifest_bucket_kind_even_if_ready_is_stale(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    payload["promotion_policy"] = {
        "auto_prepare": True,
        "auto_promote": False,
        "require_final_human_confirmation": True,
        "allow_reviewed_bucket_kinds": [],
    }
    task = task_from_dict(payload)
    pkg = _write_promotion_artifacts(task, _package_root(tmp_path), review_kinds=[])
    decision = review_promotion_artifacts(task, package_root=pkg)
    assert decision.metadata["review_required_bucket_kinds"] == ["snapshot"]
    assert "PROMOTION_REVIEWED_BUCKET_KIND_NOT_AUTHORIZED" in _codes(decision)


def test_review_promotion_reports_missing_artifacts(tmp_path: Path) -> None:
    decision = review_promotion_artifacts(_task(tmp_path), package_root=_package_root(tmp_path))
    assert decision.status == "FAIL"
    assert _codes(decision) == {"ARTIFACT_MISSING"}
