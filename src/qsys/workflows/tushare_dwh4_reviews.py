"""Read-only review gates for the DWH4 dual-entry Tushare workflow."""
from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from qsys.workflows.tushare_dwh4_commands import default_package_root
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

REVIEW_REQUIRED_BUCKET_KINDS = {"scope", "snapshot"}
PROMOTION_ACTIONS = (
    "copy_new",
    "skip_identical",
    "replace_verified_incremental",
    "replace_verified_latest",
    "active_manifest_update",
    "block_non_identical",
    "superseded_legacy_keep",
    "delete_request_only",
)
PROMOTION_ACTION_SET = set(PROMOTION_ACTIONS)
PROMOTION_CONFIRMATION_ACTIONS = {"copy_new", "replace_verified_incremental", "replace_verified_latest", "active_manifest_update"}
PROMOTION_NO_MUTATION_ACTIONS = {"skip_identical", "superseded_legacy_keep"}


@dataclass(frozen=True)
class ReviewGateIssue:
    """Machine-readable review gate failure."""

    code: str
    field: str
    message: str


@dataclass(frozen=True)
class ReviewGateDecision:
    """Review gate decision for one workflow stage."""

    stage: str
    status: str
    checked: tuple[str, ...]
    issues: tuple[ReviewGateIssue, ...] = ()
    metadata: dict[str, Any] | None = None

    @property
    def passed(self) -> bool:
        """Return whether this gate passed."""
        return self.status == "PASS"


def _issue(code: str, field: str, message: str) -> ReviewGateIssue:
    return ReviewGateIssue(code=code, field=field, message=message)


def _decision(
    stage: str,
    *,
    checked: list[str],
    issues: list[ReviewGateIssue],
    metadata: dict[str, Any] | None = None,
) -> ReviewGateDecision:
    return ReviewGateDecision(
        stage=stage,
        status="PASS" if not issues else "FAIL",
        checked=tuple(checked),
        issues=tuple(issues),
        metadata=metadata or {},
    )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return payload


def _read_csv_rows(path: Path) -> tuple[dict[str, str], ...]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return tuple(dict(row) for row in csv.DictReader(handle))


def _missing_artifact_issues(paths: list[Path], root: Path) -> list[ReviewGateIssue]:
    return [
        _issue("ARTIFACT_MISSING", str(path.relative_to(root) if path.is_relative_to(root) else path), f"required artifact not found: {path}")
        for path in paths
        if not path.exists()
    ]


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except ValueError:
            return None


def _truthy_csv(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _action_count_from_ready(ready: dict[str, Any], action: str) -> int:
    value = ready.get(f"planned_{action}_count")
    if value is None and action == "block_non_identical":
        value = ready.get("planned_block_non_identical_count")
    coerced = _coerce_int(value)
    return coerced or 0


def _promotion_action_counts(ready: dict[str, Any], plan_rows: tuple[dict[str, str], ...]) -> tuple[dict[str, int], tuple[str, ...]]:
    row_counts = {action: 0 for action in PROMOTION_ACTIONS}
    unsupported: list[str] = []
    for row in plan_rows:
        action = str(row.get("action") or "").strip()
        if not action:
            continue
        if action not in PROMOTION_ACTION_SET:
            unsupported.append(action)
            continue
        row_counts[action] = row_counts.get(action, 0) + 1
    counts = {
        action: row_counts[action] if row_counts[action] else _action_count_from_ready(ready, action)
        for action in PROMOTION_ACTIONS
    }
    return {action: count for action, count in counts.items() if count}, tuple(sorted(set(unsupported)))


def _promotion_actions_present(action_counts: dict[str, int]) -> list[str]:
    return [action for action in PROMOTION_ACTIONS if action_counts.get(action, 0) > 0]


def review_ingest_artifacts(task: Dwh4TushareTask, *, output_root: str | Path | None = None) -> ReviewGateDecision:
    """Review Tushare raw ingest artifacts without running ingestion."""
    root = Path(output_root) if output_root is not None else task.output_root
    artifacts = root / "artifacts" / "tushare_raw_acquisition"
    summary_path = artifacts / "operator_summary.json"
    by_api_path = artifacts / "operator_summary_by_api.csv"
    manifest_path = artifacts / "tushare_acquisition_manifest.json"
    checked = ["required_artifacts"]
    issues = _missing_artifact_issues([summary_path, by_api_path, manifest_path], artifacts)
    if issues:
        return _decision("review-ingest", checked=checked, issues=issues, metadata={"artifacts_root": str(artifacts)})

    summary = _read_json(summary_path)
    by_api_rows = _read_csv_rows(by_api_path)
    manifest = _read_json(manifest_path)
    metadata: dict[str, Any] = {
        "artifacts_root": str(artifacts),
        "rough_check": summary.get("rough_check"),
        "api_count": len(by_api_rows),
        "planned_partitions": summary.get("planned_partitions"),
        "status_counts": summary.get("status_counts", {}),
        "abnormal_counts": summary.get("abnormal_counts", {}),
        "request_date_count": manifest.get("request_date_count"),
    }
    if task.auto_review_policy.require_ingest_rough_check_pass:
        checked.append("operator_summary.rough_check")
        if summary.get("rough_check") != "PASS":
            issues.append(_issue("INGEST_ROUGH_CHECK_NOT_PASS", "operator_summary.rough_check", "operator_summary rough_check must be PASS"))

    if task.auto_review_policy.require_all_api_rough_check_pass:
        checked.append("operator_summary_by_api.rough_check")
        failing = [row.get("api_name", "") for row in by_api_rows if row.get("rough_check") != "PASS"]
        if failing:
            issues.append(_issue("INGEST_BY_API_ROUGH_CHECK_NOT_PASS", "operator_summary_by_api.rough_check", f"API rough_check failed for: {','.join(failing)}"))

    abnormal_counts = summary.get("abnormal_counts", {})
    if not isinstance(abnormal_counts, dict):
        issues.append(_issue("INGEST_ABNORMAL_COUNTS_INVALID", "operator_summary.abnormal_counts", "abnormal_counts must be an object"))
        abnormal_counts = {}
    count_requirements = [
        ("bad_status_partitions", task.auto_review_policy.require_zero_bad_status_partitions),
        ("failed_partitions", task.auto_review_policy.require_zero_failed_partitions),
        ("disallowed_empty_partitions", task.auto_review_policy.require_zero_disallowed_empty_partitions),
        ("duplicate_partitions", task.auto_review_policy.require_zero_duplicate_partitions),
        ("missing_data_files", task.auto_review_policy.require_zero_missing_data_files),
        ("missing_metadata_files", task.auto_review_policy.require_zero_missing_metadata_files),
        ("required_contract_fields_missing", task.auto_review_policy.require_zero_missing_required_fields),
    ]
    for key, enabled in count_requirements:
        if not enabled:
            continue
        checked.append(f"operator_summary.abnormal_counts.{key}")
        value = _coerce_int(abnormal_counts.get(key))
        if value is None:
            issues.append(_issue("INGEST_ABNORMAL_COUNT_MISSING", f"operator_summary.abnormal_counts.{key}", f"{key} is missing or not numeric"))
        elif value != 0:
            issues.append(_issue("INGEST_ABNORMAL_COUNT_NONZERO", f"operator_summary.abnormal_counts.{key}", f"{key} must be 0, found {value}"))

    return _decision("review-ingest", checked=checked, issues=issues, metadata=metadata)


def review_compact_artifacts(
    task: Dwh4TushareTask,
    *,
    package_root: str | Path | None = None,
) -> ReviewGateDecision:
    """Review local compact artifacts without preparing or promoting data."""
    pkg = Path(package_root) if package_root is not None else task.execution_repo / default_package_root(task)
    manifest_path = pkg / "compact_manifest.json"
    qa_path = pkg / "compact_qa_report.csv"
    inventory_path = pkg / "raw_asset_inventory.csv"
    lineage_path = pkg / "compact_source_lineage.csv"
    ready_marker = pkg / "_LOCAL_COMPACT_READY.txt"
    checked = ["required_artifacts"]
    issues = _missing_artifact_issues([manifest_path, qa_path, inventory_path, lineage_path, ready_marker], pkg)
    if issues:
        return _decision("review-compact", checked=checked, issues=issues, metadata={"package_root": str(pkg)})

    manifest = _read_json(manifest_path)
    qa_rows = _read_csv_rows(qa_path)
    assets = manifest.get("compact_assets", [])
    metadata: dict[str, Any] = {
        "package_root": str(pkg),
        "provider": manifest.get("provider"),
        "dataset_version": manifest.get("dataset_version"),
        "compact_assets": len(assets) if isinstance(assets, list) else None,
        "total_rows": manifest.get("total_rows"),
        "failed_backlog_task_count": manifest.get("failed_backlog_task_count"),
        "qa_rows": len(qa_rows),
    }

    checked.append("_LOCAL_COMPACT_READY.txt")
    if not ready_marker.is_file():
        issues.append(_issue("COMPACT_READY_MARKER_MISSING", "_LOCAL_COMPACT_READY.txt", "_LOCAL_COMPACT_READY.txt must exist"))

    if task.auto_review_policy.require_compact_qa_all_ok:
        checked.append("compact_qa_report.ok")
        bad_rows = [row for row in qa_rows if not _truthy_csv(row.get("ok"))]
        if bad_rows:
            issues.append(_issue("COMPACT_QA_NOT_OK", "compact_qa_report.ok", f"compact QA has {len(bad_rows)} non-ok rows"))

    if task.auto_review_policy.require_zero_failed_backlog:
        checked.append("compact_manifest.failed_backlog_task_count")
        failed = _coerce_int(manifest.get("failed_backlog_task_count"))
        if failed is None:
            issues.append(_issue("COMPACT_FAILED_BACKLOG_MISSING", "compact_manifest.failed_backlog_task_count", "failed_backlog_task_count is missing or not numeric"))
        elif failed != 0:
            issues.append(_issue("COMPACT_FAILED_BACKLOG_NONZERO", "compact_manifest.failed_backlog_task_count", f"failed_backlog_task_count must be 0, found {failed}"))

    checked.append("compact_manifest.total_rows")
    total_rows = _coerce_int(manifest.get("total_rows"))
    if total_rows is None or total_rows <= 0:
        issues.append(_issue("COMPACT_TOTAL_ROWS_INVALID", "compact_manifest.total_rows", "compact_manifest total_rows must be > 0"))

    checked.append("compact_manifest.compact_assets")
    if not isinstance(assets, list) or not assets:
        issues.append(_issue("COMPACT_ASSETS_EMPTY", "compact_manifest.compact_assets", "compact_manifest compact_assets must be non-empty"))

    return _decision("review-compact", checked=checked, issues=issues, metadata=metadata)


def _review_required_bucket_kinds(ready: dict[str, Any], manifest: dict[str, Any]) -> tuple[str, ...]:
    kinds: set[str] = set()
    ready_kinds = ready.get("review_required_bucket_kinds", [])
    if isinstance(ready_kinds, list):
        kinds.update(str(kind) for kind in ready_kinds if kind)
    assets = manifest.get("compact_assets", [])
    if isinstance(assets, list):
        for asset in assets:
            if isinstance(asset, dict) and asset.get("bucket_kind") in REVIEW_REQUIRED_BUCKET_KINDS:
                kinds.add(str(asset["bucket_kind"]))
    return tuple(sorted(kinds))


def review_promotion_artifacts(
    task: Dwh4TushareTask,
    *,
    package_root: str | Path | None = None,
) -> ReviewGateDecision:
    """Review promotion readiness artifacts without writing Drive."""
    pkg = Path(package_root) if package_root is not None else task.execution_repo / default_package_root(task)
    ready_path = pkg / "READY_FOR_PROMOTION.json"
    plan_path = pkg / "drive_collision_plan.csv"
    manifest_path = pkg / "compact_manifest.json"
    qa_path = pkg / "compact_qa_report.csv"
    checked = ["required_artifacts"]
    issues = _missing_artifact_issues([ready_path, plan_path, manifest_path, qa_path], pkg)
    if issues:
        return _decision("review-promotion", checked=checked, issues=issues, metadata={"package_root": str(pkg)})

    ready = _read_json(ready_path)
    manifest = _read_json(manifest_path)
    plan_rows = _read_csv_rows(plan_path)
    qa_rows = _read_csv_rows(qa_path)
    required_review = _review_required_bucket_kinds(ready, manifest)
    authorized = tuple(task.promotion_policy.allow_reviewed_bucket_kinds)
    action_counts, unsupported_actions = _promotion_action_counts(ready, plan_rows)
    actions_present = _promotion_actions_present(action_counts)
    planned_block_non_identical_count = action_counts.get("block_non_identical", 0)
    planned_copy_new_count = action_counts.get("copy_new", 0)
    planned_skip_identical_count = action_counts.get("skip_identical", 0)
    metadata: dict[str, Any] = {
        "package_root": str(pkg),
        "ready_for_promotion": ready.get("ready_for_promotion"),
        "promotion_name": ready.get("promotion_name"),
        "provider": ready.get("provider"),
        "dataset_version": ready.get("dataset_version"),
        "planned_copy_new_count": planned_copy_new_count,
        "planned_skip_identical_count": planned_skip_identical_count,
        "planned_block_non_identical_count": planned_block_non_identical_count,
        "planned_replace_verified_incremental_count": action_counts.get("replace_verified_incremental", 0),
        "planned_replace_verified_latest_count": action_counts.get("replace_verified_latest", 0),
        "planned_active_manifest_update_count": action_counts.get("active_manifest_update", 0),
        "planned_superseded_legacy_keep_count": action_counts.get("superseded_legacy_keep", 0),
        "planned_delete_request_only_count": action_counts.get("delete_request_only", 0),
        "promotion_action_counts": dict(action_counts),
        "promotion_actions_present": actions_present,
        "promotion_actions_requiring_confirmation": [action for action in actions_present if action in PROMOTION_CONFIRMATION_ACTIONS],
        "promotion_no_mutation_actions": [action for action in actions_present if action in PROMOTION_NO_MUTATION_ACTIONS],
        "unsupported_promotion_actions": list(unsupported_actions),
        "delete_request_generated": action_counts.get("delete_request_only", 0) > 0,
        "review_required_bucket_kinds": list(required_review),
        "authorized_bucket_kinds": list(authorized),
        "collision_rows": len(plan_rows),
    }

    checked.append("promotion_action_vocabulary")
    if unsupported_actions:
        issues.append(_issue("PROMOTION_ACTION_UNSUPPORTED", "drive_collision_plan.action", f"unsupported promotion actions: {','.join(unsupported_actions)}"))

    if task.auto_review_policy.require_ready_for_promotion:
        checked.append("READY_FOR_PROMOTION.ready_for_promotion")
        if ready.get("ready_for_promotion") is not True:
            issues.append(_issue("PROMOTION_NOT_READY", "READY_FOR_PROMOTION.ready_for_promotion", "ready_for_promotion must be true"))

    checked.append("READY_FOR_PROMOTION.identity")
    if ready.get("promotion_name") != task.promotion_name:
        issues.append(_issue("PROMOTION_NAME_MISMATCH", "READY_FOR_PROMOTION.promotion_name", "promotion_name does not match task"))
    if ready.get("provider") != task.provider:
        issues.append(_issue("PROMOTION_PROVIDER_MISMATCH", "READY_FOR_PROMOTION.provider", "provider does not match task"))
    if ready.get("dataset_version") != task.dataset_version:
        issues.append(_issue("PROMOTION_DATASET_VERSION_MISMATCH", "READY_FOR_PROMOTION.dataset_version", "dataset_version does not match task"))

    checked.append("compact_manifest.identity")
    if manifest.get("promotion_name") != task.promotion_name:
        issues.append(_issue("COMPACT_PROMOTION_NAME_MISMATCH", "compact_manifest.promotion_name", "promotion_name does not match task"))
    if manifest.get("provider") != task.provider:
        issues.append(_issue("COMPACT_PROVIDER_MISMATCH", "compact_manifest.provider", "provider does not match task"))
    if manifest.get("dataset_version") != task.dataset_version:
        issues.append(_issue("COMPACT_DATASET_VERSION_MISMATCH", "compact_manifest.dataset_version", "dataset_version does not match task"))

    if task.auto_review_policy.block_non_identical_drive_collision:
        checked.append("drive_collision_plan.action")
        blocked_rows = [row for row in plan_rows if row.get("action") == "block_non_identical"]
        if blocked_rows:
            issues.append(_issue("PROMOTION_COLLISION_BLOCK_NON_IDENTICAL", "drive_collision_plan.action", f"collision plan has {len(blocked_rows)} block_non_identical rows"))
        planned_block = planned_block_non_identical_count
        checked.append("READY_FOR_PROMOTION.planned_block_non_identical_count")
        if planned_block != 0:
            issues.append(_issue("PROMOTION_BLOCK_COUNT_NONZERO", "READY_FOR_PROMOTION.planned_block_non_identical_count", f"planned_block_non_identical_count must be 0, found {planned_block}"))

    if task.auto_review_policy.require_compact_qa_all_ok:
        checked.append("compact_qa_report.ok")
        bad_rows = [row for row in qa_rows if not _truthy_csv(row.get("ok"))]
        if bad_rows:
            issues.append(_issue("PROMOTION_COMPACT_QA_NOT_OK", "compact_qa_report.ok", f"compact QA has {len(bad_rows)} non-ok rows"))

    checked.append("review_required_bucket_kinds")
    missing_authorization = [kind for kind in required_review if kind not in authorized]
    if missing_authorization:
        issues.append(_issue("PROMOTION_REVIEWED_BUCKET_KIND_NOT_AUTHORIZED", "review_required_bucket_kinds", f"reviewed bucket kinds not authorized by task: {','.join(missing_authorization)}"))

    return _decision("review-promotion", checked=checked, issues=issues, metadata=metadata)
