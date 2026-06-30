"""Candidate active manifest generation for DWH4.1 Tushare assets."""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
from typing import Any

from qsys.workflows.tushare_dwh4_drive_inventory import DriveInventoryAsset, DriveInventoryResult
from qsys.workflows.tushare_dwh4_incremental_merge import IncrementalMergeResult
from qsys.workflows.tushare_dwh4_incremental_plan import IncrementalPlanResult, IncrementalPlanRow
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

STABLE_LATEST_REPORT_COLUMNS = [
    "api_name",
    "bucket",
    "active_relative_path",
    "existing_relative_path",
    "old_coverage",
    "new_coverage",
    "action",
    "superseded_count",
    "status",
    "reason",
]


@dataclass(frozen=True)
class StableLatestReportRow:
    """One stable latest active-path planning row."""

    api_name: str
    bucket: str
    active_relative_path: str
    existing_relative_path: str
    old_coverage: str
    new_coverage: str
    action: str
    superseded_count: int
    status: str
    reason: str


@dataclass(frozen=True)
class ActiveManifestResult:
    """Candidate active manifest plus summaries and stable latest report rows."""

    workflow_name: str
    dataset_version: str
    active_manifest_path: str
    active_manifest: dict[str, Any]
    stable_latest_rows: tuple[StableLatestReportRow, ...]
    summary: dict[str, Any]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_relative_path(value: str, *, label: str) -> PurePosixPath:
    if not value or "\\" in value or ":" in value:
        raise ValueError(f"{label} must be a safe relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"{label} must be a safe relative POSIX path")
    return path


def _validate_active_manifest_path(value: str, dataset_version: str) -> str:
    path = _safe_relative_path(value, label="active_manifest_path")
    parts = path.parts
    if len(parts) < 5 or parts[:4] != ("catalog", "active", "tushare", dataset_version):
        raise ValueError("active_manifest_path must be under catalog/active/tushare/<dataset_version>/")
    if parts[-1] != "dwh4_tushare_active_manifest.json":
        raise ValueError("active_manifest_path must end with dwh4_tushare_active_manifest.json")
    return path.as_posix()


def _asset_coverage(asset: DriveInventoryAsset) -> str:
    if asset.snapshot_date:
        return asset.snapshot_date
    if asset.min_date and asset.max_date:
        return f"{asset.min_date}-{asset.max_date}"
    if asset.max_date:
        return asset.max_date
    return ""


def _plan_coverage(row: IncrementalPlanRow) -> str:
    if row.plan_kind == "stable_latest_snapshot":
        return row.fetch_end_date
    if row.fetch_start_date and row.fetch_end_date:
        return f"{row.fetch_start_date}-{row.fetch_end_date}"
    return row.fetch_end_date


def _bucket(row: IncrementalPlanRow) -> str:
    return f"{row.target_bucket_kind}={row.target_bucket_value}"


def _active_asset_from_inventory(asset: DriveInventoryAsset, *, action: str = "keep_existing") -> dict[str, Any]:
    return {
        "provider": asset.provider,
        "source_family": asset.source_family,
        "api_name": asset.api_name,
        "dataset_version": asset.dataset_version,
        "relative_path": asset.relative_path,
        "bucket": f"{asset.bucket_kind}={asset.bucket_value}",
        "action": action,
        "rows": asset.rows,
        "sha256": asset.sha256,
        "coverage": _asset_coverage(asset),
        "metadata_exists": asset.metadata_exists,
    }


def _active_asset_from_plan(row: IncrementalPlanRow) -> dict[str, Any]:
    action = "replace_verified_latest" if row.existing_relative_path else "copy_new_latest"
    return {
        "provider": row.provider,
        "source_family": row.source_family,
        "api_name": row.api_name,
        "dataset_version": row.dataset_version,
        "relative_path": row.target_relative_path,
        "bucket": _bucket(row),
        "action": action,
        "requested_start_date": row.fetch_start_date,
        "requested_end_date": row.fetch_end_date,
        "coverage": _plan_coverage(row),
    }


def _active_assets_from_merge(merge_result: IncrementalMergeResult | None) -> dict[str, dict[str, Any]]:
    if merge_result is None:
        return {}
    assets: dict[str, dict[str, Any]] = {}
    for asset in merge_result.candidate_active_manifest.get("active_assets", []):
        if not isinstance(asset, dict):
            continue
        relative_path = str(asset.get("relative_path") or "")
        if not relative_path:
            continue
        assets[relative_path] = {
            "provider": str(asset.get("provider") or ""),
            "source_family": str(asset.get("source_family") or ""),
            "api_name": str(asset.get("api_name") or ""),
            "dataset_version": str(asset.get("dataset_version") or ""),
            "relative_path": relative_path,
            "candidate_relative_path": str(asset.get("candidate_relative_path") or ""),
            "candidate_rows": asset.get("candidate_rows"),
            "candidate_sha256": str(asset.get("candidate_sha256") or ""),
            "bucket": _bucket_from_relative_path(relative_path),
            "action": str(asset.get("action") or "replace_verified_incremental"),
        }
    return assets


def _bucket_from_relative_path(relative_path: str) -> str:
    parts = PurePosixPath(relative_path).parts
    if len(parts) >= 2:
        for part in parts:
            if "=" in part:
                return part
    return ""


def _stable_plan_rows(plan: IncrementalPlanResult) -> tuple[IncrementalPlanRow, ...]:
    return tuple(row for row in plan.rows if row.plan_kind in {"stable_latest_range", "stable_latest_snapshot"})


def _ok_assets(inventory: DriveInventoryResult) -> tuple[DriveInventoryAsset, ...]:
    return tuple(asset for asset in inventory.assets if asset.status == "ok")


def _superseded_assets_for(row: IncrementalPlanRow, inventory: DriveInventoryResult) -> tuple[DriveInventoryAsset, ...]:
    return tuple(
        asset
        for asset in _ok_assets(inventory)
        if asset.api_name == row.api_name
        and asset.dataset_version == row.dataset_version
        and asset.bucket_kind == row.target_bucket_kind
        and asset.relative_path != row.target_relative_path
    )


def _superseded_record(asset: DriveInventoryAsset, row: IncrementalPlanRow) -> dict[str, Any]:
    return {
        "relative_path": asset.relative_path,
        "api_name": asset.api_name,
        "bucket": f"{asset.bucket_kind}={asset.bucket_value}",
        "rows": asset.rows,
        "coverage": _asset_coverage(asset),
        "reason": f"superseded_by_{row.target_bucket_kind}_latest",
        "deleted": False,
    }


def _stable_latest_report_row(
    row: IncrementalPlanRow,
    *,
    inventory: DriveInventoryResult,
    superseded_count: int,
) -> StableLatestReportRow:
    existing = next((asset for asset in _ok_assets(inventory) if asset.relative_path == row.existing_relative_path), None)
    return StableLatestReportRow(
        api_name=row.api_name,
        bucket=_bucket(row),
        active_relative_path=row.target_relative_path,
        existing_relative_path=row.existing_relative_path,
        old_coverage=_asset_coverage(existing) if existing is not None else "",
        new_coverage=_plan_coverage(row),
        action="replace_verified_latest" if row.existing_relative_path else "copy_new_latest",
        superseded_count=superseded_count,
        status="READY",
        reason=f"{_bucket(row)} active path selected; superseded legacy assets kept",
    )


def _summary(
    task: Dwh4TushareTask,
    *,
    active_assets: list[dict[str, Any]],
    superseded: list[dict[str, Any]],
    stable_rows: tuple[StableLatestReportRow, ...],
    merge_result: IncrementalMergeResult | None,
) -> dict[str, Any]:
    action_counts: dict[str, int] = {}
    for asset in active_assets:
        action = str(asset.get("action") or "")
        action_counts[action] = action_counts.get(action, 0) + 1
    return {
        "workflow_name": task.workflow_name,
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "promotion_name": task.promotion_name,
        "active_asset_count": len(active_assets),
        "superseded_asset_count": len(superseded),
        "stable_latest_count": len(stable_rows),
        "active_action_counts": dict(sorted(action_counts.items())),
        "merge_candidate_active_asset_count": len(merge_result.candidate_active_manifest.get("active_assets", [])) if merge_result is not None else 0,
        "candidate_manifest_only": True,
        "active_manifest_written_to_drive": False,
        "drive_write_executed": False,
        "drive_delete_executed": False,
        "old_window_snapshot_deleted": False,
    }


def build_active_manifest(
    task: Dwh4TushareTask,
    inventory: DriveInventoryResult,
    plan: IncrementalPlanResult,
    *,
    merge_result: IncrementalMergeResult | None = None,
    generated_at: str | None = None,
) -> ActiveManifestResult:
    """Build a candidate active manifest without writing it to Drive."""
    if task.incremental_policy is None:
        raise ValueError("DWH4.1 active manifest generation requires incremental_policy")
    manifest_path = _validate_active_manifest_path(task.incremental_policy.active_manifest_policy.active_manifest_path, task.dataset_version)
    merge_assets = _active_assets_from_merge(merge_result)
    stable_rows = _stable_plan_rows(plan)
    stable_targets = {row.target_relative_path for row in stable_rows}
    stable_superseded: list[dict[str, Any]] = []
    stable_report_rows: list[StableLatestReportRow] = []
    active_by_path: dict[str, dict[str, Any]] = {}
    for asset in _ok_assets(inventory):
        if asset.dataset_version != task.dataset_version:
            continue
        if asset.relative_path in merge_assets:
            continue
        if asset.relative_path in stable_targets:
            continue
        if any(
            asset.api_name == row.api_name
            and asset.bucket_kind == row.target_bucket_kind
            and asset.relative_path != row.target_relative_path
            for row in stable_rows
        ):
            continue
        active_by_path[asset.relative_path] = _active_asset_from_inventory(asset)
    for relative_path, asset in merge_assets.items():
        active_by_path[relative_path] = asset
    for row in stable_rows:
        superseded = _superseded_assets_for(row, inventory)
        for asset in superseded:
            stable_superseded.append(_superseded_record(asset, row))
        stable_report_rows.append(_stable_latest_report_row(row, inventory=inventory, superseded_count=len(superseded)))
        active_by_path[row.target_relative_path] = _active_asset_from_plan(row)
    active_assets = [active_by_path[path] for path in sorted(active_by_path)]
    superseded_assets = sorted(stable_superseded, key=lambda item: str(item["relative_path"]))
    manifest = {
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "updated_at": generated_at or _utc_now(),
        "promotion_name": task.promotion_name,
        "active_manifest_path": manifest_path,
        "active_assets": active_assets,
        "superseded_assets_not_deleted": superseded_assets,
        "candidate_manifest_only": True,
        "active_manifest_written_to_drive": False,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }
    stable_tuple = tuple(stable_report_rows)
    return ActiveManifestResult(
        workflow_name=task.workflow_name,
        dataset_version=task.dataset_version,
        active_manifest_path=manifest_path,
        active_manifest=manifest,
        stable_latest_rows=stable_tuple,
        summary=_summary(task, active_assets=active_assets, superseded=superseded_assets, stable_rows=stable_tuple, merge_result=merge_result),
    )


def stable_latest_report_rows(result: ActiveManifestResult) -> list[dict[str, object]]:
    """Return JSON/CSV-safe stable latest report rows."""
    return [asdict(row) for row in result.stable_latest_rows]


def write_active_manifest_artifacts(result: ActiveManifestResult, artifact_root: str | Path) -> dict[str, Path]:
    """Write candidate active manifest, stable latest report, and summary artifacts locally."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "candidate_active_manifest.json"
    summary_path = root / "active_manifest_summary.json"
    stable_path = root / "stable_latest_report.csv"
    rows = stable_latest_report_rows(result)
    with stable_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=STABLE_LATEST_REPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in STABLE_LATEST_REPORT_COLUMNS})
    manifest_path.write_text(json.dumps(result.active_manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "candidate_active_manifest": manifest_path,
        "active_manifest_summary": summary_path,
        "stable_latest_report": stable_path,
    }
