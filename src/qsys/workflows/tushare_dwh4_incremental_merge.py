"""Verified local incremental merge for DWH4.1 Tushare open-year buckets."""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any

import pandas as pd

from qsys.data.sources.tushare_source_registry import source_specs_by_api
from qsys.workflows.tushare_dwh4_incremental_plan import IncrementalPlanResult, IncrementalPlanRow
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

MERGE_REPORT_COLUMNS = [
    "provider",
    "source_family",
    "api_name",
    "dataset_version",
    "target_relative_path",
    "old_relative_path",
    "new_relative_path",
    "candidate_relative_path",
    "status",
    "merge_action",
    "primary_key",
    "old_rows",
    "new_rows",
    "candidate_rows",
    "old_only_rows",
    "new_only_rows",
    "identical_overlap_rows",
    "conflict_rows",
    "duplicate_key_rows",
    "schema_mismatch",
    "candidate_written",
    "reason",
]


@dataclass(frozen=True)
class IncrementalMergeReportRow:
    """One verified merge report row for a planned target bucket."""

    provider: str
    source_family: str
    api_name: str
    dataset_version: str
    target_relative_path: str
    old_relative_path: str
    new_relative_path: str
    candidate_relative_path: str
    status: str
    merge_action: str
    primary_key: str
    old_rows: int
    new_rows: int
    candidate_rows: int
    old_only_rows: int
    new_only_rows: int
    identical_overlap_rows: int
    conflict_rows: int
    duplicate_key_rows: int
    schema_mismatch: bool
    candidate_written: bool
    reason: str


@dataclass(frozen=True)
class IncrementalMergeResult:
    """Local candidate writes plus report and summary payloads."""

    workflow_name: str
    dataset_version: str
    candidate_root: str
    rows: tuple[IncrementalMergeReportRow, ...]
    summary: dict[str, Any]
    candidate_active_manifest: dict[str, Any]


def _safe_relative_path(value: str, *, label: str) -> PurePosixPath:
    if not value or "\\" in value or ":" in value:
        raise ValueError(f"{label} must be a safe relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"{label} must be a safe relative POSIX path")
    return path


def _safe_join(root: str | Path, relative_path: str, *, label: str) -> Path:
    rel = _safe_relative_path(relative_path, label=label)
    base = Path(root).resolve()
    resolved = (base / Path(*rel.parts)).resolve()
    resolved.relative_to(base)
    return resolved


def _path_digest(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _primary_key_for_api(task: Dwh4TushareTask, api_name: str, registry_path: str | Path | None) -> tuple[str, ...]:
    registry = Path(registry_path) if registry_path is not None else task.execution_repo / "configs" / "tushare" / "source_registry.yaml"
    spec = source_specs_by_api(registry).get(api_name)
    if spec is None:
        raise ValueError(f"missing Tushare registry spec for API: {api_name}")
    if not spec.primary_key:
        raise ValueError(f"Tushare registry spec has no primary key for API: {api_name}")
    return tuple(spec.primary_key)


def _report_row(
    plan_row: IncrementalPlanRow,
    *,
    status: str,
    merge_action: str,
    primary_key: tuple[str, ...],
    old_rows: int = 0,
    new_rows: int = 0,
    candidate_rows: int = 0,
    old_only_rows: int = 0,
    new_only_rows: int = 0,
    identical_overlap_rows: int = 0,
    conflict_rows: int = 0,
    duplicate_key_rows: int = 0,
    schema_mismatch: bool = False,
    candidate_written: bool = False,
    candidate_relative_path: str = "",
    reason: str = "",
) -> IncrementalMergeReportRow:
    return IncrementalMergeReportRow(
        provider=plan_row.provider,
        source_family=plan_row.source_family,
        api_name=plan_row.api_name,
        dataset_version=plan_row.dataset_version,
        target_relative_path=plan_row.target_relative_path,
        old_relative_path=plan_row.existing_relative_path,
        new_relative_path=plan_row.target_relative_path,
        candidate_relative_path=candidate_relative_path,
        status=status,
        merge_action=merge_action,
        primary_key=json.dumps(list(primary_key), ensure_ascii=False),
        old_rows=old_rows,
        new_rows=new_rows,
        candidate_rows=candidate_rows,
        old_only_rows=old_only_rows,
        new_only_rows=new_only_rows,
        identical_overlap_rows=identical_overlap_rows,
        conflict_rows=conflict_rows,
        duplicate_key_rows=duplicate_key_rows,
        schema_mismatch=schema_mismatch,
        candidate_written=candidate_written,
        reason=reason,
    )


def _missing_key_columns(frame: pd.DataFrame, primary_key: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(key for key in primary_key if key not in frame.columns)


def _duplicate_key_row_count(frame: pd.DataFrame, primary_key: tuple[str, ...]) -> int:
    if frame.empty:
        return 0
    return int(frame.duplicated(list(primary_key), keep=False).sum())


def _normal_cell(value: object) -> object:
    return None if pd.isna(value) else value


def _key_tuple(record: dict[str, object], primary_key: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(_normal_cell(record[key]) for key in primary_key)


def _canonical_tuple(record: dict[str, object], columns: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(_normal_cell(record[column]) for column in columns)


def _records_by_key(frame: pd.DataFrame, primary_key: tuple[str, ...], columns: tuple[str, ...]) -> dict[tuple[object, ...], dict[str, Any]]:
    records: dict[tuple[object, ...], dict[str, Any]] = {}
    for record in frame[list(columns)].to_dict(orient="records"):
        records[_key_tuple(record, primary_key)] = {
            "raw": record,
            "canonical": _canonical_tuple(record, columns),
        }
    return records


def _read_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _write_candidate(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _candidate_asset(
    plan_row: IncrementalPlanRow,
    *,
    candidate_relative_path: str,
    candidate_path: Path,
    candidate_rows: int,
    merge_action: str,
) -> dict[str, Any]:
    return {
        "provider": plan_row.provider,
        "source_family": plan_row.source_family,
        "api_name": plan_row.api_name,
        "dataset_version": plan_row.dataset_version,
        "relative_path": plan_row.target_relative_path,
        "candidate_relative_path": candidate_relative_path,
        "candidate_rows": candidate_rows,
        "candidate_sha256": _path_digest(candidate_path),
        "action": merge_action,
    }


def _merge_open_year_row(
    task: Dwh4TushareTask,
    plan_row: IncrementalPlanRow,
    *,
    drive_dwh_root: Path,
    local_incremental_root: Path,
    candidate_root: Path,
    registry_path: str | Path | None,
) -> tuple[IncrementalMergeReportRow, dict[str, Any] | None]:
    primary_key = _primary_key_for_api(task, plan_row.api_name, registry_path)
    new_path = _safe_join(local_incremental_root, plan_row.target_relative_path, label="target_relative_path")
    candidate_path = _safe_join(candidate_root, plan_row.target_relative_path, label="target_relative_path")
    candidate_relative_path = candidate_path.relative_to(candidate_root.resolve()).as_posix()
    merge_action = "replace_verified_incremental" if plan_row.existing_relative_path else "copy_new_open_year"
    if not new_path.exists():
        return (
            _report_row(
                plan_row,
                status="BLOCKED",
                merge_action=merge_action,
                primary_key=primary_key,
                reason=f"new local incremental parquet missing: {plan_row.target_relative_path}",
            ),
            None,
        )
    if plan_row.existing_relative_path:
        old_path = _safe_join(drive_dwh_root, plan_row.existing_relative_path, label="existing_relative_path")
        if not old_path.exists():
            return (
                _report_row(
                    plan_row,
                    status="BLOCKED",
                    merge_action=merge_action,
                    primary_key=primary_key,
                    reason=f"old Drive parquet missing: {plan_row.existing_relative_path}",
                ),
                None,
            )
        old_frame = _read_frame(old_path)
    else:
        old_frame = pd.DataFrame()
    new_frame = _read_frame(new_path)
    old_rows = int(len(old_frame))
    new_rows = int(len(new_frame))
    key_missing = _missing_key_columns(new_frame, primary_key)
    if plan_row.existing_relative_path:
        key_missing = tuple(sorted(set(key_missing) | set(_missing_key_columns(old_frame, primary_key))))
    if key_missing:
        return (
            _report_row(
                plan_row,
                status="BLOCKED",
                merge_action=merge_action,
                primary_key=primary_key,
                old_rows=old_rows,
                new_rows=new_rows,
                schema_mismatch=True,
                reason=f"primary key columns missing: {', '.join(key_missing)}",
            ),
            None,
        )
    duplicate_rows = _duplicate_key_row_count(new_frame, primary_key)
    if plan_row.existing_relative_path:
        duplicate_rows += _duplicate_key_row_count(old_frame, primary_key)
    if duplicate_rows:
        return (
            _report_row(
                plan_row,
                status="BLOCKED",
                merge_action=merge_action,
                primary_key=primary_key,
                old_rows=old_rows,
                new_rows=new_rows,
                duplicate_key_rows=duplicate_rows,
                reason="duplicate primary-key rows found in candidate inputs",
            ),
            None,
        )
    if plan_row.existing_relative_path:
        old_columns = tuple(str(column) for column in old_frame.columns)
        new_columns = tuple(str(column) for column in new_frame.columns)
        if set(old_columns) != set(new_columns):
            return (
                _report_row(
                    plan_row,
                    status="BLOCKED",
                    merge_action=merge_action,
                    primary_key=primary_key,
                    old_rows=old_rows,
                    new_rows=new_rows,
                    schema_mismatch=True,
                    reason="old and new parquet schemas differ",
                ),
                None,
            )
        columns = old_columns
        new_frame = new_frame[list(columns)]
    else:
        columns = tuple(str(column) for column in new_frame.columns)
    old_records = _records_by_key(old_frame, primary_key, columns) if plan_row.existing_relative_path else {}
    new_records = _records_by_key(new_frame, primary_key, columns)
    conflict_keys: list[tuple[object, ...]] = []
    identical_overlap = 0
    for key, new_record in new_records.items():
        old_record = old_records.get(key)
        if old_record is None:
            continue
        if old_record["canonical"] == new_record["canonical"]:
            identical_overlap += 1
        else:
            conflict_keys.append(key)
    old_only = sum(1 for key in old_records if key not in new_records)
    new_only = sum(1 for key in new_records if key not in old_records)
    if conflict_keys:
        return (
            _report_row(
                plan_row,
                status="BLOCKED",
                merge_action=merge_action,
                primary_key=primary_key,
                old_rows=old_rows,
                new_rows=new_rows,
                old_only_rows=old_only,
                new_only_rows=new_only,
                identical_overlap_rows=identical_overlap,
                conflict_rows=len(conflict_keys),
                reason="non-identical rows share the same primary key",
            ),
            None,
        )
    candidate_records: list[dict[str, object]] = []
    if plan_row.existing_relative_path:
        candidate_records.extend(old_frame[list(columns)].to_dict(orient="records"))
        candidate_records.extend(new_record["raw"] for key, new_record in new_records.items() if key not in old_records)
    else:
        candidate_records.extend(new_frame[list(columns)].to_dict(orient="records"))
    candidate_frame = pd.DataFrame(candidate_records, columns=list(columns))
    _write_candidate(candidate_frame, candidate_path)
    candidate_rows = int(len(candidate_frame))
    report_row = _report_row(
        plan_row,
        status="PASS",
        merge_action=merge_action,
        primary_key=primary_key,
        old_rows=old_rows,
        new_rows=new_rows,
        candidate_rows=candidate_rows,
        old_only_rows=old_only,
        new_only_rows=new_only,
        identical_overlap_rows=identical_overlap,
        candidate_written=True,
        candidate_relative_path=candidate_relative_path,
        reason="candidate bucket written locally after verified merge",
    )
    return (
        report_row,
        _candidate_asset(
            plan_row,
            candidate_relative_path=candidate_relative_path,
            candidate_path=candidate_path,
            candidate_rows=candidate_rows,
            merge_action=merge_action,
        ),
    )


def _skipped_row(plan_row: IncrementalPlanRow) -> IncrementalMergeReportRow:
    return _report_row(
        plan_row,
        status="SKIPPED",
        merge_action="not_incremental_merge_scope",
        primary_key=(),
        reason=f"plan_kind {plan_row.plan_kind} is handled outside I4 incremental merge",
    )


def _summary(task: Dwh4TushareTask, rows: tuple[IncrementalMergeReportRow, ...]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row.status] = status_counts.get(row.status, 0) + 1
        action_counts[row.merge_action] = action_counts.get(row.merge_action, 0) + 1
    blocked = [row for row in rows if row.status == "BLOCKED"]
    return {
        "workflow_name": task.workflow_name,
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "merge_row_count": len(rows),
        "pass_count": status_counts.get("PASS", 0),
        "blocked_count": status_counts.get("BLOCKED", 0),
        "skipped_count": status_counts.get("SKIPPED", 0),
        "status_counts": dict(sorted(status_counts.items())),
        "merge_action_counts": dict(sorted(action_counts.items())),
        "candidate_written_count": sum(1 for row in rows if row.candidate_written),
        "candidate_rows": sum(row.candidate_rows for row in rows),
        "old_rows": sum(row.old_rows for row in rows),
        "new_rows": sum(row.new_rows for row in rows),
        "identical_overlap_rows": sum(row.identical_overlap_rows for row in rows),
        "conflict_rows": sum(row.conflict_rows for row in rows),
        "duplicate_key_rows": sum(row.duplicate_key_rows for row in rows),
        "schema_mismatch_count": sum(1 for row in rows if row.schema_mismatch),
        "blocking_reasons": [row.reason for row in blocked],
        "incremental_merge_executed": True,
        "local_candidate_write_executed": any(row.candidate_written for row in rows),
        "verified_replacement_executed": False,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }


def _candidate_active_manifest(task: Dwh4TushareTask, assets: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    return {
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "active_assets": list(assets),
        "superseded_assets_not_deleted": [],
        "generated_by": "dwh4.1_i4_incremental_merge_candidate",
        "candidate_manifest_only": True,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }


def build_incremental_merge(
    task: Dwh4TushareTask,
    plan: IncrementalPlanResult,
    *,
    local_incremental_root: str | Path,
    candidate_root: str | Path,
    drive_dwh_root: str | Path | None = None,
    registry_path: str | Path | None = None,
) -> IncrementalMergeResult:
    """Build verified local candidate buckets from open-year incremental plan rows."""
    drive_root = Path(drive_dwh_root) if drive_dwh_root is not None else task.drive_dwh_root
    incremental_root = Path(local_incremental_root)
    candidate_base = Path(candidate_root)
    rows: list[IncrementalMergeReportRow] = []
    assets: list[dict[str, Any]] = []
    for plan_row in plan.rows:
        if plan_row.plan_kind != "by_trade_date_open_year":
            rows.append(_skipped_row(plan_row))
            continue
        report_row, asset = _merge_open_year_row(
            task,
            plan_row,
            drive_dwh_root=drive_root,
            local_incremental_root=incremental_root,
            candidate_root=candidate_base,
            registry_path=registry_path,
        )
        rows.append(report_row)
        if asset is not None:
            assets.append(asset)
    row_tuple = tuple(rows)
    asset_tuple = tuple(assets)
    return IncrementalMergeResult(
        workflow_name=task.workflow_name,
        dataset_version=task.dataset_version,
        candidate_root=str(candidate_base),
        rows=row_tuple,
        summary=_summary(task, row_tuple),
        candidate_active_manifest=_candidate_active_manifest(task, asset_tuple),
    )


def incremental_merge_report_rows(result: IncrementalMergeResult) -> list[dict[str, object]]:
    """Return JSON/CSV-safe incremental merge report rows."""
    return [asdict(row) for row in result.rows]


def write_incremental_merge_artifacts(result: IncrementalMergeResult, artifact_root: str | Path) -> dict[str, Path]:
    """Write incremental merge report, summary, and candidate manifest artifacts."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    report_path = root / "incremental_merge_report.csv"
    summary_path = root / "incremental_merge_summary.json"
    manifest_path = root / "candidate_active_manifest.json"
    rows = incremental_merge_report_rows(result)
    with report_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MERGE_REPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in MERGE_REPORT_COLUMNS})
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(result.candidate_active_manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "incremental_merge_report": report_path,
        "incremental_merge_summary": summary_path,
        "candidate_active_manifest": manifest_path,
    }
