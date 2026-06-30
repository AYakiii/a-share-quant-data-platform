"""Incremental planning for DWH4.1 Tushare Drive-aware updates."""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path, PurePosixPath
from typing import Any

from qsys.data.sources.tushare_source_registry import source_specs_by_api
from qsys.workflows.tushare_dwh4_drive_inventory import DriveInventoryAsset, DriveInventoryResult
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

PLAN_COLUMNS = [
    "provider",
    "source_family",
    "api_name",
    "dataset_version",
    "plan_kind",
    "query_mode",
    "fetch_start_date",
    "fetch_end_date",
    "target_bucket_kind",
    "target_bucket_value",
    "target_relative_path",
    "existing_relative_path",
    "existing_max_date",
    "existing_rows",
    "open_year",
    "planned_action",
    "reason",
]


@dataclass(frozen=True)
class IncrementalPlanRow:
    """One per-API fetch-window plan row."""

    provider: str
    source_family: str
    api_name: str
    dataset_version: str
    plan_kind: str
    query_mode: str
    fetch_start_date: str
    fetch_end_date: str
    target_bucket_kind: str
    target_bucket_value: str
    target_relative_path: str
    existing_relative_path: str
    existing_max_date: str
    existing_rows: int | None
    open_year: str
    planned_action: str
    reason: str


@dataclass(frozen=True)
class IncrementalPlanResult:
    """Incremental plan rows and fixed-size summary."""

    workflow_name: str
    dataset_version: str
    latest_open_trading_day: str
    target_end_date: str
    rows: tuple[IncrementalPlanRow, ...]
    summary: dict[str, Any]


def _valid_yyyymmdd(value: str) -> bool:
    if len(value) != 8 or not value.isdigit():
        return False
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return False
    return True


def _require_yyyymmdd(value: str, *, label: str) -> str:
    text = str(value or "")
    if not _valid_yyyymmdd(text):
        raise ValueError(f"{label} must be YYYYMMDD")
    return text


def _date(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d")


def _fmt(value: datetime) -> str:
    return value.strftime("%Y%m%d")


def _subtract_trading_days(value: str, days: int) -> str:
    current = _date(_require_yyyymmdd(value, label="date"))
    remaining = int(days)
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return _fmt(current)


def _clip_to_open_year(value: str, open_year: str) -> str:
    year_start = f"{open_year}0101"
    return year_start if value < year_start else value


def _target_end_date(task: Dwh4TushareTask, *, latest_open_trading_day: str | None) -> tuple[str, str]:
    if task.incremental_policy is None:
        raise ValueError("DWH4.1 incremental planner requires incremental_policy")
    latest = latest_open_trading_day or task.end_date
    latest = _require_yyyymmdd(latest, label="latest_open_trading_day")
    return latest, _subtract_trading_days(latest, task.incremental_policy.data_lag_trading_days)


def _target_relative_path(
    *,
    provider: str,
    source_family: str,
    api_name: str,
    dataset_version: str,
    bucket_kind: str,
    bucket_value: str,
) -> str:
    return str(
        PurePosixPath("raw")
        / provider
        / source_family
        / api_name
        / dataset_version
        / f"{bucket_kind}={bucket_value}"
        / "data.parquet"
    )


def _split_bucket_spec(value: str, *, label: str) -> tuple[str, str]:
    if "=" not in value:
        raise ValueError(f"{label} must use key=value format")
    kind, bucket = value.split("=", 1)
    if not kind or not bucket:
        raise ValueError(f"{label} must use key=value format")
    return kind, bucket


def _max_date(values: list[str]) -> str:
    clean = [value for value in values if _valid_yyyymmdd(value)]
    return max(clean) if clean else ""


def _asset_date(asset: DriveInventoryAsset) -> str:
    return _max_date([asset.max_date, asset.snapshot_date])


def _assets_for_api(inventory: DriveInventoryResult, api_name: str) -> tuple[DriveInventoryAsset, ...]:
    return tuple(asset for asset in inventory.assets if asset.api_name == api_name and asset.status == "ok")


def _select_asset(
    assets: tuple[DriveInventoryAsset, ...],
    *,
    bucket_kind: str,
    bucket_value: str,
) -> DriveInventoryAsset | None:
    matches = [asset for asset in assets if asset.bucket_kind == bucket_kind and asset.bucket_value == bucket_value]
    if not matches:
        return None
    return sorted(matches, key=lambda asset: (asset.relative_path, asset.rows or 0))[-1]


def _source_family(api_name: str, specs: dict[str, Any], assets: tuple[DriveInventoryAsset, ...]) -> str:
    spec = specs.get(api_name)
    if spec is not None:
        return str(spec.source_family)
    if assets:
        return assets[0].source_family
    raise ValueError(f"missing Tushare registry spec for API: {api_name}")


def _query_mode(api_name: str, specs: dict[str, Any]) -> str:
    spec = specs.get(api_name)
    return str(spec.query_mode) if spec is not None else "unknown"


def _plan_by_trade_date(
    task: Dwh4TushareTask,
    api_name: str,
    assets: tuple[DriveInventoryAsset, ...],
    *,
    source_family: str,
    query_mode: str,
    target_end_date: str,
) -> IncrementalPlanRow:
    if task.incremental_policy is None:
        raise ValueError("DWH4.1 incremental planner requires incremental_policy")
    open_year = target_end_date[:4]
    open_year_asset = _select_asset(assets, bucket_kind="year", bucket_value=open_year)
    closed_year_assets = [asset for asset in assets if asset.bucket_kind == "year" and asset.bucket_value < open_year]
    if open_year_asset is None:
        fetch_start = f"{open_year}0101" if closed_year_assets else task.start_date
        existing_max = ""
        existing_rows = None
        existing_relative = ""
        action = "copy_new_open_year"
        reason = "no current open-year Drive bucket"
    else:
        existing_max = _asset_date(open_year_asset)
        if not existing_max:
            fetch_start = f"{open_year}0101"
            reason = "current open-year bucket has no date range metadata"
        else:
            fetch_start = _subtract_trading_days(existing_max, task.incremental_policy.open_year_policy.overlap_trading_days)
            if task.incremental_policy.open_year_policy.clip_overlap_to_open_year:
                fetch_start = _clip_to_open_year(fetch_start, open_year)
            reason = "current open-year bucket found; overlap applied"
        existing_rows = open_year_asset.rows
        existing_relative = open_year_asset.relative_path
        action = "replace_verified_incremental_candidate"
    return IncrementalPlanRow(
        provider=task.provider,
        source_family=source_family,
        api_name=api_name,
        dataset_version=task.dataset_version,
        plan_kind="by_trade_date_open_year",
        query_mode=query_mode,
        fetch_start_date=fetch_start,
        fetch_end_date=target_end_date,
        target_bucket_kind="year",
        target_bucket_value=open_year,
        target_relative_path=_target_relative_path(
            provider=task.provider,
            source_family=source_family,
            api_name=api_name,
            dataset_version=task.dataset_version,
            bucket_kind="year",
            bucket_value=open_year,
        ),
        existing_relative_path=existing_relative,
        existing_max_date=existing_max,
        existing_rows=existing_rows,
        open_year=open_year,
        planned_action=action,
        reason=reason,
    )


def _plan_stable_latest(
    task: Dwh4TushareTask,
    api_name: str,
    assets: tuple[DriveInventoryAsset, ...],
    *,
    source_family: str,
    query_mode: str,
    target_end_date: str,
    bucket_kind: str,
    bucket_value: str,
    plan_kind: str,
) -> IncrementalPlanRow:
    existing = _select_asset(assets, bucket_kind=bucket_kind, bucket_value=bucket_value)
    is_snapshot = bucket_kind == "snapshot"
    fetch_start = target_end_date if is_snapshot else task.start_date
    fetch_end = target_end_date
    return IncrementalPlanRow(
        provider=task.provider,
        source_family=source_family,
        api_name=api_name,
        dataset_version=task.dataset_version,
        plan_kind=plan_kind,
        query_mode=query_mode,
        fetch_start_date=fetch_start,
        fetch_end_date=fetch_end,
        target_bucket_kind=bucket_kind,
        target_bucket_value=bucket_value,
        target_relative_path=_target_relative_path(
            provider=task.provider,
            source_family=source_family,
            api_name=api_name,
            dataset_version=task.dataset_version,
            bucket_kind=bucket_kind,
            bucket_value=bucket_value,
        ),
        existing_relative_path=existing.relative_path if existing is not None else "",
        existing_max_date=_asset_date(existing) if existing is not None else "",
        existing_rows=existing.rows if existing is not None else None,
        open_year=target_end_date[:4],
        planned_action="replace_verified_latest_candidate" if existing is not None else "copy_new_latest",
        reason=f"stable latest {bucket_kind} refresh",
    )


def _summary(
    task: Dwh4TushareTask,
    *,
    rows: tuple[IncrementalPlanRow, ...],
    latest_open_trading_day: str,
    target_end_date: str,
    inventory: DriveInventoryResult,
) -> dict[str, Any]:
    by_kind: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    for row in rows:
        by_kind[row.plan_kind] = by_kind.get(row.plan_kind, 0) + 1
        action_counts[row.planned_action] = action_counts.get(row.planned_action, 0) + 1
    open_year = target_end_date[:4]
    closed_year_assets = [
        asset
        for asset in inventory.assets
        if asset.dataset_version == task.dataset_version and asset.bucket_kind == "year" and asset.bucket_value < open_year
    ]
    return {
        "workflow_name": task.workflow_name,
        "provider": task.provider,
        "dataset_version": task.dataset_version,
        "latest_open_trading_day": latest_open_trading_day,
        "target_end_date": target_end_date,
        "data_lag_trading_days": task.incremental_policy.data_lag_trading_days if task.incremental_policy is not None else None,
        "plan_row_count": len(rows),
        "api_count": len({row.api_name for row in rows}),
        "plan_kind_counts": dict(sorted(by_kind.items())),
        "planned_action_counts": dict(sorted(action_counts.items())),
        "closed_year_asset_count": len(closed_year_assets),
        "open_year": open_year,
        "incremental_merge_executed": False,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }


def build_incremental_plan(
    task: Dwh4TushareTask,
    inventory: DriveInventoryResult,
    *,
    latest_open_trading_day: str | None = None,
    registry_path: str | Path | None = None,
) -> IncrementalPlanResult:
    """Build per-API incremental fetch windows without fetching or mutating data."""
    if task.incremental_policy is None or not task.incremental_policy.enabled:
        raise ValueError("DWH4.1 incremental planner requires enabled incremental_policy")
    latest_open, target_end = _target_end_date(task, latest_open_trading_day=latest_open_trading_day)
    registry = Path(registry_path) if registry_path is not None else task.execution_repo / "configs" / "tushare" / "source_registry.yaml"
    specs = source_specs_by_api(registry)
    stable = task.incremental_policy.stable_latest_policy
    range_apis = set(stable.range_apis if stable.enabled else ())
    snapshot_apis = set(stable.snapshot_apis if stable.enabled else ())
    range_bucket_kind, range_bucket_value = _split_bucket_spec(stable.range_bucket, label="stable_latest_policy.range_bucket")
    snapshot_bucket_kind, snapshot_bucket_value = _split_bucket_spec(stable.snapshot_bucket, label="stable_latest_policy.snapshot_bucket")
    rows: list[IncrementalPlanRow] = []
    for api_name in task.api_names:
        api_assets = _assets_for_api(inventory, api_name)
        family = _source_family(api_name, specs, api_assets)
        query_mode = _query_mode(api_name, specs)
        if api_name in range_apis:
            rows.append(
                _plan_stable_latest(
                    task,
                    api_name,
                    api_assets,
                    source_family=family,
                    query_mode=query_mode,
                    target_end_date=target_end,
                    bucket_kind=range_bucket_kind,
                    bucket_value=range_bucket_value,
                    plan_kind="stable_latest_range",
                )
            )
        elif api_name in snapshot_apis:
            rows.append(
                _plan_stable_latest(
                    task,
                    api_name,
                    api_assets,
                    source_family=family,
                    query_mode=query_mode,
                    target_end_date=target_end,
                    bucket_kind=snapshot_bucket_kind,
                    bucket_value=snapshot_bucket_value,
                    plan_kind="stable_latest_snapshot",
                )
            )
        else:
            rows.append(
                _plan_by_trade_date(
                    task,
                    api_name,
                    api_assets,
                    source_family=family,
                    query_mode=query_mode,
                    target_end_date=target_end,
                )
            )
    row_tuple = tuple(rows)
    summary = _summary(task, rows=row_tuple, latest_open_trading_day=latest_open, target_end_date=target_end, inventory=inventory)
    return IncrementalPlanResult(
        workflow_name=task.workflow_name,
        dataset_version=task.dataset_version,
        latest_open_trading_day=latest_open,
        target_end_date=target_end,
        rows=row_tuple,
        summary=summary,
    )


def incremental_plan_rows(result: IncrementalPlanResult) -> list[dict[str, object]]:
    """Return JSON/CSV-safe incremental plan rows."""
    return [asdict(row) for row in result.rows]


def write_incremental_plan_artifacts(result: IncrementalPlanResult, artifact_root: str | Path) -> dict[str, Path]:
    """Write incremental_plan.csv and incremental_plan_summary.json to a local artifact directory."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    plan_path = root / "incremental_plan.csv"
    summary_path = root / "incremental_plan_summary.json"
    rows = incremental_plan_rows(result)
    with plan_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in PLAN_COLUMNS})
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "incremental_plan": plan_path,
        "incremental_plan_summary": summary_path,
    }
