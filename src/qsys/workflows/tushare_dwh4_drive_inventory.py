"""Read-only Drive inventory for DWH4.1 Tushare raw assets."""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import pandas as pd

from qsys.workflows.tushare_dwh4_task import Dwh4DriveInventoryPolicy, Dwh4TushareTask

DRIVE_RAW_RELATIVE_ROOT = PurePosixPath("raw/tushare")
DATE_COLUMN_CANDIDATES = (
    "trade_date",
    "cal_date",
    "suspend_date",
    "ann_date",
    "report_date",
    "date",
    "start_date",
    "end_date",
    "snapshot_date",
)
CSV_COLUMNS = [
    "drive_dwh_root",
    "path",
    "relative_path",
    "provider",
    "source_family",
    "api_name",
    "dataset_version",
    "bucket_kind",
    "bucket_value",
    "partitions",
    "rows",
    "columns",
    "column_count",
    "sha256",
    "size_bytes",
    "date_column",
    "min_date",
    "max_date",
    "snapshot_date",
    "metadata_exists",
    "metadata_keys",
    "status",
    "error_message",
]


@dataclass(frozen=True)
class DriveInventoryAsset:
    """One Drive raw/tushare parquet asset observed by a read-only scan."""

    drive_dwh_root: str
    path: str
    relative_path: str
    provider: str
    source_family: str
    api_name: str
    dataset_version: str
    bucket_kind: str
    bucket_value: str
    partitions: dict[str, str]
    rows: int | None
    columns: tuple[str, ...]
    column_count: int | None
    sha256: str
    size_bytes: int
    date_column: str
    min_date: str
    max_date: str
    snapshot_date: str
    metadata_exists: bool
    metadata_keys: tuple[str, ...]
    status: str
    error_message: str = ""


@dataclass(frozen=True)
class DriveInventoryResult:
    """Inventory rows plus a fixed-size summary payload."""

    drive_dwh_root: str
    dataset_version: str
    assets: tuple[DriveInventoryAsset, ...]
    summary: dict[str, Any]
    active_manifest: dict[str, Any] | None = None


def file_sha256(path: str | Path) -> str:
    """Compute a file-level SHA-256 digest without changing the file."""
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_relative_path(value: str) -> PurePosixPath:
    if not value or "\\" in value or ":" in value:
        raise ValueError(f"unsafe relative path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"unsafe relative path: {value!r}")
    return path


def _safe_join(root: Path, relative_path: str) -> Path:
    rel = _safe_relative_path(relative_path)
    resolved = (root / Path(*rel.parts)).resolve()
    resolved.relative_to(root.resolve())
    return resolved


def _parse_partition_segments(parts: Iterable[str]) -> dict[str, str]:
    partitions: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key:
            partitions[key] = value
    return partitions


def _bucket_from_partitions(partitions: dict[str, str]) -> tuple[str, str]:
    for key in ("year", "window", "snapshot", "scope", "since"):
        if partitions.get(key):
            return key, partitions[key]
    if partitions.get("start_date") and partitions.get("end_date"):
        return "window", f"{partitions['start_date']}_{partitions['end_date']}"
    for key in DATE_COLUMN_CANDIDATES:
        if partitions.get(key):
            value = "".join(ch for ch in partitions[key] if ch.isdigit())
            if len(value) >= 4:
                return "year", value[:4]
    return "unknown", "unknown"


def _normal_date(value: object) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _date_range(frame: pd.DataFrame, partitions: dict[str, str]) -> tuple[str, str, str, str]:
    for column in DATE_COLUMN_CANDIDATES:
        if column not in frame.columns:
            continue
        values = sorted({date for date in (_normal_date(value) for value in frame[column].dropna().tolist()) if date})
        if values:
            return column, values[0], values[-1], _normal_date(partitions.get("snapshot", ""))
    if partitions.get("window"):
        digits = [part for part in str(partitions["window"]).replace("-", "_").split("_") if _normal_date(part)]
        if len(digits) >= 2:
            return "partition.window", _normal_date(digits[0]), _normal_date(digits[-1]), ""
    if partitions.get("snapshot"):
        snapshot = _normal_date(partitions["snapshot"])
        return "partition.snapshot", snapshot, snapshot, snapshot
    if partitions.get("year"):
        year = "".join(ch for ch in str(partitions["year"]) if ch.isdigit())[:4]
        if year:
            return "partition.year", f"{year}0101", f"{year}1231", ""
    return "", "", "", ""


def parse_drive_asset_path(drive_dwh_root: str | Path, data_path: str | Path) -> dict[str, Any]:
    """Parse a canonical Drive raw/tushare data.parquet path."""
    root = Path(drive_dwh_root)
    path = Path(data_path)
    relative = path.relative_to(root).as_posix()
    parts = PurePosixPath(relative).parts
    if len(parts) < 7 or parts[:2] != DRIVE_RAW_RELATIVE_ROOT.parts or parts[-1] != "data.parquet":
        raise ValueError(f"not a canonical Drive raw/tushare asset: {relative}")
    partitions = _parse_partition_segments(parts[5:-1])
    bucket_kind, bucket_value = _bucket_from_partitions(partitions)
    return {
        "relative_path": relative,
        "provider": parts[1],
        "source_family": parts[2],
        "api_name": parts[3],
        "dataset_version": parts[4],
        "partitions": partitions,
        "bucket_kind": bucket_kind,
        "bucket_value": bucket_value,
    }


def _read_sidecar_metadata(data_path: Path) -> tuple[bool, tuple[str, ...]]:
    metadata_path = data_path.with_name("metadata.json")
    if not metadata_path.exists():
        return False, ()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return True, ()
    return True, tuple(sorted(str(key) for key in payload))


def _read_parquet_asset(
    data_path: Path,
    partitions: dict[str, str],
    *,
    read_parquet_metadata: bool,
) -> tuple[int | None, tuple[str, ...], int | None, str, str, str, str]:
    if not read_parquet_metadata:
        return None, (), None, "", "", "", ""
    frame = pd.read_parquet(data_path)
    columns = tuple(str(column) for column in frame.columns)
    date_column, min_date, max_date, snapshot_date = _date_range(frame, partitions)
    return int(len(frame)), columns, len(columns), date_column, min_date, max_date, snapshot_date


def _asset_from_path(
    drive_dwh_root: Path,
    data_path: Path,
    *,
    read_parquet_metadata: bool,
    compute_sha256: bool,
    fail_on_unreadable_existing_asset: bool,
) -> DriveInventoryAsset:
    parsed = parse_drive_asset_path(drive_dwh_root, data_path)
    metadata_exists, metadata_keys = _read_sidecar_metadata(data_path)
    rows: int | None = None
    columns: tuple[str, ...] = ()
    column_count: int | None = None
    date_column = ""
    min_date = ""
    max_date = ""
    snapshot_date = ""
    status = "ok"
    error_message = ""
    try:
        rows, columns, column_count, date_column, min_date, max_date, snapshot_date = _read_parquet_asset(
            data_path,
            parsed["partitions"],
            read_parquet_metadata=read_parquet_metadata,
        )
    except Exception as exc:  # noqa: BLE001 - inventory reports or raises according to policy.
        if fail_on_unreadable_existing_asset:
            raise ValueError(f"unreadable Drive asset: {data_path}") from exc
        status = "unreadable"
        error_message = f"{type(exc).__name__}: {exc}"
    return DriveInventoryAsset(
        drive_dwh_root=str(drive_dwh_root),
        path=str(data_path),
        relative_path=str(parsed["relative_path"]),
        provider=str(parsed["provider"]),
        source_family=str(parsed["source_family"]),
        api_name=str(parsed["api_name"]),
        dataset_version=str(parsed["dataset_version"]),
        bucket_kind=str(parsed["bucket_kind"]),
        bucket_value=str(parsed["bucket_value"]),
        partitions=dict(parsed["partitions"]),
        rows=rows,
        columns=columns,
        column_count=column_count,
        sha256=file_sha256(data_path) if compute_sha256 else "",
        size_bytes=data_path.stat().st_size,
        date_column=date_column,
        min_date=min_date,
        max_date=max_date,
        snapshot_date=snapshot_date,
        metadata_exists=metadata_exists,
        metadata_keys=metadata_keys,
        status=status,
        error_message=error_message,
    )


def read_active_manifest(drive_dwh_root: str | Path, active_manifest_path: str | None) -> dict[str, Any] | None:
    """Read a relative active manifest path if supplied and present."""
    if not active_manifest_path:
        return None
    root = Path(drive_dwh_root)
    path = _safe_join(root, active_manifest_path)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"active manifest must be a JSON object: {path}")
    return payload


def _summary(
    *,
    drive_dwh_root: Path,
    dataset_version: str,
    assets: tuple[DriveInventoryAsset, ...],
    ignored_dataset_version_count: int,
    active_manifest: dict[str, Any] | None,
) -> dict[str, Any]:
    api_counts: dict[str, int] = {}
    family_counts: dict[str, int] = {}
    bucket_counts: dict[str, int] = {}
    for asset in assets:
        api_counts[asset.api_name] = api_counts.get(asset.api_name, 0) + 1
        family_counts[asset.source_family] = family_counts.get(asset.source_family, 0) + 1
        bucket_counts[asset.bucket_kind] = bucket_counts.get(asset.bucket_kind, 0) + 1
    active_assets = active_manifest.get("active_assets", []) if isinstance(active_manifest, dict) else []
    return {
        "drive_dwh_root": str(drive_dwh_root),
        "dataset_version": dataset_version,
        "asset_count": len(assets),
        "ok_asset_count": sum(1 for asset in assets if asset.status == "ok"),
        "unreadable_asset_count": sum(1 for asset in assets if asset.status == "unreadable"),
        "total_rows": sum(int(asset.rows or 0) for asset in assets),
        "api_counts": dict(sorted(api_counts.items())),
        "source_family_counts": dict(sorted(family_counts.items())),
        "bucket_kind_counts": dict(sorted(bucket_counts.items())),
        "ignored_dataset_version_count": ignored_dataset_version_count,
        "active_manifest_exists": active_manifest is not None,
        "active_manifest_asset_count": len(active_assets) if isinstance(active_assets, list) else 0,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }


def build_drive_inventory(
    drive_dwh_root: str | Path,
    *,
    dataset_version: str,
    policy: Dwh4DriveInventoryPolicy | None = None,
    active_manifest_path: str | None = None,
) -> DriveInventoryResult:
    """Scan Drive raw/tushare assets in read-only mode."""
    root = Path(drive_dwh_root)
    scan_enabled = policy.scan_raw_tushare if policy is not None else True
    read_parquet_metadata = policy.read_parquet_metadata if policy is not None else True
    compute_digest = policy.compute_sha256 if policy is not None else True
    fail_on_unreadable = policy.fail_on_unreadable_existing_asset if policy is not None else True
    active_manifest = read_active_manifest(root, active_manifest_path)
    assets: list[DriveInventoryAsset] = []
    ignored_dataset_version_count = 0
    raw_root = root / Path(*DRIVE_RAW_RELATIVE_ROOT.parts)
    if scan_enabled and raw_root.exists():
        for data_path in sorted(raw_root.rglob("data.parquet")):
            parsed = parse_drive_asset_path(root, data_path)
            if parsed["dataset_version"] != dataset_version:
                ignored_dataset_version_count += 1
                continue
            assets.append(
                _asset_from_path(
                    root,
                    data_path,
                    read_parquet_metadata=read_parquet_metadata,
                    compute_sha256=compute_digest,
                    fail_on_unreadable_existing_asset=fail_on_unreadable,
                )
            )
    asset_tuple = tuple(assets)
    summary = _summary(
        drive_dwh_root=root,
        dataset_version=dataset_version,
        assets=asset_tuple,
        ignored_dataset_version_count=ignored_dataset_version_count,
        active_manifest=active_manifest,
    )
    return DriveInventoryResult(
        drive_dwh_root=str(root),
        dataset_version=dataset_version,
        assets=asset_tuple,
        summary=summary,
        active_manifest=active_manifest,
    )


def build_drive_inventory_for_task(task: Dwh4TushareTask) -> DriveInventoryResult:
    """Build inventory using the DWH4.1 policy fields attached to a task."""
    policy = task.drive_inventory_policy
    if policy is None or not policy.enabled:
        raise ValueError("DWH4.1 Drive inventory requires enabled drive_inventory_policy")
    active_manifest_path = None
    if task.incremental_policy is not None:
        active_manifest_path = task.incremental_policy.active_manifest_policy.active_manifest_path
    return build_drive_inventory(
        task.drive_dwh_root,
        dataset_version=task.dataset_version,
        policy=policy,
        active_manifest_path=active_manifest_path,
    )


def inventory_asset_row(asset: DriveInventoryAsset) -> dict[str, object]:
    """Return a CSV/JSON-safe inventory row."""
    row = asdict(asset)
    row["partitions"] = json.dumps(asset.partitions, ensure_ascii=False, sort_keys=True)
    row["columns"] = json.dumps(list(asset.columns), ensure_ascii=False)
    row["metadata_keys"] = json.dumps(list(asset.metadata_keys), ensure_ascii=False)
    return row


def drive_inventory_rows(result: DriveInventoryResult) -> list[dict[str, object]]:
    """Return inventory rows sorted by relative path."""
    return [inventory_asset_row(asset) for asset in sorted(result.assets, key=lambda item: item.relative_path)]


def write_drive_inventory_artifacts(result: DriveInventoryResult, artifact_root: str | Path) -> dict[str, Path]:
    """Write drive_inventory.csv and drive_inventory_summary.json to a local artifact directory."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    inventory_path = root / "drive_inventory.csv"
    summary_path = root / "drive_inventory_summary.json"
    rows = drive_inventory_rows(result)
    with inventory_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "drive_inventory": inventory_path,
        "drive_inventory_summary": summary_path,
    }
