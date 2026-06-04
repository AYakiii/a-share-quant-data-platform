from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

RAW_RELATIVE_ROOT = Path("data") / "raw" / "akshare"
COMPACT_ROOT_PARENT = Path("outputs") / "raw_acquisition_compact"
TIME_KEYS = ("snapshot", "year", "trade_date", "report_date", "date", "start_date", "end_date", "since_date")
REVIEW_REQUIRED_BUCKET_KINDS = {"scope", "snapshot"}


@dataclass(frozen=True)
class RawAsset:
    """One already-landed local Raw parquet asset."""

    output_root: str
    path: str
    relative_path: str
    source_family: str
    api_name: str
    partitions: dict[str, str]
    rows: int
    columns: list[str]
    sha256: str


@dataclass(frozen=True)
class ClassifiedRawAsset:
    """A Raw asset with its lineage-derived compact bucket."""

    asset: RawAsset
    bucket_kind: str
    bucket_value: str


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def file_sha256(path: str | Path) -> str:
    """Compute a file-level SHA-256 digest."""
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_partition_segments(relative_path: str | Path) -> dict[str, str]:
    """Parse key=value physical partition path segments from a Raw relative path."""
    parts = Path(relative_path).parts
    partitions: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key:
            partitions[key] = value
    return partitions


def _digits(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip() if ch.isdigit())


def classify_bucket(partitions: dict[str, str], *, start_date: str, end_date: str) -> tuple[str, str]:
    """Classify a compact bucket using only Raw partition lineage."""
    if partitions.get("snapshot"):
        return "snapshot", str(partitions["snapshot"])
    if partitions.get("year"):
        return "year", _digits(partitions["year"])[:4]
    for key in ("trade_date", "report_date", "date"):
        if partitions.get(key):
            value = _digits(partitions[key])
            if len(value) < 4:
                raise ValueError(f"partition {key} does not contain a year: {partitions[key]!r}")
            return "year", value[:4]
    if partitions.get("start_date") and partitions.get("end_date"):
        start = _digits(partitions["start_date"])
        end = _digits(partitions["end_date"])
        if len(start) < 4 or len(end) < 4:
            raise ValueError("start_date/end_date partitions must contain years")
        if start[:4] == end[:4]:
            return "year", start[:4]
        return "window", f"{start}_{end}"
    if partitions.get("since_date"):
        value = _digits(partitions["since_date"])
        if len(value) != 8:
            raise ValueError(f"since_date partition must be YYYYMMDD: {partitions['since_date']!r}")
        return "since", value
    return "scope", f"run_{start_date}_{end_date}"


def scan_raw_assets(output_root: str | Path) -> list[RawAsset]:
    """Scan already-landed Raw parquet files below <output_root>/data/raw/akshare."""
    root = Path(output_root)
    raw_root = root / RAW_RELATIVE_ROOT
    if not raw_root.exists():
        return []
    assets: list[RawAsset] = []
    for data_path in sorted(raw_root.rglob("data.parquet")):
        rel = data_path.relative_to(root)
        rel_parts = rel.parts
        if len(rel_parts) < 6:
            raise ValueError(f"raw parquet path is too shallow: {data_path}")
        source_family = rel_parts[3]
        api_name = rel_parts[4]
        partitions = parse_partition_segments(Path(*rel_parts[5:-1]))
        df = pd.read_parquet(data_path)
        assets.append(
            RawAsset(
                output_root=str(root),
                path=str(data_path),
                relative_path=str(rel),
                source_family=source_family,
                api_name=api_name,
                partitions=partitions,
                rows=int(len(df)),
                columns=[str(c) for c in df.columns],
                sha256=file_sha256(data_path),
            )
        )
    return assets


def classify_raw_assets(assets: Iterable[RawAsset], *, start_date: str, end_date: str) -> list[ClassifiedRawAsset]:
    """Classify Raw assets into lineage-derived compact buckets."""
    out: list[ClassifiedRawAsset] = []
    for asset in assets:
        kind, value = classify_bucket(asset.partitions, start_date=start_date, end_date=end_date)
        out.append(ClassifiedRawAsset(asset=asset, bucket_kind=kind, bucket_value=value))
    return out


def _bucket_path(package_root: Path, source_family: str, api_name: str, bucket_kind: str, bucket_value: str) -> Path:
    return package_root / RAW_RELATIVE_ROOT / source_family / api_name / f"{bucket_kind}={bucket_value}" / "data.parquet"


def _infer_window(output_root: Path) -> tuple[str, str]:
    text = output_root.name
    import re

    m = re.search(r"(\d{8})_(\d{8})", text)
    if m:
        return m.group(1), m.group(2)
    return "unknown", "unknown"


def _read_failed_backlog(output_root: Path) -> list[dict[str, Any]]:
    candidates = [
        output_root / "_operation_review" / "recovery_tasks.csv",
        output_root / "p0_recovery_tasks.csv",
        output_root / "recovery_tasks.csv",
    ]
    frames: list[pd.DataFrame] = []
    for path in candidates:
        if path.exists():
            frame = pd.read_csv(path)
            if not frame.empty:
                frame["backlog_source"] = str(path.relative_to(output_root))
                frames.append(frame)
    if not frames:
        return []
    return pd.concat(frames, ignore_index=True).fillna("").to_dict("records")


def _write_empty_or_rows_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=columns)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def compact_raw_lake(output_root: str | Path, package_root: str | Path | None = None, *, promotion_name: str | None = None, start_date: str | None = None, end_date: str | None = None) -> dict[str, Any]:
    """Compact local Raw parquet files into one parquet per source_family/api_name/bucket.

    The compaction is inventory-driven and lineage-driven. It does not deduplicate,
    normalize, delete rows, or delete columns.
    """
    out_root = Path(output_root)
    inferred_start, inferred_end = _infer_window(out_root)
    start = start_date or inferred_start
    end = end_date or inferred_end
    name = promotion_name or f"raw_lake_{out_root.name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    pkg = Path(package_root) if package_root is not None else COMPACT_ROOT_PARENT / name
    if pkg.exists():
        shutil.rmtree(pkg)
    pkg.mkdir(parents=True, exist_ok=True)

    assets = scan_raw_assets(out_root)
    classified = classify_raw_assets(assets, start_date=start, end_date=end)

    inventory_rows: list[dict[str, Any]] = []
    class_rows: list[dict[str, Any]] = []
    lineage_rows: list[dict[str, Any]] = []
    qa_rows: list[dict[str, Any]] = []
    manifest_assets: list[dict[str, Any]] = []

    for item in classified:
        a = item.asset
        inventory_rows.append({**asdict(a), "partitions": json.dumps(a.partitions, ensure_ascii=False), "columns": json.dumps(a.columns, ensure_ascii=False)})
        class_rows.append({"relative_path": a.relative_path, "source_family": a.source_family, "api_name": a.api_name, "bucket_kind": item.bucket_kind, "bucket_value": item.bucket_value, "partitions": json.dumps(a.partitions, ensure_ascii=False)})

    groups: dict[tuple[str, str, str, str], list[ClassifiedRawAsset]] = {}
    for item in classified:
        key = (item.asset.source_family, item.asset.api_name, item.bucket_kind, item.bucket_value)
        groups.setdefault(key, []).append(item)

    for (source_family, api_name, bucket_kind, bucket_value), items in sorted(groups.items()):
        frames: list[pd.DataFrame] = []
        expected_rows = 0
        expected_columns: list[str] | None = None
        source_files: list[str] = []
        for item in items:
            df = pd.read_parquet(item.asset.path)
            cols = [str(c) for c in df.columns]
            if expected_columns is None:
                expected_columns = cols
            elif cols != expected_columns:
                raise ValueError(f"column mismatch within compact bucket {source_family}/{api_name}/{bucket_kind}={bucket_value}")
            frames.append(df)
            expected_rows += len(df)
            source_files.append(item.asset.relative_path)
        compact_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=expected_columns or [])
        if len(compact_df) != expected_rows:
            raise ValueError("compact row preservation check failed before write")
        dst = _bucket_path(pkg, source_family, api_name, bucket_kind, bucket_value)
        dst.parent.mkdir(parents=True, exist_ok=True)
        compact_df.to_parquet(dst, index=False)
        reopened = pd.read_parquet(dst)
        rows = int(len(reopened))
        columns = [str(c) for c in reopened.columns]
        digest = file_sha256(dst)
        ok = rows == expected_rows and columns == (expected_columns or [])
        rel_dst = str(dst.relative_to(pkg))
        qa_rows.append({"relative_path": rel_dst, "source_family": source_family, "api_name": api_name, "bucket_kind": bucket_kind, "bucket_value": bucket_value, "expected_rows": expected_rows, "actual_rows": rows, "expected_columns": len(expected_columns or []), "actual_columns": len(columns), "sha256": digest, "ok": ok})
        if not ok:
            raise ValueError(f"compact QA failed for {rel_dst}")
        for item in items:
            lineage_rows.append({"compact_relative_path": rel_dst, "source_relative_path": item.asset.relative_path, "source_sha256": item.asset.sha256, "source_rows": item.asset.rows, "source_columns": json.dumps(item.asset.columns, ensure_ascii=False), "bucket_kind": bucket_kind, "bucket_value": bucket_value})
        manifest_assets.append({"relative_path": rel_dst, "source_family": source_family, "api_name": api_name, "bucket_kind": bucket_kind, "bucket_value": bucket_value, "rows": rows, "columns": columns, "column_count": len(columns), "sha256": digest, "source_files": source_files})

    failed_backlog = _read_failed_backlog(out_root)
    known_gap_manifest = {"policy": "failed_tasks_preserved_as_recovery_backlog", "failed_backlog_tasks": failed_backlog}
    manifest: dict[str, Any] = {
        "promotion_name": name,
        "package_root": str(pkg),
        "output_root": str(out_root),
        "acquisition_window": {"start_date": start, "end_date": end},
        "created_at": datetime.now(UTC).isoformat(),
        "compact_assets": manifest_assets,
        "total_rows": int(sum(int(a["rows"]) for a in manifest_assets)),
        "failed_backlog_tasks": failed_backlog,
        "known_gap_policy": known_gap_manifest["policy"],
        "review_required_bucket_kinds": sorted({a["bucket_kind"] for a in manifest_assets if a["bucket_kind"] in REVIEW_REQUIRED_BUCKET_KINDS}),
    }

    _write_empty_or_rows_csv(pkg / "raw_asset_inventory.csv", inventory_rows, ["output_root", "path", "relative_path", "source_family", "api_name", "partitions", "rows", "columns", "sha256"])
    _write_empty_or_rows_csv(pkg / "raw_compact_classification.csv", class_rows, ["relative_path", "source_family", "api_name", "bucket_kind", "bucket_value", "partitions"])
    _write_empty_or_rows_csv(pkg / "compact_source_lineage.csv", lineage_rows, ["compact_relative_path", "source_relative_path", "source_sha256", "source_rows", "source_columns", "bucket_kind", "bucket_value"])
    _write_empty_or_rows_csv(pkg / "compact_qa_report.csv", qa_rows, ["relative_path", "source_family", "api_name", "bucket_kind", "bucket_value", "expected_rows", "actual_rows", "expected_columns", "actual_columns", "sha256", "ok"])
    _write_json(pkg / "known_gap_manifest.json", known_gap_manifest)
    _write_json(pkg / "compact_manifest.json", manifest)
    (pkg / "_LOCAL_COMPACT_READY.txt").write_text("local compact QA passed\n", encoding="utf-8")
    return manifest


def load_manifest(package_root: str | Path) -> dict[str, Any]:
    return json.loads((Path(package_root) / "compact_manifest.json").read_text(encoding="utf-8"))


def verify_parquet_asset(path: str | Path, *, expected_rows: int, expected_columns: list[str], expected_sha256: str) -> None:
    df = pd.read_parquet(path)
    if int(len(df)) != int(expected_rows):
        raise ValueError(f"row count mismatch for {path}: {len(df)} != {expected_rows}")
    columns = [str(c) for c in df.columns]
    if columns != list(expected_columns):
        raise ValueError(f"column mismatch for {path}: {columns} != {expected_columns}")
    actual_sha = file_sha256(path)
    if actual_sha != expected_sha256:
        raise ValueError(f"sha256 mismatch for {path}: {actual_sha} != {expected_sha256}")
