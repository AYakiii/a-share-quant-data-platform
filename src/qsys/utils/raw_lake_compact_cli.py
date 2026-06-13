from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsys.data.factor_lake.raw_compact import (
    COMPACT_ROOT_PARENT,
    DRIVE_RAW_RELATIVE_ROOT,
    LOCAL_COMPACT_ASSET_RELATIVE_ROOT,
    compact_asset_relative_root,
    drive_raw_relative_root,
    REVIEW_REQUIRED_BUCKET_KINDS,
    assert_path_within,
    compact_raw_lake,
    file_sha256,
    load_manifest,
    resolve_compact_parent,
    validate_promotion_name,
    verify_parquet_asset,
)

IMMUTABLE_CATALOG_ARTIFACTS = [
    "compact_manifest.json",
    "compact_qa_report.csv",
    "raw_asset_inventory.csv",
    "compact_source_lineage.csv",
    "known_gap_manifest.json",
    "raw_compact_classification.csv",
    "READY_FOR_PROMOTION.json",
    "_LOCAL_COMPACT_READY.txt",
]

COLLISION_PLAN_COLUMNS = [
    "source_family",
    "api_name",
    "bucket_kind",
    "bucket_value",
    "rows",
    "relative_path",
    "source_path",
    "drive_path",
    "exists_on_drive",
    "identical",
    "action",
    "source_sha256",
    "drive_sha256",
]

DRIVE_ROOT_CHANGED_ERROR = "Drive DWH root differs from the operator-reviewed prepare plan. Rerun prepare for the new Drive root before promotion."


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _require_drive_root(path: str | Path) -> Path:
    root = Path(path).resolve()
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Drive DWH root is unavailable: {root}")
    # Colab Drive-like paths must be backed by the mounted /content/gdrive root.
    try:
        root.relative_to(Path("/content/gdrive"))
    except ValueError:
        return root
    if not os.path.ismount("/content/gdrive"):
        raise FileNotFoundError("Drive root is under /content/gdrive but /content/gdrive is not an active mountpoint; mount Drive explicitly before running")
    return root


def _resolve_package_root(package_root: str | Path) -> Path:
    return assert_path_within(Path(package_root), resolve_compact_parent(), label="package_root")


def _safe_manifest_relative_path(relative_path: str, expected_root: Path) -> Path:
    rel = Path(relative_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"unsafe compact asset relative path: {relative_path}")
    if rel.parts[: len(expected_root.parts)] != expected_root.parts:
        raise ValueError(f"compact asset must be under {expected_root}: {relative_path}")
    return rel


def _package_asset_path(package_root: Path, relative_path: str, expected_root: Path | None = None) -> Path:
    rel = _safe_manifest_relative_path(relative_path, expected_root or LOCAL_COMPACT_ASSET_RELATIVE_ROOT)
    return assert_path_within(package_root / rel, package_root, label="local compact asset")


def _drive_raw_path(drive_root: Path, relative_path: str, expected_root: Path | None = None) -> Path:
    root_rel = expected_root or DRIVE_RAW_RELATIVE_ROOT
    rel = _safe_manifest_relative_path(relative_path, root_rel)
    raw_root = drive_root / root_rel
    return assert_path_within(drive_root / rel, raw_root, label="Drive Raw target")


def _catalog_dir(drive_root: Path, promotion_name: str) -> Path:
    name = validate_promotion_name(promotion_name)
    return drive_root / "catalog" / "promotions" / name


def _validate_local_package(package_root: Path, manifest: dict[str, Any]) -> None:
    for asset in manifest.get("compact_assets", []):
        expected_root = Path(str(manifest.get("local_compact_asset_relative_root", LOCAL_COMPACT_ASSET_RELATIVE_ROOT)))
        src = _package_asset_path(package_root, str(asset["relative_path"]), expected_root)
        verify_parquet_asset(src, expected_rows=int(asset["rows"]), expected_columns=list(asset["columns"]), expected_sha256=str(asset["sha256"]))


def build_collision_plan(package_root: str | Path, drive_dwh_root: str | Path) -> list[dict[str, Any]]:
    pkg = _resolve_package_root(package_root)
    manifest = load_manifest(pkg)
    drive_root = _require_drive_root(drive_dwh_root)
    rows: list[dict[str, Any]] = []
    for asset in manifest.get("compact_assets", []):
        rel = str(asset["relative_path"])
        compact_root = Path(str(manifest.get("local_compact_asset_relative_root", LOCAL_COMPACT_ASSET_RELATIVE_ROOT)))
        drive_raw_root = Path(str(manifest.get("drive_raw_relative_root", DRIVE_RAW_RELATIVE_ROOT)))
        src = _package_asset_path(pkg, rel, compact_root)
        dst = _drive_raw_path(drive_root, rel, drive_raw_root)
        src_sha = file_sha256(src)
        if not dst.exists():
            action = "copy_new"
            dst_sha = ""
            identical = False
        else:
            dst_sha = file_sha256(dst)
            identical = src_sha == dst_sha
            action = "skip_identical" if identical else "block_non_identical"
        rows.append({"source_family": asset.get("source_family", ""), "api_name": asset.get("api_name", ""), "bucket_kind": asset.get("bucket_kind", ""), "bucket_value": asset.get("bucket_value", ""), "rows": int(asset.get("rows", 0)), "relative_path": rel, "source_path": str(src), "drive_path": str(dst), "exists_on_drive": dst.exists(), "identical": identical, "action": action, "source_sha256": src_sha, "drive_sha256": dst_sha})
    return rows


def write_collision_plan(package_root: str | Path, drive_dwh_root: str | Path) -> list[dict[str, Any]]:
    pkg = _resolve_package_root(package_root)
    rows = build_collision_plan(pkg, drive_dwh_root)
    pd.DataFrame(rows, columns=COLLISION_PLAN_COLUMNS).to_csv(pkg / "drive_collision_plan.csv", index=False, encoding="utf-8-sig")
    return rows


def _bucket_asset_counts(assets: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for asset in assets:
        key = f"{asset.get('bucket_kind')}={asset.get('bucket_value')}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _planned_count(collision_rows: list[dict[str, Any]], action: str) -> int:
    return sum(1 for row in collision_rows if row.get("action") == action)


def _ready_payload(manifest: dict[str, Any], collision_rows: list[dict[str, Any]], *, drive_root: Path, package_root: Path, collision_plan_path: Path) -> dict[str, Any]:
    assets = list(manifest.get("compact_assets", []))
    blocked = [r for r in collision_rows if r.get("action") == "block_non_identical"]
    promotion_name = validate_promotion_name(str(manifest["promotion_name"]))
    return {
        "promotion_name": manifest["promotion_name"],
        "package_root": manifest["package_root"],
        "output_root": manifest["output_root"],
        "acquisition_window": manifest.get("acquisition_window", {}),
        "compact_assets": assets,
        "total_rows": int(manifest.get("total_rows", 0)),
        "failed_backlog_task_count": int(manifest.get("failed_backlog_task_count", 0)),
        "failed_backlog_tasks": manifest.get("failed_backlog_tasks", []),
        "known_gap_policy": manifest.get("known_gap_policy", ""),
        "blocked_collisions": blocked,
        "bucket_asset_counts": _bucket_asset_counts(assets),
        "review_required_bucket_kinds": sorted({a.get("bucket_kind") for a in assets if a.get("bucket_kind") in REVIEW_REQUIRED_BUCKET_KINDS}),
        "prepared_drive_dwh_root": str(drive_root.resolve()),
        "provider": manifest.get("provider", "akshare"),
        "storage_schema_version": manifest.get("storage_schema_version", "v1"),
        "prepared_drive_raw_root": str((drive_root / Path(str(manifest.get("drive_raw_relative_root", DRIVE_RAW_RELATIVE_ROOT)))).resolve()),
        "prepared_drive_catalog_root": str(_catalog_dir(drive_root, promotion_name).resolve()),
        "drive_collision_plan_path": str(collision_plan_path.resolve()),
        "drive_collision_plan_sha256": file_sha256(collision_plan_path),
        "planned_asset_count": len(collision_rows),
        "planned_copy_new_count": _planned_count(collision_rows, "copy_new"),
        "planned_skip_identical_count": _planned_count(collision_rows, "skip_identical"),
        "planned_block_non_identical_count": _planned_count(collision_rows, "block_non_identical"),
        "ready_for_promotion": not blocked,
    }


def _prepare_review_summary(ready: dict[str, Any]) -> dict[str, Any]:
    return {
        key: ready[key]
        for key in [
            "promotion_name",
            "package_root",
            "prepared_drive_dwh_root",
            "prepared_drive_raw_root",
            "prepared_drive_catalog_root",
            "drive_collision_plan_path",
            "drive_collision_plan_sha256",
            "planned_asset_count",
            "planned_copy_new_count",
            "planned_skip_identical_count",
            "planned_block_non_identical_count",
            "ready_for_promotion",
        ]
    }


def prepare(args: argparse.Namespace) -> int:
    drive_root = _require_drive_root(args.drive_dwh_root)
    promotion_name = validate_promotion_name(args.promotion_name) if args.promotion_name is not None else None
    manifest = compact_raw_lake(args.output_root, promotion_name=promotion_name, start_date=args.start_date, end_date=args.end_date, replace_existing=bool(args.replace_local_package), provider=args.provider, storage_schema_version=args.storage_schema_version)
    package_root = _resolve_package_root(manifest["package_root"])
    collisions = write_collision_plan(package_root, drive_root)
    collision_plan_path = package_root / "drive_collision_plan.csv"
    ready = _ready_payload(manifest, collisions, drive_root=drive_root, package_root=package_root, collision_plan_path=collision_plan_path)
    _write_json(package_root / "READY_FOR_PROMOTION.json", ready)
    print(json.dumps(_prepare_review_summary(ready), ensure_ascii=False, indent=2))
    return 0


def _load_ready(package_root: Path) -> dict[str, Any]:
    path = package_root / "READY_FOR_PROMOTION.json"
    if not path.exists():
        raise FileNotFoundError(f"READY_FOR_PROMOTION.json is required: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_reviewed(value: str | None) -> set[str]:
    return {part.strip() for part in (value or "").split(",") if part.strip()}


def _validate_ready_against_manifest(ready: dict[str, Any], manifest: dict[str, Any], package_root: Path) -> None:
    if ready.get("promotion_name") != manifest.get("promotion_name"):
        raise ValueError("READY_FOR_PROMOTION.json promotion_name does not match compact_manifest.json")
    if Path(str(ready.get("package_root", ""))).resolve() != package_root.resolve() or Path(str(manifest.get("package_root", ""))).resolve() != package_root.resolve():
        raise ValueError("READY_FOR_PROMOTION.json package_root does not match compact_manifest.json and CLI package root")
    if ready.get("ready_for_promotion") is not True:
        raise ValueError("READY_FOR_PROMOTION.json does not mark this package ready_for_promotion=true")


def _validate_prepared_drive_root(ready: dict[str, Any], drive_root: Path) -> None:
    prepared = Path(str(ready.get("prepared_drive_dwh_root", ""))).resolve()
    if prepared != drive_root.resolve():
        raise ValueError(DRIVE_ROOT_CHANGED_ERROR)


def _load_reviewed_collision_plan(ready: dict[str, Any], package_root: Path) -> pd.DataFrame:
    plan_path = Path(str(ready.get("drive_collision_plan_path", ""))).resolve()
    if plan_path != (package_root / "drive_collision_plan.csv").resolve():
        raise ValueError("READY_FOR_PROMOTION.json drive_collision_plan_path does not match the local package plan")
    expected_sha = str(ready.get("drive_collision_plan_sha256", ""))
    if not plan_path.exists() or not expected_sha:
        raise FileNotFoundError("operator-reviewed drive_collision_plan.csv and SHA256 are required before promotion")
    actual_sha = file_sha256(plan_path)
    if actual_sha != expected_sha:
        raise ValueError("operator-reviewed drive_collision_plan.csv SHA256 mismatch. Rerun prepare before promotion.")
    return pd.read_csv(plan_path).fillna("")


def _target_path_set(rows: list[dict[str, Any]] | pd.DataFrame) -> set[str]:
    if isinstance(rows, pd.DataFrame):
        return {str(v) for v in rows.get("drive_path", pd.Series(dtype=str)).tolist()}
    return {str(row.get("drive_path", "")) for row in rows}


def _plan_state_signature(rows: list[dict[str, Any]] | pd.DataFrame) -> set[tuple[str, str, str, str, str]]:
    if isinstance(rows, pd.DataFrame):
        records = rows.to_dict("records")
    else:
        records = rows
    return {
        (
            str(row.get("drive_path", "")),
            str(row.get("action", "")),
            str(row.get("exists_on_drive", "")),
            str(row.get("identical", "")),
            str(row.get("drive_sha256", "")),
        )
        for row in records
    }


def _validate_current_plan_matches_reviewed(reviewed_plan: pd.DataFrame, current_rows: list[dict[str, Any]]) -> None:
    if _target_path_set(reviewed_plan) != _target_path_set(current_rows):
        raise ValueError("Current Drive target path set differs from the operator-reviewed prepare plan. Rerun prepare before promotion.")
    if _plan_state_signature(reviewed_plan) != _plan_state_signature(current_rows):
        raise ValueError("Current Drive collision plan differs from the operator-reviewed prepare plan. Rerun prepare before promotion.")


def _copy_immutable_file(src: Path, dst: Path) -> str:
    if not src.exists():
        return "missing_source"
    if dst.exists():
        if file_sha256(src) == file_sha256(dst):
            return "skip_identical"
        raise FileExistsError(f"non-identical Drive catalog artifact overwrite is forbidden: {dst}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return "copy_new"



def _check_immutable_catalog_artifacts(package_root: Path, drive_root: Path, promotion_name: str) -> None:
    target = _catalog_dir(drive_root, promotion_name)
    for filename in IMMUTABLE_CATALOG_ARTIFACTS:
        src = package_root / filename
        dst = target / filename
        if src.exists() and dst.exists() and file_sha256(src) != file_sha256(dst):
            raise FileExistsError(f"non-identical Drive catalog artifact overwrite is forbidden: {dst}")

def _copy_audit_artifacts(package_root: Path, drive_root: Path, promotion_name: str, collision_plan_path: Path) -> dict[str, str]:
    target = _catalog_dir(drive_root, promotion_name)
    target.mkdir(parents=True, exist_ok=True)
    actions: dict[str, str] = {}
    for filename in IMMUTABLE_CATALOG_ARTIFACTS:
        src = package_root / filename
        if src.exists():
            actions[filename] = _copy_immutable_file(src, target / filename)
    actions[collision_plan_path.name] = _copy_immutable_file(collision_plan_path, target / collision_plan_path.name)
    return actions


def promote(args: argparse.Namespace) -> int:
    package_root = _resolve_package_root(args.package_root)
    drive_root = _require_drive_root(args.drive_dwh_root)
    ready = _load_ready(package_root)
    manifest = load_manifest(package_root)
    promotion_name = validate_promotion_name(str(manifest["promotion_name"]))
    _validate_ready_against_manifest(ready, manifest, package_root)
    _validate_prepared_drive_root(ready, drive_root)
    reviewed_plan = _load_reviewed_collision_plan(ready, package_root)
    if not args.confirm_promotion:
        raise ValueError("--confirm-promotion is required")
    if args.confirm_promotion != promotion_name:
        raise ValueError("--confirm-promotion must exactly match promotion_name")

    _validate_local_package(package_root, manifest)

    required_review = sorted({a.get("bucket_kind") for a in manifest.get("compact_assets", []) if a.get("bucket_kind") in REVIEW_REQUIRED_BUCKET_KINDS})
    allowed_review = _parse_reviewed(args.allow_reviewed_bucket_kinds)
    missing_review = sorted(set(required_review) - allowed_review)
    if missing_review:
        raise ValueError(f"bucket kinds require explicit review opt-in: {missing_review}")

    _check_immutable_catalog_artifacts(package_root, drive_root, promotion_name)

    collisions = build_collision_plan(package_root, drive_root)
    _validate_current_plan_matches_reviewed(reviewed_plan, collisions)
    blocked = [r for r in collisions if r["action"] == "block_non_identical"]
    if blocked:
        raise FileExistsError("non-identical Drive overwrite is forbidden")

    copied: list[str] = []
    skipped: list[str] = []
    for row in collisions:
        src = Path(row["source_path"])
        dst = Path(row["drive_path"])
        compact_root = Path(str(manifest.get("local_compact_asset_relative_root", LOCAL_COMPACT_ASSET_RELATIVE_ROOT)))
        drive_raw_root = Path(str(manifest.get("drive_raw_relative_root", DRIVE_RAW_RELATIVE_ROOT)))
        _package_asset_path(package_root, row["relative_path"], compact_root)
        _drive_raw_path(drive_root, row["relative_path"], drive_raw_root)
        if row["action"] == "copy_new":
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(row["relative_path"])
        elif row["action"] == "skip_identical":
            skipped.append(row["relative_path"])
        else:
            raise FileExistsError(f"unexpected blocked collision: {row['relative_path']}")

    for asset in manifest.get("compact_assets", []):
        verify_parquet_asset(_drive_raw_path(drive_root, asset["relative_path"], Path(str(manifest.get("drive_raw_relative_root", DRIVE_RAW_RELATIVE_ROOT)))), expected_rows=int(asset["rows"]), expected_columns=list(asset["columns"]), expected_sha256=str(asset["sha256"]))

    attempt_name = datetime.now(UTC).strftime("promotion_attempt_%Y%m%dT%H%M%S%fZ")
    report = {"promotion_name": promotion_name, "promoted_at": datetime.now(UTC).isoformat(), "copied": copied, "skipped_identical": skipped, "verified_assets": len(manifest.get("compact_assets", [])), "review_required_bucket_kinds": required_review}
    report_path = package_root / f"{attempt_name}.json"
    collision_attempt_path = package_root / f"{attempt_name}_drive_collision_plan.csv"
    pd.DataFrame(collisions).to_csv(collision_attempt_path, index=False, encoding="utf-8-sig")
    catalog_actions = _copy_audit_artifacts(package_root, drive_root, promotion_name, collision_attempt_path)
    report["drive_catalog_actions"] = catalog_actions
    _write_json(report_path, report)
    _copy_immutable_file(report_path, _catalog_dir(drive_root, promotion_name) / report_path.name)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def audit(args: argparse.Namespace) -> int:
    drive_root = _require_drive_root(args.drive_dwh_root)
    promotion_name = validate_promotion_name(args.promotion_name)
    promo_dir = _catalog_dir(drive_root, promotion_name)
    manifest = json.loads((promo_dir / "compact_manifest.json").read_text(encoding="utf-8"))
    summary: dict[str, int] = {}
    for asset in manifest.get("compact_assets", []):
        verify_parquet_asset(_drive_raw_path(drive_root, asset["relative_path"], Path(str(manifest.get("drive_raw_relative_root", DRIVE_RAW_RELATIVE_ROOT)))), expected_rows=int(asset["rows"]), expected_columns=list(asset["columns"]), expected_sha256=str(asset["sha256"]))
        kind = str(asset["bucket_kind"])
        summary[kind] = summary.get(kind, 0) + 1
    print("Bucket summary")
    for kind, count in sorted(summary.items()):
        print(f"{kind}: {count}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare, promote, and audit local Raw Lake compact packages.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("prepare", help="Build a local compact package and Drive collision dry-run plan; never writes Drive Raw parquet.")
    p.add_argument("--output-root", required=True)
    p.add_argument("--drive-dwh-root", required=True)
    p.add_argument("--promotion-name")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--replace-local-package", action="store_true", help=f"Replace an existing local package under {COMPACT_ROOT_PARENT}; never affects Drive")
    p.add_argument("--provider", default="akshare")
    p.add_argument("--storage-schema-version", default="v1")
    p.set_defaults(func=prepare)

    p = sub.add_parser("promote", help="Human-gated Drive promotion of a ready compact package.")
    p.add_argument("--package-root", required=True)
    p.add_argument("--drive-dwh-root", required=True)
    p.add_argument("--confirm-promotion")
    p.add_argument("--allow-reviewed-bucket-kinds", help="Comma-separated reviewed bucket kinds, e.g. scope,snapshot")
    p.set_defaults(func=promote)

    p = sub.add_parser("audit", help="Read-only independent Drive audit of promoted Raw parquet assets.")
    p.add_argument("--promotion-name", required=True)
    p.add_argument("--drive-dwh-root", required=True)
    p.set_defaults(func=audit)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
