from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qsys.data.factor_lake.raw_compact import (
    RAW_RELATIVE_ROOT,
    REVIEW_REQUIRED_BUCKET_KINDS,
    compact_raw_lake,
    file_sha256,
    load_manifest,
    verify_parquet_asset,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _require_drive_root(path: str | Path) -> Path:
    root = Path(path)
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Drive DWH root is unavailable: {root}")
    return root


def _drive_raw_path(drive_root: Path, relative_path: str) -> Path:
    return drive_root / Path(relative_path)


def build_collision_plan(package_root: str | Path, drive_dwh_root: str | Path) -> list[dict[str, Any]]:
    manifest = load_manifest(package_root)
    drive_root = _require_drive_root(drive_dwh_root)
    rows: list[dict[str, Any]] = []
    for asset in manifest.get("compact_assets", []):
        rel = str(asset["relative_path"])
        src = Path(package_root) / rel
        dst = _drive_raw_path(drive_root, rel)
        src_sha = file_sha256(src)
        if not dst.exists():
            action = "copy_new"
            dst_sha = ""
            identical = False
        else:
            dst_sha = file_sha256(dst)
            identical = src_sha == dst_sha
            action = "skip_identical" if identical else "block_non_identical"
        rows.append({"relative_path": rel, "source_path": str(src), "drive_path": str(dst), "source_sha256": src_sha, "drive_sha256": dst_sha, "exists_on_drive": dst.exists(), "identical": identical, "action": action})
    return rows


def write_collision_plan(package_root: str | Path, drive_dwh_root: str | Path) -> list[dict[str, Any]]:
    rows = build_collision_plan(package_root, drive_dwh_root)
    pd.DataFrame(rows, columns=["relative_path", "source_path", "drive_path", "source_sha256", "drive_sha256", "exists_on_drive", "identical", "action"]).to_csv(Path(package_root) / "drive_collision_plan.csv", index=False, encoding="utf-8-sig")
    return rows


def _bucket_asset_counts(assets: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for asset in assets:
        key = f"{asset.get('bucket_kind')}={asset.get('bucket_value')}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _ready_payload(manifest: dict[str, Any], collision_rows: list[dict[str, Any]]) -> dict[str, Any]:
    assets = list(manifest.get("compact_assets", []))
    blocked = [r for r in collision_rows if r.get("action") == "block_non_identical"]
    return {
        "promotion_name": manifest["promotion_name"],
        "package_root": manifest["package_root"],
        "output_root": manifest["output_root"],
        "acquisition_window": manifest.get("acquisition_window", {}),
        "compact_assets": assets,
        "total_rows": int(manifest.get("total_rows", 0)),
        "failed_backlog_tasks": manifest.get("failed_backlog_tasks", []),
        "known_gap_policy": manifest.get("known_gap_policy", ""),
        "blocked_collisions": blocked,
        "bucket_asset_counts": _bucket_asset_counts(assets),
        "review_required_bucket_kinds": sorted({a.get("bucket_kind") for a in assets if a.get("bucket_kind") in REVIEW_REQUIRED_BUCKET_KINDS}),
        "ready_for_promotion": not blocked,
    }


def prepare(args: argparse.Namespace) -> int:
    drive_root = _require_drive_root(args.drive_dwh_root)
    manifest = compact_raw_lake(args.output_root, promotion_name=args.promotion_name, start_date=args.start_date, end_date=args.end_date)
    package_root = Path(manifest["package_root"])
    collisions = write_collision_plan(package_root, drive_root)
    ready = _ready_payload(manifest, collisions)
    _write_json(package_root / "READY_FOR_PROMOTION.json", ready)
    print(json.dumps({"package_root": str(package_root), "ready_for_promotion": ready["ready_for_promotion"], "blocked_collisions": len(ready["blocked_collisions"])}, ensure_ascii=False, indent=2))
    return 0


def _load_ready(package_root: Path) -> dict[str, Any]:
    path = package_root / "READY_FOR_PROMOTION.json"
    if not path.exists():
        raise FileNotFoundError(f"READY_FOR_PROMOTION.json is required: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_reviewed(value: str | None) -> set[str]:
    return {part.strip() for part in (value or "").split(",") if part.strip()}


def _copy_audit_artifacts(package_root: Path, drive_root: Path, promotion_name: str) -> None:
    target = drive_root / "catalog" / "promotions" / promotion_name
    target.mkdir(parents=True, exist_ok=True)
    for filename in [
        "compact_manifest.json",
        "compact_qa_report.csv",
        "raw_asset_inventory.csv",
        "compact_source_lineage.csv",
        "known_gap_manifest.json",
        "raw_compact_classification.csv",
        "drive_collision_plan.csv",
        "READY_FOR_PROMOTION.json",
        "_LOCAL_COMPACT_READY.txt",
        "promotion_report.json",
    ]:
        src = package_root / filename
        if src.exists():
            shutil.copy2(src, target / filename)


def promote(args: argparse.Namespace) -> int:
    package_root = Path(args.package_root)
    drive_root = _require_drive_root(args.drive_dwh_root)
    ready = _load_ready(package_root)
    manifest = load_manifest(package_root)
    promotion_name = str(manifest["promotion_name"])
    if not args.confirm_promotion:
        raise ValueError("--confirm-promotion is required")
    if args.confirm_promotion != promotion_name:
        raise ValueError("--confirm-promotion must exactly match promotion_name")
    if not ready.get("ready_for_promotion"):
        raise ValueError("READY_FOR_PROMOTION.json does not mark this package ready_for_promotion=true")
    required_review = set(ready.get("review_required_bucket_kinds", []))
    allowed_review = _parse_reviewed(args.allow_reviewed_bucket_kinds)
    missing_review = sorted(required_review - allowed_review)
    if missing_review:
        raise ValueError(f"bucket kinds require explicit review opt-in: {missing_review}")

    collisions = write_collision_plan(package_root, drive_root)
    blocked = [r for r in collisions if r["action"] == "block_non_identical"]
    if blocked:
        raise FileExistsError("non-identical Drive overwrite is forbidden")

    copied: list[str] = []
    skipped: list[str] = []
    for row in collisions:
        src = Path(row["source_path"])
        dst = Path(row["drive_path"])
        if row["action"] == "copy_new":
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(row["relative_path"])
        elif row["action"] == "skip_identical":
            skipped.append(row["relative_path"])
        else:
            raise FileExistsError(f"unexpected blocked collision: {row['relative_path']}")

    for asset in manifest.get("compact_assets", []):
        verify_parquet_asset(_drive_raw_path(drive_root, asset["relative_path"]), expected_rows=int(asset["rows"]), expected_columns=list(asset["columns"]), expected_sha256=str(asset["sha256"]))

    report = {"promotion_name": promotion_name, "promoted_at": datetime.now(UTC).isoformat(), "copied": copied, "skipped_identical": skipped, "verified_assets": len(manifest.get("compact_assets", []))}
    _write_json(package_root / "promotion_report.json", report)
    _copy_audit_artifacts(package_root, drive_root, promotion_name)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def audit(args: argparse.Namespace) -> int:
    drive_root = _require_drive_root(args.drive_dwh_root)
    promo_dir = drive_root / "catalog" / "promotions" / args.promotion_name
    manifest = json.loads((promo_dir / "compact_manifest.json").read_text(encoding="utf-8"))
    summary: dict[str, int] = {}
    for asset in manifest.get("compact_assets", []):
        verify_parquet_asset(_drive_raw_path(drive_root, asset["relative_path"]), expected_rows=int(asset["rows"]), expected_columns=list(asset["columns"]), expected_sha256=str(asset["sha256"]))
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
