from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd


def promote_compact(compact_root: Path, drive_root: Path, asset_name: str, promote_to_drive: bool, allow_overwrite: bool) -> dict[str, object]:
    target = drive_root / asset_name
    plan = {"compact_root": str(compact_root), "target": str(target), "promote_to_drive": bool(promote_to_drive), "allow_overwrite": bool(allow_overwrite)}
    (compact_root / "promotion_plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    if not promote_to_drive:
        report = {"status": "dry_run", "copied": False, "target": str(target)}
        (compact_root / "promotion_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    if target.exists() and not allow_overwrite:
        raise FileExistsError(f"target already exists: {target}")
    if target.exists() and allow_overwrite:
        shutil.rmtree(target)
    shutil.copytree(compact_root, target)
    report = {"status": "promoted", "copied": True, "target": str(target)}
    (compact_root / "promotion_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def qa_promoted_asset(drive_root: Path, asset_name: str, compact_root: Path | None = None) -> dict[str, object]:
    asset = drive_root / asset_name
    catalog = pd.read_csv(asset / "compact_catalog.csv")
    errs: list[str] = []
    for c in ["output_path", "metadata_path", "api_name"]:
        if c not in catalog.columns:
            errs.append(f"missing column: {c}")
    if not catalog.empty:
        if catalog["output_path"].astype(str).str.contains("/nan/", case=False).any():
            errs.append("/nan/ found in output path")
        if catalog["metadata_path"].astype(str).str.contains("/nan/", case=False).any():
            errs.append("/nan/ found in metadata path")
        for p in catalog["output_path"].astype(str):
            if not Path(p).exists() or Path(p).suffix != ".parquet":
                errs.append(f"missing parquet: {p}")
                break

    drive_rows = int(pd.to_numeric(catalog.get("rows", 0), errors="coerce").fillna(0).sum())
    if drive_rows <= 0:
        errs.append("drive total rows must be > 0")
    if compact_root is not None:
        compact_catalog = pd.read_csv(compact_root / "compact_catalog.csv")
        compact_rows = int(pd.to_numeric(compact_catalog.get("rows", 0), errors="coerce").fillna(0).sum())
        if compact_rows != drive_rows:
            errs.append("drive and compact row count mismatch")

    report = {"asset": str(asset), "is_valid": len(errs) == 0, "errors": errs, "drive_rows": drive_rows}
    (asset / "drive_qa_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame([{"check": "is_valid", "value": report["is_valid"]}, {"check": "drive_rows", "value": drive_rows}]).to_csv(asset / "drive_qa_summary.csv", index=False)
    if errs:
        raise ValueError("; ".join(errs))
    return report
