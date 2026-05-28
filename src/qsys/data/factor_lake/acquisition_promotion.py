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
    (target / "promotion_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def qa_promoted_asset(drive_root: Path, asset_name: str, compact_root: Path | None = None) -> dict[str, object]:
    asset = drive_root / asset_name
    catalog = pd.read_csv(asset / "compact_catalog.csv")
    errs: list[str] = []
    for c in ["relative_output_path", "relative_metadata_path", "api_name"]:
        if c not in catalog.columns:
            errs.append(f"missing column: {c}")

    drive_rows = int(pd.to_numeric(catalog.get("rows", 0), errors="coerce").fillna(0).sum())
    for _, rec in catalog.iterrows():
        rel_out = str(rec.get("relative_output_path") or "")
        rel_meta = str(rec.get("relative_metadata_path") or "")
        rows_num = pd.to_numeric(rec.get("rows", 0), errors="coerce")
        rows = int(rows_num) if pd.notna(rows_num) else 0
        if "/nan/" in rel_out.lower() or "/nan/" in rel_meta.lower():
            errs.append("/nan/ found in relative path")
            break
        if rows > 0:
            if not rel_out:
                errs.append("missing relative_output_path for non-empty row")
                break
            out_path = asset / rel_out
            if not out_path.exists() or out_path.suffix != ".parquet":
                errs.append(f"missing parquet: {out_path}")
                break
            if not rel_meta:
                errs.append("missing relative_metadata_path for non-empty row")
                break
            meta_path = asset / rel_meta
            if not meta_path.exists():
                errs.append(f"missing metadata: {meta_path}")
                break

    if drive_rows <= 0:
        errs.append("drive total rows must be > 0")
    if compact_root is not None:
        compact_catalog = pd.read_csv(compact_root / "compact_catalog.csv")
        compact_rows = int(pd.to_numeric(compact_catalog.get("rows", 0), errors="coerce").fillna(0).sum())
        if compact_rows != drive_rows:
            errs.append("drive and compact row count mismatch")

    for req in ["compact_manifest.json", "promotion_report.json"]:
        if not (asset / req).exists():
            errs.append(f"missing required artifact: {req}")

    report = {"asset": str(asset), "is_valid": len(errs) == 0, "errors": errs, "drive_rows": drive_rows}
    (asset / "drive_qa_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame([{"check": "is_valid", "value": report["is_valid"]}, {"check": "drive_rows", "value": drive_rows}]).to_csv(asset / "drive_qa_summary.csv", index=False)
    if errs:
        raise ValueError("; ".join(errs))
    return report
