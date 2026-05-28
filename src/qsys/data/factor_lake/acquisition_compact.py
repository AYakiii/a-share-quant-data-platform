from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd


def _clean_label(value: object, fallback: str) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return fallback
    return text


def compact_run(run_dir: Path, compact_root: Path) -> dict[str, object]:
    compact_root.mkdir(parents=True, exist_ok=True)
    catalog = pd.read_csv(run_dir / "p0_wave_catalog.csv")
    rows: list[dict[str, object]] = []
    for _, rec in catalog.iterrows():
        output_path = Path(str(rec.get("output_path", "")))
        metadata_path = Path(str(rec.get("metadata_path", "")))
        source_family = _clean_label(rec.get("source_family"), "unknown_family")
        api_name = _clean_label(rec.get("api_name"), "unknown_api")
        if "/nan/" in str(output_path).replace("\\", "/").lower() or "/nan/" in str(metadata_path).replace("\\", "/").lower():
            raise ValueError("/nan/ path is not allowed")

        dst_data = compact_root / "data" / source_family / api_name / output_path.name
        dst_meta = compact_root / "metadata" / source_family / api_name / metadata_path.name
        dst_data.parent.mkdir(parents=True, exist_ok=True)
        dst_meta.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            shutil.copy2(output_path, dst_data)
        if metadata_path.exists():
            shutil.copy2(metadata_path, dst_meta)
        new = rec.to_dict()
        new["source_family"] = source_family
        new["api_name"] = api_name
        new["output_path"] = str(dst_data)
        new["metadata_path"] = str(dst_meta)
        rows.append(new)

    out_df = pd.DataFrame(rows)
    in_rows = int(pd.to_numeric(catalog.get("rows", 0), errors="coerce").fillna(0).sum())
    out_rows = int(pd.to_numeric(out_df.get("rows", 0), errors="coerce").fillna(0).sum())
    if in_rows != out_rows:
        raise ValueError(f"row conservation failed: {in_rows} != {out_rows}")
    out_df.to_csv(compact_root / "compact_catalog.csv", index=False)
    manifest = {"input_total_rows": in_rows, "output_total_rows": out_rows, "row_conservation_ok": True}
    (compact_root / "compact_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
