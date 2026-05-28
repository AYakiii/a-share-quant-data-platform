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


def _safe_text(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _rel_from_run(run_dir: Path, src: Path, source_family: str, api_name: str, fallback_name: str) -> Path:
    try:
        return src.relative_to(run_dir)
    except ValueError:
        return Path("data") / "raw" / "akshare" / source_family / api_name / fallback_name


def compact_run(run_dir: Path, compact_root: Path) -> dict[str, object]:
    compact_root.mkdir(parents=True, exist_ok=True)
    catalog = pd.read_csv(run_dir / "p0_wave_catalog.csv")
    rows: list[dict[str, object]] = []
    expected_non_empty = 0
    copied_non_empty = 0
    parquet_actual_rows = 0
    parquet_counted = 0
    has_failed_or_timeout = False

    for _, rec in catalog.iterrows():
        status = _safe_text(rec.get("status"))
        row_count = int(pd.to_numeric(rec.get("rows", 0), errors="coerce") if pd.notna(pd.to_numeric(rec.get("rows", 0), errors="coerce")) else 0)
        source_family = _clean_label(rec.get("source_family"), "unknown_family")
        api_name = _clean_label(rec.get("api_name"), "unknown_api")
        output_text = _safe_text(rec.get("output_path"))
        metadata_text = _safe_text(rec.get("metadata_path"))

        if "/nan/" in output_text.replace("\\", "/").lower() or "/nan/" in metadata_text.replace("\\", "/").lower():
            raise ValueError("/nan/ path is not allowed")

        if status in {"failed", "timeout"}:
            has_failed_or_timeout = True

        rel_out = ""
        rel_meta = ""
        dst_data = None
        dst_meta = None

        if output_text:
            output_path = Path(output_text)
            rel_out = str(_rel_from_run(run_dir, output_path, source_family, api_name, output_path.name))
            dst_data = compact_root / rel_out
        if metadata_text:
            metadata_path = Path(metadata_text)
            rel_meta = str(_rel_from_run(run_dir, metadata_path, source_family, api_name, metadata_path.name))
            dst_meta = compact_root / rel_meta

        if row_count > 0:
            if not output_text:
                raise ValueError("rows > 0 requires output_path")
            output_path = Path(output_text)
            if not output_path.exists():
                raise FileNotFoundError(f"rows > 0 source data file missing: {output_path}")
            expected_non_empty += 1
            assert dst_data is not None
            dst_data.parent.mkdir(parents=True, exist_ok=True)
            if dst_data.exists():
                raise FileExistsError(f"compact destination already exists: {dst_data}")
            shutil.copy2(output_path, dst_data)
            copied_non_empty += 1
            if dst_data.suffix.lower() == ".parquet":
                try:
                    parquet_actual_rows += len(pd.read_parquet(dst_data))
                    parquet_counted += 1
                except Exception:
                    pass

            if metadata_text:
                metadata_path = Path(metadata_text)
                if not metadata_path.exists():
                    raise FileNotFoundError(f"metadata file missing for non-empty row: {metadata_path}")
                assert dst_meta is not None
                dst_meta.parent.mkdir(parents=True, exist_ok=True)
                if dst_meta.exists():
                    raise FileExistsError(f"compact metadata destination already exists: {dst_meta}")
                shutil.copy2(metadata_path, dst_meta)
        else:
            if output_text and Path(output_text).exists() and dst_data is not None:
                dst_data.parent.mkdir(parents=True, exist_ok=True)
                if not dst_data.exists():
                    shutil.copy2(Path(output_text), dst_data)
            if metadata_text and Path(metadata_text).exists() and dst_meta is not None:
                dst_meta.parent.mkdir(parents=True, exist_ok=True)
                if not dst_meta.exists():
                    shutil.copy2(Path(metadata_text), dst_meta)

        new = rec.to_dict()
        new["source_family"] = source_family
        new["api_name"] = api_name
        new["relative_output_path"] = rel_out
        new["relative_metadata_path"] = rel_meta
        rows.append(new)

    if has_failed_or_timeout:
        raise ValueError("compact cannot proceed with failed/timeout rows in catalog")
    if copied_non_empty != expected_non_empty:
        raise ValueError(f"copied non-empty data file count mismatch: {copied_non_empty} != {expected_non_empty}")

    out_df = pd.DataFrame(rows)
    if out_df["relative_output_path"].astype(str).str.contains("/nan/", case=False).any() or out_df["relative_metadata_path"].astype(str).str.contains("/nan/", case=False).any():
        raise ValueError("compact catalog contains /nan/ in relative paths")

    in_rows = int(pd.to_numeric(catalog.get("rows", 0), errors="coerce").fillna(0).sum())
    out_rows = int(pd.to_numeric(out_df.get("rows", 0), errors="coerce").fillna(0).sum())
    if in_rows != out_rows:
        raise ValueError(f"row conservation failed: {in_rows} != {out_rows}")

    out_df.to_csv(compact_root / "compact_catalog.csv", index=False)
    manifest = {
        "input_total_rows": in_rows,
        "output_total_rows": out_rows,
        "row_conservation_ok": True,
        "expected_non_empty_source_files": expected_non_empty,
        "copied_non_empty_data_files": copied_non_empty,
        "parquet_row_sum": parquet_actual_rows,
        "parquet_row_sum_counted_files": parquet_counted,
    }
    (compact_root / "compact_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
