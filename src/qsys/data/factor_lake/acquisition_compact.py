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


def _safe_int(value: object) -> int:
    val = pd.to_numeric(value, errors="coerce")
    return int(val) if pd.notna(val) else 0


def _rel_from_run(run_dir: Path, src: Path, source_family: str, api_name: str, fallback_name: str) -> Path:
    try:
        return src.relative_to(run_dir)
    except ValueError:
        return Path("data") / "raw" / "akshare" / source_family / api_name / fallback_name


def _effective_catalog(run_dir: Path) -> pd.DataFrame:
    main = pd.read_csv(run_dir / "p0_wave_catalog.csv")
    has_fail_timeout = main["status"].fillna("").astype(str).isin(["failed", "timeout"]).any()
    if not has_fail_timeout:
        return main

    acceptance_path = run_dir / "p0_final_acceptance_report.json"
    if not acceptance_path.exists():
        raise ValueError("final acceptance report is required when main catalog has failed/timeout rows")
    acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
    if acceptance.get("final_status") != "accepted":
        raise ValueError("final acceptance status must be accepted when main catalog has failed/timeout rows")
    if int(acceptance.get("unresolved_failed_count", 1)) != 0:
        raise ValueError("unresolved_failed_count must be zero when main catalog has failed/timeout rows")

    recovery_path = run_dir / "p0_recovery_catalog.csv"
    if not recovery_path.exists():
        raise ValueError("recovery catalog is required when main catalog has failed/timeout rows")
    recovery = pd.read_csv(recovery_path)
    if recovery.empty:
        raise ValueError("recovery catalog is empty")

    main_ok = main[~main["status"].fillna("").astype(str).isin(["failed", "timeout"])].copy()
    recovery_status = recovery["status"].fillna("").astype(str)
    recovery_rows_num = pd.to_numeric(recovery.get("rows", 0), errors="coerce").fillna(0)
    recovery_ok = recovery[(recovery_status == "success") | ((recovery_status == "already_exists") & (recovery_rows_num > 0))].copy()

    failed_sources = main[main["status"].fillna("").astype(str).isin(["failed", "timeout"])][["source_family", "api_name"]].fillna("").astype(str)
    failed_pairs = set(tuple(x) for x in failed_sources.to_numpy())
    recovered_pairs = set(tuple(x) for x in recovery_ok[["source_family", "api_name"]].fillna("").astype(str).to_numpy())
    missing = sorted(failed_pairs - recovered_pairs)
    if missing:
        raise ValueError(f"some failed/timeout sources are not recovered: {missing}")

    for col in main_ok.columns:
        if col not in recovery_ok.columns:
            recovery_ok[col] = ""
    recovery_ok = recovery_ok[main_ok.columns]
    return pd.concat([main_ok, recovery_ok], ignore_index=True)


def compact_run(run_dir: Path, compact_root: Path) -> dict[str, object]:
    compact_root.mkdir(parents=True, exist_ok=True)
    catalog = _effective_catalog(run_dir)
    rows: list[dict[str, object]] = []
    expected_non_empty = 0
    copied_non_empty = 0
    parquet_actual_rows = 0
    parquet_counted = 0

    for _, rec in catalog.iterrows():
        row_count = _safe_int(rec.get("rows", 0))
        source_family = _clean_label(rec.get("source_family"), "unknown_family")
        api_name = _clean_label(rec.get("api_name"), "unknown_api")
        output_text = _safe_text(rec.get("output_path"))
        metadata_text = _safe_text(rec.get("metadata_path"))

        if "/nan/" in output_text.replace("\\", "/").lower() or "/nan/" in metadata_text.replace("\\", "/").lower():
            raise ValueError("/nan/ path is not allowed")

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
            if not metadata_text:
                raise ValueError("rows > 0 requires metadata_path")
            output_path = Path(output_text)
            metadata_path = Path(metadata_text)
            if not output_path.exists():
                raise FileNotFoundError(f"rows > 0 source data file missing: {output_path}")
            if not metadata_path.exists():
                raise FileNotFoundError(f"metadata file missing for non-empty row: {metadata_path}")

            expected_non_empty += 1
            assert dst_data is not None and dst_meta is not None
            dst_data.parent.mkdir(parents=True, exist_ok=True)
            dst_meta.parent.mkdir(parents=True, exist_ok=True)
            if dst_data.exists() or dst_meta.exists():
                raise FileExistsError("compact destination already exists")
            shutil.copy2(output_path, dst_data)
            shutil.copy2(metadata_path, dst_meta)
            copied_non_empty += 1

            if dst_data.suffix.lower() == ".parquet":
                try:
                    parquet_actual_rows += len(pd.read_parquet(dst_data))
                    parquet_counted += 1
                except Exception:
                    pass

        new = rec.to_dict()
        new["source_family"] = source_family
        new["api_name"] = api_name
        new["relative_output_path"] = rel_out
        new["relative_metadata_path"] = rel_meta
        rows.append(new)

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
