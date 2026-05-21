from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_rescue_manifest(
    *,
    output_path: Path,
    source_name: str,
    source_ref: str,
    df: pd.DataFrame,
    stock_col: str,
    date_col: str,
    duplicate_subset: list[str],
    critical_fields: list[str],
    pit_risk: str,
    manual_review_required: bool,
    limitations: list[str],
) -> None:
    payload = {
        "source_name": source_name,
        "source_ref": source_ref,
        "row_count": int(len(df)),
        "unique_stock_count": int(df[stock_col].nunique(dropna=True)),
        "date_range": {
            "min": None if df.empty else str(pd.to_datetime(df[date_col], errors="coerce").min().date()),
            "max": None if df.empty else str(pd.to_datetime(df[date_col], errors="coerce").max().date()),
        },
        "duplicate_key_count": int(df.duplicated(subset=duplicate_subset).sum()),
        "null_counts": {c: int(df[c].isna().sum()) for c in critical_fields},
        "pit_risk": pit_risk,
        "manual_review_required": manual_review_required,
        "limitations": limitations,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
