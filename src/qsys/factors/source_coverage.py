"""Loaders and validators for raw adapter coverage and factor family taxonomy."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

RAW_ADAPTER_COLUMNS = [
    "adapter_function",
    "module",
    "api_name",
    "source_family",
    "data_type",
    "phase_added",
    "parameterized",
    "key_request_params",
    "raw_only",
    "computes_factors",
    "test_module",
    "status",
    "notes",
]

TAXONOMY_COLUMNS = [
    "factor_family",
    "source_families",
    "source_apis",
    "intended_role",
    "current_status",
    "pit_difficulty",
    "first_builder_phase",
    "notes",
]


def load_raw_adapter_coverage(path: str | Path | None = None) -> pd.DataFrame:
    fp = Path(path) if path is not None else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "raw_adapter_coverage_v0.csv"
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def load_factor_family_taxonomy(path: str | Path | None = None) -> pd.DataFrame:
    fp = Path(path) if path is not None else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "factor_family_taxonomy_v0.csv"
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def _is_bool_str(v: str) -> bool:
    return str(v).strip().lower() in {"true", "false"}


def validate_raw_adapter_coverage(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []
    missing = [c for c in RAW_ADAPTER_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["adapter_function", "module", "api_name", "source_family", "status"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")

    for c in ["raw_only", "computes_factors", "parameterized"]:
        if c in df.columns:
            bad = ~df[c].astype(str).map(_is_bool_str)
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} must be true/false")

    if "computes_factors" in df.columns:
        bad = df["computes_factors"].astype(str).str.lower() == "true"
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: computes_factors must remain false for raw adapters")

    return msgs


def validate_factor_family_taxonomy(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []
    missing = [c for c in TAXONOMY_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["factor_family", "current_status", "source_families", "intended_role"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")

    if "factor_family" in df.columns:
        dups = df["factor_family"].astype(str)
        for v, k in dups.value_counts().items():
            if k > 1:
                msgs.append(f"Duplicate factor_family: {v}")

    if "factor_family" in df.columns and "current_status" in df.columns:
        row = df[df["factor_family"] == "technical_liquidity"]
        if row.empty:
            msgs.append("technical_liquidity must exist")
        elif str(row.iloc[0]["current_status"]).strip() != "builder_v0_exists":
            msgs.append("technical_liquidity current_status must be builder_v0_exists")

        fq = df[df["factor_family"] == "fundamental_quality"]
        if fq.empty:
            msgs.append("fundamental_quality must exist")
        else:
            status_ok = str(fq.iloc[0]["current_status"]).strip() == "prototype_source_only"
            note_ok = "pit" in str(fq.iloc[0].get("notes", "")).lower() or "disclosure" in str(fq.iloc[0].get("notes", "")).lower()
            if not (status_ok or note_ok):
                msgs.append("fundamental_quality must be prototype_source_only or explicitly require PIT/disclosure alignment")

    return msgs
