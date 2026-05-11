"""Static factor source inventory loader and validator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "api_name",
    "source_family",
    "data_type",
    "status",
    "key_fields",
    "date_field",
    "symbol_field",
    "pit_quality",
    "lookahead_risk_fields",
    "research_value",
    "recommended_role",
    "recommended_phase",
    "notes",
]

ALLOWED_STATUS = {"success", "unstable", "failed", "candidate", "prototype"}


def _default_inventory_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "akshare_free_factor_source_inventory_v0.csv"


def load_source_inventory(path: str | Path | None = None) -> pd.DataFrame:
    """Load AkShare free factor source inventory CSV."""

    fp = Path(path) if path is not None else _default_inventory_path()
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def validate_source_inventory(df: pd.DataFrame) -> list[str]:
    """Validate inventory schema and return human-readable messages."""

    msgs: list[str] = []

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    if "lookahead_risk_fields" not in df.columns:
        msgs.append("Column 'lookahead_risk_fields' must exist (may be empty).")

    if "api_name" in df.columns:
        bad = df["api_name"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: api_name is empty")

    if "source_family" in df.columns:
        bad = df["source_family"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: source_family is empty")

    if "status" in df.columns:
        bad_mask = ~df["status"].astype(str).str.strip().isin(ALLOWED_STATUS)
        for i in df.index[bad_mask].tolist():
            v = str(df.loc[i, "status"])
            msgs.append(f"Row {int(i)}: invalid status '{v}'")

    if "pit_quality" in df.columns:
        bad = df["pit_quality"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: pit_quality is empty")

    if "research_value" in df.columns:
        bad = df["research_value"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: research_value is empty")

    return msgs
