"""Raw schema contract loader and validator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "api_name",
    "source_family",
    "data_type",
    "field_name",
    "field_role",
    "tradable_feature_allowed",
    "lookahead_risk",
    "required_for_panel_alignment",
    "notes",
]

ALLOWED_FIELD_ROLES = {
    "identifier",
    "date",
    "raw_value",
    "raw_text",
    "metadata",
    "label",
    "post_event_outcome",
    "forbidden_feature",
    "unknown",
}

BOOL_COLUMNS = ["tradable_feature_allowed", "lookahead_risk", "required_for_panel_alignment"]


def _default_schema_contract_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "akshare_raw_schema_contract_v0.csv"


def load_schema_contract(path: str | Path | None = None) -> pd.DataFrame:
    """Load raw schema contract CSV."""

    fp = Path(path) if path is not None else _default_schema_contract_path()
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def _to_bool(v: str) -> bool | None:
    s = str(v).strip().lower()
    if s == "true":
        return True
    if s == "false":
        return False
    return None


def validate_schema_contract(df: pd.DataFrame) -> list[str]:
    """Validate schema contract and return human-readable messages."""

    msgs: list[str] = []

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    if "api_name" in df.columns:
        bad = df["api_name"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: api_name is empty")

    if "field_name" in df.columns:
        bad = df["field_name"].astype(str).str.strip() == ""
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: field_name is empty")

    if "field_role" in df.columns:
        bad = ~df["field_role"].astype(str).str.strip().isin(ALLOWED_FIELD_ROLES)
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: invalid field_role '{df.loc[i, 'field_role']}'")

    for col in BOOL_COLUMNS:
        if col not in df.columns:
            continue
        parsed = df[col].astype(str).map(_to_bool)
        for i in df.index[parsed.isna()].tolist():
            msgs.append(f"Row {int(i)}: column '{col}' must be true/false")

    if all(c in df.columns for c in ["field_role", "tradable_feature_allowed", "lookahead_risk"]):
        for i, row in df.iterrows():
            role = str(row["field_role"]).strip()
            tradable = _to_bool(str(row["tradable_feature_allowed"]))
            risk = _to_bool(str(row["lookahead_risk"]))

            if role in {"post_event_outcome", "forbidden_feature"} and tradable is True:
                msgs.append(
                    f"Row {int(i)}: field_role '{role}' requires tradable_feature_allowed=false"
                )
            if risk is True and tradable is True:
                msgs.append(
                    f"Row {int(i)}: lookahead_risk=true requires tradable_feature_allowed=false"
                )

    return msgs
