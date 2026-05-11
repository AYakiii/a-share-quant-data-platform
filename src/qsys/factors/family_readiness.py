"""Loader and validator for factor family builder readiness matrix."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "factor_family",
    "current_status",
    "recommended_next_step",
    "builder_readiness",
    "required_preconditions",
    "pit_requirement",
    "event_window_requirement",
    "forbidden_field_risks",
    "first_safe_builder_scope",
    "priority",
    "notes",
]

ALLOWED_BUILDER_READINESS = {
    "ready_for_builder",
    "needs_pit_alignment",
    "needs_event_window_design",
    "needs_membership_asof",
    "raw_source_only",
    "deferred",
}


def load_factor_family_readiness(path: str | Path | None = None) -> pd.DataFrame:
    fp = Path(path) if path is not None else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "factor_family_builder_readiness_v0.csv"
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def validate_factor_family_readiness(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["factor_family", "current_status", "recommended_next_step", "priority"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")

    if "factor_family" in df.columns:
        vals = df["factor_family"].astype(str)
        for v, n in vals.value_counts().items():
            if n > 1:
                msgs.append(f"Duplicate factor_family: {v}")

    if "builder_readiness" in df.columns:
        bad = ~df["builder_readiness"].astype(str).isin(ALLOWED_BUILDER_READINESS)
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: invalid builder_readiness '{df.loc[i, 'builder_readiness']}'")

    if "factor_family" in df.columns and "builder_readiness" in df.columns:
        tl = df[df["factor_family"] == "technical_liquidity"]
        if tl.empty:
            msgs.append("technical_liquidity must exist")
        elif str(tl.iloc[0]["builder_readiness"]) != "ready_for_builder":
            msgs.append("technical_liquidity must be ready_for_builder")

        fq = df[df["factor_family"] == "fundamental_quality"]
        if fq.empty:
            msgs.append("fundamental_quality must exist")
        elif str(fq.iloc[0]["builder_readiness"]) == "ready_for_builder":
            msgs.append("fundamental_quality must not be ready_for_builder")

    needed = {"forbidden_field_risks", "notes", "builder_readiness"}
    if needed.issubset(df.columns):
        for i, row in df.iterrows():
            risks = str(row["forbidden_field_risks"]).strip()
            readiness = str(row["builder_readiness"]).strip()
            notes = str(row["notes"]).lower()
            if risks and readiness == "ready_for_builder" and ("safety" not in notes and "blacklist" not in notes):
                msgs.append(f"Row {int(i)}: forbidden_field_risks present but readiness is ready_for_builder without safety justification")

    return msgs
