"""Loader and validator for factor builder coverage matrix (Phase 17Q)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "factor_family",
    "builder_function",
    "module",
    "input_contract",
    "required_columns",
    "optional_columns",
    "output_type",
    "current_status",
    "first_phase",
    "allowed_for_real_data_runner",
    "requires_pit_alignment",
    "requires_event_window_design",
    "computes_signals",
    "runs_backtest",
    "notes",
]


_BOOL_COLUMNS = ["allowed_for_real_data_runner", "computes_signals", "runs_backtest"]
_REQUIRED_FAMILIES = {"technical_liquidity", "margin_leverage", "market_regime"}


def load_factor_builder_coverage(path: str | Path | None = None) -> pd.DataFrame:
    fp = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "factor_builder_coverage_v0.csv"
    )
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def _is_bool_str(v: str) -> bool:
    return str(v).strip().lower() in {"true", "false"}


def validate_factor_builder_coverage(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["factor_family", "builder_function", "module", "current_status"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")

    if "factor_family" in df.columns:
        vals = df["factor_family"].astype(str)
        for v, n in vals.value_counts().items():
            if n > 1:
                msgs.append(f"Duplicate factor_family: {v}")
        missing_families = _REQUIRED_FAMILIES - set(vals.tolist())
        for fam in sorted(missing_families):
            msgs.append(f"{fam} must exist")

    for c in _BOOL_COLUMNS:
        if c in df.columns:
            bad = ~df[c].astype(str).map(_is_bool_str)
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} must be true/false")

    if "computes_signals" in df.columns:
        bad = df["computes_signals"].astype(str).str.lower() == "true"
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: computes_signals must remain false")

    if "runs_backtest" in df.columns:
        bad = df["runs_backtest"].astype(str).str.lower() == "true"
        for i in df.index[bad].tolist():
            msgs.append(f"Row {int(i)}: runs_backtest must remain false")

    return msgs
