from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.factors.family_readiness import (
    REQUIRED_COLUMNS,
    load_factor_family_readiness,
    validate_factor_family_readiness,
)


def test_readiness_file_exists_and_loads_non_empty() -> None:
    fp = Path("config/factor_sources/factor_family_builder_readiness_v0.csv")
    assert fp.exists()
    df = load_factor_family_readiness()
    assert len(df) > 0


def test_required_columns_present() -> None:
    df = load_factor_family_readiness()
    assert set(REQUIRED_COLUMNS).issubset(df.columns)


def test_validator_passes_for_committed_csv() -> None:
    df = load_factor_family_readiness()
    assert validate_factor_family_readiness(df) == []


def test_duplicate_factor_family_produces_message() -> None:
    df = pd.DataFrame([{c: "x" for c in REQUIRED_COLUMNS} for _ in range(2)])
    df.loc[:, "factor_family"] = "dup"
    df.loc[:, "builder_readiness"] = "raw_source_only"
    df.loc[:, "current_status"] = "raw_adapter_exists"
    df.loc[:, "recommended_next_step"] = "step"
    df.loc[:, "priority"] = "P1"
    msgs = validate_factor_family_readiness(df)
    assert any("Duplicate factor_family" in m for m in msgs)


def test_invalid_builder_readiness_produces_message() -> None:
    df = pd.DataFrame([{c: "x" for c in REQUIRED_COLUMNS}])
    df.loc[0, "factor_family"] = "x"
    df.loc[0, "current_status"] = "raw_adapter_exists"
    df.loc[0, "recommended_next_step"] = "step"
    df.loc[0, "priority"] = "P1"
    df.loc[0, "builder_readiness"] = "bad_value"
    msgs = validate_factor_family_readiness(df)
    assert any("invalid builder_readiness" in m for m in msgs)


def test_missing_recommended_next_step_produces_message() -> None:
    df = pd.DataFrame([{c: "x" for c in REQUIRED_COLUMNS}])
    df.loc[0, "factor_family"] = "x"
    df.loc[0, "current_status"] = "raw_adapter_exists"
    df.loc[0, "recommended_next_step"] = ""
    df.loc[0, "priority"] = "P1"
    df.loc[0, "builder_readiness"] = "raw_source_only"
    msgs = validate_factor_family_readiness(df)
    assert any("recommended_next_step is empty" in m for m in msgs)


def test_technical_liquidity_ready_for_builder() -> None:
    df = load_factor_family_readiness()
    row = df[df["factor_family"] == "technical_liquidity"].iloc[0]
    assert row["builder_readiness"] == "ready_for_builder"


def test_fundamental_quality_not_ready_for_builder() -> None:
    df = load_factor_family_readiness()
    row = df[df["factor_family"] == "fundamental_quality"].iloc[0]
    assert row["builder_readiness"] != "ready_for_builder"


def test_trading_attention_records_forbidden_field_risks() -> None:
    df = load_factor_family_readiness()
    row = df[df["factor_family"] == "trading_attention"].iloc[0]
    text = str(row["forbidden_field_risks"])
    assert "上榜后1日" in text and "上榜后10日" in text
