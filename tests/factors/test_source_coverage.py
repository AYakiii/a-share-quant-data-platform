from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.factors.source_coverage import (
    RAW_ADAPTER_COLUMNS,
    TAXONOMY_COLUMNS,
    load_factor_family_taxonomy,
    load_raw_adapter_coverage,
    validate_factor_family_taxonomy,
    validate_raw_adapter_coverage,
)


def test_raw_adapter_coverage_file_exists_and_loads_non_empty() -> None:
    fp = Path("config/factor_sources/raw_adapter_coverage_v0.csv")
    assert fp.exists()
    df = load_raw_adapter_coverage()
    assert len(df) > 0


def test_factor_family_taxonomy_file_exists_and_loads_non_empty() -> None:
    fp = Path("config/factor_sources/factor_family_taxonomy_v0.csv")
    assert fp.exists()
    df = load_factor_family_taxonomy()
    assert len(df) > 0


def test_required_columns_present() -> None:
    a = load_raw_adapter_coverage()
    b = load_factor_family_taxonomy()
    assert set(RAW_ADAPTER_COLUMNS).issubset(a.columns)
    assert set(TAXONOMY_COLUMNS).issubset(b.columns)


def test_validators_pass_for_committed_csvs() -> None:
    a = load_raw_adapter_coverage()
    b = load_factor_family_taxonomy()
    assert validate_raw_adapter_coverage(a) == []
    assert validate_factor_family_taxonomy(b) == []


def test_malformed_coverage_computes_factors_true_returns_message() -> None:
    df = pd.DataFrame([{c: "x" for c in RAW_ADAPTER_COLUMNS}])
    df.loc[0, "adapter_function"] = "fetch_x"
    df.loc[0, "module"] = "m.py"
    df.loc[0, "api_name"] = "x"
    df.loc[0, "source_family"] = "s"
    df.loc[0, "status"] = "active"
    df.loc[0, "raw_only"] = "true"
    df.loc[0, "parameterized"] = "false"
    df.loc[0, "computes_factors"] = "true"
    msgs = validate_raw_adapter_coverage(df)
    assert any("computes_factors must remain false" in m for m in msgs)


def test_malformed_taxonomy_duplicate_factor_family_returns_message() -> None:
    df = pd.DataFrame([{c: "x" for c in TAXONOMY_COLUMNS} for _ in range(2)])
    df.loc[:, "factor_family"] = "dup"
    df.loc[:, "current_status"] = "raw_adapter_exists"
    msgs = validate_factor_family_taxonomy(df)
    assert any("Duplicate factor_family" in m for m in msgs)


def test_taxonomy_includes_technical_liquidity() -> None:
    df = load_factor_family_taxonomy()
    row = df[df["factor_family"] == "technical_liquidity"]
    assert not row.empty


def test_taxonomy_includes_fundamental_quality_with_pit_prototype_warning() -> None:
    df = load_factor_family_taxonomy()
    row = df[df["factor_family"] == "fundamental_quality"]
    assert not row.empty
    text = (str(row.iloc[0]["current_status"]) + " " + str(row.iloc[0]["notes"]))
    assert ("prototype_source_only" in text) or ("pit" in text.lower()) or ("disclosure" in text.lower())
