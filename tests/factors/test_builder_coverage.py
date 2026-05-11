from __future__ import annotations

import pandas as pd

from qsys.factors.builder_coverage import REQUIRED_COLUMNS, load_factor_builder_coverage, validate_factor_builder_coverage


def test_coverage_file_exists_and_non_empty() -> None:
    df = load_factor_builder_coverage()
    assert not df.empty


def test_required_columns_present() -> None:
    df = load_factor_builder_coverage()
    assert set(REQUIRED_COLUMNS).issubset(df.columns)


def test_committed_csv_validator_passes() -> None:
    df = load_factor_builder_coverage()
    assert validate_factor_builder_coverage(df) == []


def test_duplicate_factor_family_validation() -> None:
    df = load_factor_builder_coverage()
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    msgs = validate_factor_builder_coverage(dup)
    assert any("Duplicate factor_family" in m for m in msgs)


def test_computes_signals_true_validation() -> None:
    df = load_factor_builder_coverage().copy()
    df.loc[0, "computes_signals"] = "true"
    msgs = validate_factor_builder_coverage(df)
    assert any("computes_signals must remain false" in m for m in msgs)


def test_runs_backtest_true_validation() -> None:
    df = load_factor_builder_coverage().copy()
    df.loc[0, "runs_backtest"] = "true"
    msgs = validate_factor_builder_coverage(df)
    assert any("runs_backtest must remain false" in m for m in msgs)


def test_required_family_rows_exist() -> None:
    df = load_factor_builder_coverage()
    families = set(df["factor_family"].tolist())
    assert {"technical_liquidity", "margin_leverage", "market_regime"}.issubset(families)
