from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.factors.source_inventory import REQUIRED_COLUMNS, load_source_inventory, validate_source_inventory


def test_default_inventory_file_exists() -> None:
    fp = Path("config/factor_sources/akshare_free_factor_source_inventory_v0.csv")
    assert fp.exists()


def test_load_source_inventory_non_empty() -> None:
    df = load_source_inventory()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_required_columns_present() -> None:
    df = load_source_inventory()
    assert set(REQUIRED_COLUMNS).issubset(df.columns)


def test_validate_source_inventory_has_no_schema_errors_for_committed_inventory() -> None:
    df = load_source_inventory()
    msgs = validate_source_inventory(df)
    assert msgs == []


def test_validate_source_inventory_malformed_rows_produce_messages() -> None:
    df = pd.DataFrame(
        [
            {
                "api_name": "",
                "source_family": "",
                "data_type": "x",
                "status": "bad_status",
                "key_fields": "k",
                "date_field": "d",
                "symbol_field": "s",
                "pit_quality": "",
                "lookahead_risk_fields": "",
                "research_value": "",
                "recommended_role": "r",
                "recommended_phase": "p",
                "notes": "n",
            }
        ]
    )
    msgs = validate_source_inventory(df)
    text = "\n".join(msgs)
    assert "api_name is empty" in text
    assert "source_family is empty" in text
    assert "invalid status" in text
    assert "pit_quality is empty" in text
    assert "research_value is empty" in text
