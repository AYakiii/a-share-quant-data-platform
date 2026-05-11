from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.factors.schema_contract import (
    ALLOWED_FIELD_ROLES,
    REQUIRED_COLUMNS,
    load_schema_contract,
    validate_schema_contract,
)


def test_default_schema_contract_file_exists() -> None:
    fp = Path("config/factor_sources/akshare_raw_schema_contract_v0.csv")
    assert fp.exists()


def test_load_schema_contract_non_empty() -> None:
    df = load_schema_contract()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_required_columns_present() -> None:
    df = load_schema_contract()
    assert set(REQUIRED_COLUMNS).issubset(df.columns)


def test_validate_schema_contract_for_committed_contract() -> None:
    df = load_schema_contract()
    msgs = validate_schema_contract(df)
    assert msgs == []


def test_malformed_field_role_produces_validation_message() -> None:
    df = pd.DataFrame(
        [{c: "x" for c in REQUIRED_COLUMNS}]
    )
    df.loc[0, "api_name"] = "stock_zh_a_hist"
    df.loc[0, "field_name"] = "收盘"
    df.loc[0, "field_role"] = "bad_role"
    df.loc[0, "tradable_feature_allowed"] = "true"
    df.loc[0, "lookahead_risk"] = "false"
    df.loc[0, "required_for_panel_alignment"] = "false"
    msgs = validate_schema_contract(df)
    assert any("invalid field_role" in m for m in msgs)


def test_invalid_boolean_value_produces_validation_message() -> None:
    df = pd.DataFrame(
        [{c: "x" for c in REQUIRED_COLUMNS}]
    )
    df.loc[0, "api_name"] = "stock_zh_a_hist"
    df.loc[0, "field_name"] = "收盘"
    df.loc[0, "field_role"] = "raw_value"
    df.loc[0, "tradable_feature_allowed"] = "yes"
    df.loc[0, "lookahead_risk"] = "false"
    df.loc[0, "required_for_panel_alignment"] = "false"
    msgs = validate_schema_contract(df)
    assert any("must be true/false" in m for m in msgs)


def test_post_event_outcome_tradable_true_produces_message() -> None:
    df = pd.DataFrame(
        [{c: "" for c in REQUIRED_COLUMNS}]
    )
    df.loc[0, "api_name"] = "GLOBAL_BLACKLIST"
    df.loc[0, "source_family"] = "safety_contract"
    df.loc[0, "data_type"] = "field_blacklist"
    df.loc[0, "field_name"] = "fwd_ret_5d"
    df.loc[0, "field_role"] = "post_event_outcome"
    df.loc[0, "tradable_feature_allowed"] = "true"
    df.loc[0, "lookahead_risk"] = "true"
    df.loc[0, "required_for_panel_alignment"] = "false"
    msgs = validate_schema_contract(df)
    assert any("requires tradable_feature_allowed=false" in m for m in msgs)


def test_lookahead_risk_true_tradable_true_produces_message() -> None:
    df = pd.DataFrame(
        [{c: "" for c in REQUIRED_COLUMNS}]
    )
    df.loc[0, "api_name"] = "GLOBAL_BLACKLIST"
    df.loc[0, "source_family"] = "safety_contract"
    df.loc[0, "data_type"] = "field_blacklist"
    df.loc[0, "field_name"] = "上榜后1日"
    df.loc[0, "field_role"] = "forbidden_feature"
    df.loc[0, "tradable_feature_allowed"] = "true"
    df.loc[0, "lookahead_risk"] = "true"
    df.loc[0, "required_for_panel_alignment"] = "false"
    msgs = validate_schema_contract(df)
    assert any("lookahead_risk=true requires tradable_feature_allowed=false" in m for m in msgs)


def test_global_blacklist_rows_include_required_fields() -> None:
    df = load_schema_contract()
    gb = df[df["api_name"] == "GLOBAL_BLACKLIST"]
    fields = set(gb["field_name"].tolist())
    required = {"fwd_ret_5d", "fwd_ret_20d", "解禁后20日涨跌幅", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"}
    assert required.issubset(fields)
    assert set(gb["field_role"]).issubset(ALLOWED_FIELD_ROLES)
