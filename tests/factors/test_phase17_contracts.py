from __future__ import annotations

from qsys.factors.phase17_contracts import (
    EVENT_COLUMNS,
    INDUSTRY_COLUMNS,
    load_event_window_contract,
    load_industry_asof_contract,
    validate_event_window_contract,
    validate_industry_asof_contract,
)


def test_industry_contract_loads_non_empty() -> None:
    df = load_industry_asof_contract()
    assert not df.empty


def test_event_contract_loads_non_empty() -> None:
    df = load_event_window_contract()
    assert not df.empty


def test_required_columns_present() -> None:
    i = load_industry_asof_contract()
    e = load_event_window_contract()
    assert set(INDUSTRY_COLUMNS).issubset(i.columns)
    assert set(EVENT_COLUMNS).issubset(e.columns)


def test_validators_pass_committed_csv() -> None:
    assert validate_industry_asof_contract(load_industry_asof_contract()) == []
    assert validate_event_window_contract(load_event_window_contract()) == []


def test_missing_asof_rule_validation() -> None:
    df = load_industry_asof_contract().copy()
    df.loc[0, "asof_rule"] = ""
    msgs = validate_industry_asof_contract(df)
    assert any("asof_rule is empty" in m for m in msgs)


def test_missing_primary_event_date_validation() -> None:
    df = load_event_window_contract().copy()
    df.loc[0, "primary_event_date"] = ""
    msgs = validate_event_window_contract(df)
    assert any("primary_event_date is empty" in m for m in msgs)


def test_lhb_post_event_fields_present() -> None:
    df = load_event_window_contract()
    row = df[df["source_api"] == "stock_lhb_detail_em"].iloc[0]
    got = str(row["post_event_outcome_fields"])
    for f in ["上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"]:
        assert f in got


def test_restricted_release_post_event_field_present() -> None:
    df = load_event_window_contract()
    row = df[df["source_api"] == "stock_restricted_release_detail_em"].iloc[0]
    assert "解禁后20日涨跌幅" in str(row["post_event_outcome_fields"])
