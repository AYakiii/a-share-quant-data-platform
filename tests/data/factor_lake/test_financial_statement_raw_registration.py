from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake.raw_ingest import (
    API_POLICY_METADATA,
    COVERAGE_API_SPECS,
    FINANCIAL_STATEMENT_REPORT_DATE_APIS,
    FINANCIAL_STATEMENT_SYMBOL_APIS,
    _build_raw_partition,
    _financial_statement_symbol_pair,
    _params_for_mode,
    run_raw_coverage_ingest,
)
from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY, SOURCE_CAPABILITY_REGISTRY

SYMBOL_APIS = {
    "stock_balance_sheet_by_report_em",
    "stock_profit_sheet_by_report_em",
    "stock_cash_flow_sheet_by_report_em",
}
REPORT_DATE_APIS = {"stock_zcfz_em", "stock_lrb_em", "stock_xjll_em"}
STATEMENT_APIS = SYMBOL_APIS | REPORT_DATE_APIS
EXISTING_FINANCIAL_APIS = {
    "stock_financial_analysis_indicator",
    "stock_financial_analysis_indicator_em",
    "stock_yjyg_em",
    "stock_yysj_em",
}
FORBIDDEN_BEHAVIOR_TOKENS = {
    "drive",
    "normalized",
    "feature",
    "factor",
    "signal",
    "backtest",
    "model",
}


def test_all_financial_statement_apis_registered_under_financial_fundamental() -> None:
    specs = COVERAGE_API_SPECS["financial_fundamental"]
    by_api = {spec["api_name"]: spec for spec in specs}
    capability_apis = {spec.api_name for spec in SOURCE_CAPABILITY_REGISTRY}
    source_case_apis = {case.api_name for case in FACTOR_SOURCE_REGISTRY}

    assert STATEMENT_APIS <= set(by_api)
    assert STATEMENT_APIS <= capability_apis
    assert STATEMENT_APIS <= source_case_apis
    for api_name in SYMBOL_APIS:
        assert by_api[api_name] == {"api_name": api_name, "param_mode": "financial_statement_symbol"}
    for api_name in REPORT_DATE_APIS:
        assert by_api[api_name] == {"api_name": api_name, "param_mode": "financial_statement_report_date"}
        capability = next(spec for spec in SOURCE_CAPABILITY_REGISTRY if spec.api_name == api_name)
        assert capability.partition_keys == ("report_date",)
        assert capability.date_field == "report_date(input)"
        assert capability.symbol_field == "股票代码"
        assert capability.report_period_field == "report_date(input)"
        assert capability.announcement_date_field == "公告日期"
        assert "REPORT_DATE" not in {capability.date_field, capability.report_period_field}
        assert capability.announcement_date_field != "NOTICE_DATE"


def test_policy_metadata_marks_statement_apis_p1_raw_not_deferred() -> None:
    for family_api in FINANCIAL_STATEMENT_SYMBOL_APIS:
        policy = API_POLICY_METADATA[family_api]
        assert policy["enabled"] is True
        assert policy["default_enabled"] is True
        assert policy["manual_review_required"] is False
        assert policy["priority_tier"] == "P1"
        assert policy["data_theme"] == "financial_statement_raw"
        assert policy["acquisition_mode"] == "bulk_financial_wide"
        assert "disabled_category" not in policy

    for family_api in FINANCIAL_STATEMENT_REPORT_DATE_APIS:
        policy = API_POLICY_METADATA[family_api]
        assert policy["enabled"] is True
        assert policy["default_enabled"] is True
        assert policy["manual_review_required"] is False
        assert policy["priority_tier"] == "P1"
        assert policy["data_theme"] == "financial_statement_raw"
        assert policy["acquisition_mode"] == "bulk_financial_core"
        assert "disabled_category" not in policy


def test_symbol_mode_creates_one_task_per_selected_symbol_per_statement_api(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def _adapter(api_name: str):
        def fn(symbol: str) -> pd.DataFrame:
            calls.append((api_name, symbol))
            return pd.DataFrame({"REPORT_DATE": ["2024-03-31"], "NOTICE_DATE": ["2024-04-20"]})

        return fn

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["600519", "000001"],
        selected_api_names=sorted(SYMBOL_APIS),
        adapter_map={api_name: _adapter(api_name) for api_name in SYMBOL_APIS},
        max_workers=1,
    )

    assert len(out["rows"]) == 6
    assert {(api, symbol) for api, symbol in calls} == {
        (api_name, ak_symbol)
        for api_name in SYMBOL_APIS
        for ak_symbol in {"SH600519", "SZ000001"}
    }


def test_report_date_mode_creates_one_task_per_selected_report_date_per_summary_api(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def _adapter(api_name: str):
        def fn(date: str) -> pd.DataFrame:
            calls.append((api_name, date))
            return pd.DataFrame({"股票代码": ["600519"], "资产合计": [1.0]})

        return fn

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331", "20240630"],
        selected_api_names=sorted(REPORT_DATE_APIS),
        adapter_map={api_name: _adapter(api_name) for api_name in REPORT_DATE_APIS},
        max_workers=1,
    )

    assert len(out["rows"]) == 6
    assert {(api, date) for api, date in calls} == {
        (api_name, report_date)
        for api_name in REPORT_DATE_APIS
        for report_date in {"20240331", "20240630"}
    }


@pytest.mark.parametrize(
    ("input_symbol", "logical_symbol", "akshare_symbol"),
    [
        ("600519", "600519", "SH600519"),
        ("000001", "000001", "SZ000001"),
        ("300750", "300750", "SZ300750"),
        ("SH600519", "600519", "SH600519"),
        ("SZ000001", "000001", "SZ000001"),
    ],
)
def test_financial_statement_symbol_conversion(input_symbol: str, logical_symbol: str, akshare_symbol: str) -> None:
    assert _financial_statement_symbol_pair(input_symbol) == (logical_symbol, akshare_symbol)


def test_invalid_financial_statement_symbol_format_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="malformed financial statement symbol"):
        _financial_statement_symbol_pair("abc000001")
    with pytest.raises(ValueError, match="unsupported financial statement symbol prefix"):
        _financial_statement_symbol_pair("830000")


def test_partition_json_for_symbol_mode_uses_logical_six_digit_symbol(tmp_path: Path) -> None:
    def stock_balance_sheet_by_report_em(symbol: str) -> pd.DataFrame:
        assert symbol == "SH600519"
        return pd.DataFrame({"REPORT_DATE": ["2024-03-31"]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["SH600519"],
        selected_api_names=["stock_balance_sheet_by_report_em"],
        adapter_map={"stock_balance_sheet_by_report_em": stock_balance_sheet_by_report_em},
        max_workers=1,
    )

    [row] = out["rows"]
    assert json.loads(row["partition_json"]) == {"symbol": "600519"}
    assert row["original_symbol"] == "600519"
    assert row["akshare_symbol"] == "SH600519"
    assert "financial_fundamental/stock_balance_sheet_by_report_em/symbol=600519/data.parquet" in row["output_path"]
    assert _build_raw_partition("financial_fundamental", "stock_balance_sheet_by_report_em", {"symbol": "600519"}, {"symbol": "SH600519"}) == {"symbol": "600519"}


def test_partition_json_for_report_date_mode_uses_report_date(tmp_path: Path) -> None:
    def stock_zcfz_em(date: str) -> pd.DataFrame:
        assert date == "20240331"
        return pd.DataFrame({"股票代码": ["600519"], "资产合计": [1.0]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331"],
        selected_api_names=["stock_zcfz_em"],
        adapter_map={"stock_zcfz_em": stock_zcfz_em},
        max_workers=1,
    )

    [row] = out["rows"]
    assert json.loads(row["partition_json"]) == {"report_date": "20240331"}
    assert "financial_fundamental/stock_zcfz_em/report_date=20240331/data.parquet" in row["output_path"]
    assert _build_raw_partition("financial_fundamental", "stock_zcfz_em", {"date": "20240331"}, {"date": "20240331"}) == {"report_date": "20240331"}


def test_synthetic_wide_dataframe_columns_are_preserved_unchanged(tmp_path: Path) -> None:
    expected_columns = ["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE", "REPORT_TYPE", "REPORT_DATE_NAME", "负债合计", "所有者权益合计"]

    def stock_profit_sheet_by_report_em(symbol: str) -> pd.DataFrame:
        assert symbol == "SZ300750"
        return pd.DataFrame([["2024-03-31", "2024-04-20", "2024-04-21", "一季报", "2024一季报", 1, 2]], columns=expected_columns)

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["300750"],
        selected_api_names=["stock_profit_sheet_by_report_em"],
        adapter_map={"stock_profit_sheet_by_report_em": stock_profit_sheet_by_report_em},
        max_workers=1,
    )

    [row] = out["rows"]
    raw = pd.read_parquet(row["output_path"])
    assert list(raw.columns) == expected_columns


def test_synthetic_summary_dataframe_chinese_columns_are_preserved_unchanged(tmp_path: Path) -> None:
    expected_columns = ["股票代码", "股票简称", "公告日期", "净利润", "经营现金流"]

    def stock_xjll_em(date: str) -> pd.DataFrame:
        assert date == "20240331"
        return pd.DataFrame([["600519", "贵州茅台", "2024-04-03", 1, 2]], columns=expected_columns)

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        report_dates=["20240331"],
        selected_api_names=["stock_xjll_em"],
        adapter_map={"stock_xjll_em": stock_xjll_em},
        max_workers=1,
    )

    [row] = out["rows"]
    raw = pd.read_parquet(row["output_path"])
    assert list(raw.columns) == expected_columns


def test_existing_financial_fundamental_apis_remain_registered() -> None:
    api_names = {spec["api_name"] for spec in COVERAGE_API_SPECS["financial_fundamental"]}
    assert EXISTING_FINANCIAL_APIS <= api_names


def test_financial_statement_params_modes_are_minimal() -> None:
    assert _params_for_mode("financial_statement_symbol", ["600519", "000001"], [], [], [], [], [], "20200101", "20200101") == [
        {"symbol": "600519"},
        {"symbol": "000001"},
    ]
    assert _params_for_mode("financial_statement_report_date", [], [], ["20240331", "20240630"], [], [], [], "20200101", "20200101") == [
        {"date": "20240331"},
        {"date": "20240630"},
    ]


def test_no_forbidden_behavior_added_to_registration_files() -> None:
    assert STATEMENT_APIS.isdisjoint({"stock_financial_analysis_indicator", "stock_financial_analysis_indicator_em", "stock_yjyg_em", "stock_yysj_em"})
    for policy in (API_POLICY_METADATA[("financial_fundamental", api_name)] for api_name in STATEMENT_APIS):
        policy_text = json.dumps(policy, ensure_ascii=False).lower()
        for forbidden in FORBIDDEN_BEHAVIOR_TOKENS:
            if forbidden == "factor":
                # The existing source family is named financial_fundamental under factor_lake.
                continue
            assert forbidden not in policy_text
