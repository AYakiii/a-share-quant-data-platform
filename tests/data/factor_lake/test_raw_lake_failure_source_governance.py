from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.raw_ingest import API_POLICY_METADATA, COVERAGE_API_SPECS, run_raw_coverage_ingest


DOWNGRADED_STRUCTURAL_APIS = {
    "stock_board_concept_info_ths",
    "stock_board_industry_info_ths",
    "stock_board_concept_summary_ths",
    "stock_board_industry_summary_ths",
    "stock_industry_clf_hist_sw",
}

HEALTHY_DEFAULT_APIS = {
    ("financial_fundamental", "stock_balance_sheet_by_report_em"),
    ("financial_fundamental", "stock_profit_sheet_by_report_em"),
    ("financial_fundamental", "stock_cash_flow_sheet_by_report_em"),
    ("corporate_action", "stock_history_dividend_detail"),
    ("disclosure_ir", "stock_zh_a_disclosure_relation_cninfo"),
    ("event_ownership", "stock_zh_a_gdhs_detail_em"),
    ("margin_leverage", "stock_margin_detail_sse"),
    ("margin_leverage", "stock_margin_detail_szse"),
    ("margin_leverage", "stock_margin_underlying_info_szse"),
}


def _coverage_pairs() -> set[tuple[str, str]]:
    return {
        (family, spec["api_name"])
        for family, specs in COVERAGE_API_SPECS.items()
        for spec in specs
    }


def test_structural_ths_and_sw_sources_are_registered_manual_review_and_default_skipped(tmp_path: Path) -> None:
    coverage_pairs = _coverage_pairs()
    assert {("industry_concept", api_name) for api_name in DOWNGRADED_STRUCTURAL_APIS} <= coverage_pairs

    for api_name in DOWNGRADED_STRUCTURAL_APIS:
        policy = API_POLICY_METADATA[("industry_concept", api_name)]
        assert policy["enabled"] is False
        assert policy["default_enabled"] is False
        assert policy["manual_review_required"] is True
        assert policy["acquisition_mode"] == "manual_selected_only"
        assert "disabled_reason" in policy

    calls: list[str] = []

    def disabled_adapter(**kwargs: str) -> pd.DataFrame:  # noqa: ARG001
        calls.append("called")
        return pd.DataFrame({"x": [1]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["industry_concept"],
        symbols=["000001"],
        industry_names=["银行"],
        concept_names=["人工智能"],
        industry_codes=["801010"],
        adapter_map={api_name: disabled_adapter for api_name in DOWNGRADED_STRUCTURAL_APIS},
        max_workers=1,
    )

    target_rows = [row for row in out["rows"] if row["api_name"] in DOWNGRADED_STRUCTURAL_APIS]
    assert len(target_rows) == 5
    assert calls == []
    assert {row["status"] for row in target_rows} == {"skipped"}
    assert {row["error_type"] for row in target_rows} == {"default_disabled"}
    assert all("disabled_reason:" in row["error_message"] for row in target_rows)


def test_manual_selection_can_still_include_downgraded_ths_and_sw_sources(tmp_path: Path) -> None:
    def stock_board_concept_info_ths(**kwargs: str) -> pd.DataFrame:
        return pd.DataFrame({"name": [kwargs["symbol"]], "value": [1]})

    def stock_industry_clf_hist_sw(symbol: str) -> pd.DataFrame:
        return pd.DataFrame({"symbol": [symbol], "industry": ["银行"]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["industry_concept"],
        symbols=["000001"],
        concept_names=["人工智能"],
        selected_api_names=["stock_board_concept_info_ths", "stock_industry_clf_hist_sw"],
        adapter_map={
            "stock_board_concept_info_ths": stock_board_concept_info_ths,
            "stock_industry_clf_hist_sw": stock_industry_clf_hist_sw,
        },
        max_workers=1,
    )

    status_by_api = {row["api_name"]: row["status"] for row in out["rows"]}
    assert status_by_api == {
        "stock_board_concept_info_ths": "success",
        "stock_industry_clf_hist_sw": "success",
    }


def test_czce_mixed_text_object_columns_write_parquet_successfully(tmp_path: Path) -> None:
    def futures_warehouse_receipt_czce(date: str) -> dict[str, pd.DataFrame]:
        return {
            "SR": pd.DataFrame(
                {
                    "warehouse": ["郑州仓", 1001],
                    "receipt_delta": [1, 2],
                    "note": ["新增", None],
                }
            )
        }

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["commodity_inventory"],
        trade_dates=["20240102"],
        selected_api_names=["futures_warehouse_receipt_czce"],
        adapter_map={"futures_warehouse_receipt_czce": futures_warehouse_receipt_czce},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "success"
    raw = pd.read_parquet(row["output_path"])
    assert list(raw.columns) == ["warehouse", "receipt_delta", "note", "product_key", "source_api", "exchange", "trade_date"]
    assert raw["warehouse"].tolist() == ["郑州仓", "1001"]
    assert raw["receipt_delta"].tolist() == [1, 2]
    assert raw["exchange"].tolist() == ["CZCE", "CZCE"]


def test_investigation_markers_for_shfe_and_concept_fund_flow() -> None:
    shfe_policy = API_POLICY_METADATA[("commodity_inventory", "futures_shfe_warehouse_receipt")]
    assert shfe_policy["default_enabled"] is False
    assert shfe_policy["manual_review_required"] is True
    assert shfe_policy["acquisition_mode"] == "manual_selected_only"
    assert "non-JSON" in str(shfe_policy["disabled_reason"])
    assert "date compatibility" in str(shfe_policy["disabled_reason"])

    concept_policy = API_POLICY_METADATA[("market_sentiment", "stock_fund_flow_concept")]
    assert concept_policy["default_enabled"] is False
    assert concept_policy["manual_review_required"] is True
    assert concept_policy["acquisition_mode"] == "manual_selected_only"
    assert "page/table parse" in str(concept_policy["disabled_reason"])


def test_healthy_non_structural_sources_remain_default_acquirable() -> None:
    coverage_pairs = _coverage_pairs()
    assert HEALTHY_DEFAULT_APIS <= coverage_pairs

    for pair in HEALTHY_DEFAULT_APIS:
        policy = API_POLICY_METADATA.get(pair, {})
        assert policy.get("default_enabled", True) is not False
        assert policy.get("acquisition_mode") != "manual_selected_only"

    financial_indicator_policy = API_POLICY_METADATA[("financial_fundamental", "stock_financial_analysis_indicator_em")]
    assert financial_indicator_policy["default_enabled"] is False
    assert financial_indicator_policy["acquisition_mode"] == "manual_selected_only"
