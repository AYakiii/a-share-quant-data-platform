from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

CLOSURE_CSV = Path("config/factor_sources/raw_source_integration_closure_20260530.csv")
CLOSURE_MD = Path("config/factor_sources/raw_source_integration_closure_20260530.md")
HISTORICAL_PLAN = Path("config/factor_sources/p15p2_recovered_source_registration_plan.csv")
HISTORICAL_PLAN_SHA256 = "53616561cae63eba4c7e8d009eb9edcc8044aba05cf1431e87fb39018a7a6eb6"

REQUIRED_COLUMNS = [
    "source_family",
    "api_name",
    "priority_tier",
    "data_theme",
    "operational_status",
    "last_verified_rows",
    "last_verified_scope",
    "replacement_for",
    "default_enabled",
    "manual_review_required",
    "acquisition_mode",
    "recommended_next_action",
    "evidence_note",
]

REQUIRED_APIS = {
    ("market_sentiment", "stock_fund_flow_concept"),
    ("market_sentiment", "stock_fund_flow_industry"),
    ("market_sentiment", "stock_hsgt_fund_flow_summary_em"),
    ("commodity_inventory", "futures_inventory_em"),
    ("commodity_inventory", "futures_comex_inventory"),
    ("commodity_inventory", "futures_gfex_warehouse_receipt"),
    ("commodity_inventory", "futures_shfe_warehouse_receipt"),
    ("commodity_inventory", "futures_warehouse_receipt_czce"),
    ("financial_fundamental", "stock_financial_analysis_indicator_em"),
    ("financial_fundamental", "stock_financial_analysis_indicator"),
    ("disclosure_ir", "stock_zh_a_disclosure_relation_cninfo"),
    ("disclosure_ir", "stock_jgdy_detail_em"),
    ("event_ownership", "stock_gdfx_holding_analyse_em"),
    ("market_sentiment", "stock_market_fund_flow"),
    ("market_sentiment", "stock_individual_fund_flow_rank"),
    ("market_sentiment", "stock_sector_fund_flow_rank"),
    ("market_sentiment", "stock_sector_fund_flow_summary"),
    ("commodity_inventory", "futures_inventory_99"),
    ("event_ownership", "stock_gdfx_free_holding_analyse_em"),
    ("event_ownership", "stock_gpzy_pledge_ratio_detail_em"),
}

WAVE1_APIS = {
    ("market_sentiment", "stock_fund_flow_concept"),
    ("market_sentiment", "stock_fund_flow_industry"),
    ("market_sentiment", "stock_hsgt_fund_flow_summary_em"),
    ("commodity_inventory", "futures_inventory_em"),
    ("commodity_inventory", "futures_comex_inventory"),
    ("commodity_inventory", "futures_gfex_warehouse_receipt"),
    ("commodity_inventory", "futures_shfe_warehouse_receipt"),
    ("commodity_inventory", "futures_warehouse_receipt_czce"),
}

DEFERRED_APIS = {
    ("market_sentiment", "stock_market_fund_flow"),
    ("market_sentiment", "stock_individual_fund_flow_rank"),
    ("market_sentiment", "stock_sector_fund_flow_rank"),
    ("market_sentiment", "stock_sector_fund_flow_summary"),
    ("commodity_inventory", "futures_inventory_99"),
    ("event_ownership", "stock_gdfx_free_holding_analyse_em"),
    ("event_ownership", "stock_gpzy_pledge_ratio_detail_em"),
}


def _closure() -> pd.DataFrame:
    assert CLOSURE_CSV.exists()
    return pd.read_csv(CLOSURE_CSV, dtype=str).fillna("")


def _row(df: pd.DataFrame, family: str, api_name: str) -> pd.Series:
    match = df[(df["source_family"] == family) & (df["api_name"] == api_name)]
    assert len(match) == 1
    return match.iloc[0]


def test_closure_csv_exists_with_required_columns_and_apis() -> None:
    df = _closure()
    assert list(df.columns) == REQUIRED_COLUMNS
    assert len(df) == 20
    present = set(zip(df["source_family"], df["api_name"], strict=True))
    assert REQUIRED_APIS <= present


def test_wave1_integrated_sources_are_smoke_success_and_default_disabled() -> None:
    df = _closure()
    for family, api_name in WAVE1_APIS:
        row = _row(df, family, api_name)
        assert row["operational_status"] == "integrated_live_smoke_success"
        assert row["default_enabled"] == "false"
        assert row["manual_review_required"] == "true"


def test_financial_indicator_sources_record_current_manual_status_and_lineage() -> None:
    df = _closure()
    em = _row(df, "financial_fundamental", "stock_financial_analysis_indicator_em")
    assert em["acquisition_mode"] == "manual_selected_only"
    assert em["default_enabled"] == "false"
    assert "NOTICE_DATE" in em["evidence_note"]
    assert "REPORT_DATE" in em["evidence_note"]

    legacy = _row(df, "financial_fundamental", "stock_financial_analysis_indicator")
    assert legacy["operational_status"] == "legacy_direct_manual_verified"
    assert legacy["last_verified_rows"] == "25"
    assert legacy["acquisition_mode"] == "legacy_direct_manual_only"
    assert "start_year=1900" in legacy["evidence_note"]


def test_heavy_disclosure_and_ownership_success_rows_are_recorded() -> None:
    df = _closure()
    jgdy = _row(df, "disclosure_ir", "stock_jgdy_detail_em")
    assert jgdy["operational_status"] == "resilient_paged_adapter_live_success"
    assert jgdy["last_verified_rows"] == "3395"
    assert "since_date=20260520" in jgdy["last_verified_scope"]
    assert "total_pages=68" in jgdy["last_verified_scope"]
    assert jgdy["acquisition_mode"] == "long_detail_run"
    assert "page_catalog.csv" in jgdy["evidence_note"]
    assert "crawl_manifest.json" in jgdy["evidence_note"]
    assert "snapshot-drift guard" in jgdy["evidence_note"]

    gdfx = _row(df, "event_ownership", "stock_gdfx_holding_analyse_em")
    assert gdfx["operational_status"] == "recovered_heavy_live_success"
    assert gdfx["last_verified_rows"] == "123910"
    assert gdfx["acquisition_mode"] == "long_recovery_run"
    assert "787.49 sec" in gdfx["evidence_note"]
    assert "completed partition skipped on resume" in gdfx["evidence_note"]


def test_network_unstable_and_deferred_sources_remain_represented() -> None:
    df = _closure()
    market = _row(df, "market_sentiment", "stock_market_fund_flow")
    assert market["operational_status"] == "network_unstable_deferred"
    assert "five consecutive RemoteDisconnected" in market["evidence_note"]

    present = set(zip(df["source_family"], df["api_name"], strict=True))
    assert DEFERRED_APIS <= present
    for family, api_name in DEFERRED_APIS:
        row = _row(df, family, api_name)
        assert row["default_enabled"] == "false"
        assert row["recommended_next_action"]


def test_historical_plan_is_preserved_and_markdown_records_next_stage() -> None:
    assert HISTORICAL_PLAN.exists()
    digest = hashlib.sha256(HISTORICAL_PLAN.read_bytes()).hexdigest()
    assert digest == HISTORICAL_PLAN_SHA256

    assert CLOSURE_MD.exists()
    text = CLOSURE_MD.read_text(encoding="utf-8")
    assert "Raw Data Lake Controlled Construction" in text
    assert "Do not start normalized" in text
    assert "p15p2_recovered_source_registration_plan.csv" in text
    assert "historical pre-integration registration plan" in text
