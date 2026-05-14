from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.source_gap_audit import (
    VIABLE_SOURCE_COLUMNS,
    build_expansion_plan,
    build_gap_matrix,
    load_viable_sources,
    run_source_gap_audit,
)

REQUIRED_APIS = {
"stock_zh_a_hist","stock_individual_info_em","stock_zh_index_hist_csindex","index_stock_cons_csindex","index_stock_cons_weight_csindex",
"stock_financial_analysis_indicator","stock_yjyg_em","stock_yysj_em","stock_margin_sse","stock_margin_detail_sse","stock_margin_szse",
"stock_margin_detail_szse","stock_margin_underlying_info_szse","stock_industry_category_cninfo","stock_industry_change_cninfo",
"stock_industry_clf_hist_sw","sw_index_first_info","sw_index_second_info","sw_index_third_info","index_component_sw","index_hist_sw",
"index_realtime_sw","stock_board_industry_name_ths","stock_board_industry_index_ths","stock_board_industry_info_ths",
"stock_board_industry_summary_ths","stock_board_concept_name_ths","stock_board_concept_index_ths","stock_board_concept_info_ths",
"stock_board_concept_summary_ths","stock_zh_a_gdhs","stock_zh_a_gdhs_detail_em","stock_gdfx_free_holding_analyse_em",
"stock_gdfx_holding_analyse_em","stock_gpzy_pledge_ratio_em","stock_gpzy_pledge_ratio_detail_em","stock_gpzy_industry_data_em",
"stock_gpzy_profile_em","stock_fhps_em","stock_history_dividend","stock_history_dividend_detail","stock_restricted_release_queue_em",
"stock_restricted_release_summary_em","stock_restricted_release_detail_em","stock_dzjy_sctj","stock_dzjy_mrmx","stock_dzjy_mrtj",
"stock_dzjy_hyyybtj","stock_lhb_detail_em","stock_lhb_stock_statistic_em","stock_lhb_jgmmtj_em","stock_lhb_hyyyb_em",
"stock_lhb_yybph_em","stock_jgdy_tj_em",
}


def test_viable_csv_contains_exact_required_54():
    df = load_viable_sources("config/factor_sources/factor_test_viable_sources_v0.csv")
    assert len(df) == 54
    assert set(df["api_name"]) == REQUIRED_APIS
    assert set(df.columns) == set(VIABLE_SOURCE_COLUMNS)


def test_gap_statuses_registry_catalog_health_and_planning(tmp_path):
    viable = pd.DataFrame([
        ["market_price", "stock_zh_a_hist", "success", 1, "panel", "mixed", "", "date", "symbol", "ts", "low", False, "d", True, "r"],
        ["trading_attention", "stock_lhb_detail_em", "success", 1, "panel", "mixed", "", "date", "symbol", "ts", "low", False, "d", True, "r"],
        ["corporate_action", "stock_history_dividend_detail", "success", 1, "panel", "mixed", "", "date", "symbol", "ts", "high", True, "d", True, "r"],
    ], columns=VIABLE_SOURCE_COLUMNS)
    coverage = pd.DataFrame([{"source_family": "market_price", "api_name": "stock_zh_a_hist", "in_registry": True}])
    catalog = pd.DataFrame([{"source_family": "trading_attention", "api_name": "stock_lhb_detail_em"}])
    health = pd.DataFrame([{"source_family": "trading_attention", "api_name": "stock_lhb_detail_em"}])

    gap = build_gap_matrix(viable, coverage, catalog, health)
    rs = dict(zip(gap["api_name"], gap["registry_status"]))
    cs = dict(zip(gap["api_name"], gap["catalog_status"]))
    hs = dict(zip(gap["api_name"], gap["audit_status"]))
    ps = dict(zip(gap["api_name"], gap["coverage_status"]))
    assert rs["stock_zh_a_hist"] == "already_in_registry"
    assert cs["stock_lhb_detail_em"] == "already_seen_in_catalog"
    assert hs["stock_lhb_detail_em"] == "already_audited"
    assert ps["stock_history_dividend_detail"] == "planned_for_expansion"

    plan = build_expansion_plan(gap)
    assert set(plan["api_name"]) == {"stock_history_dividend_detail"}
    assert plan.iloc[0]["contains_ex_post_fields"]


def test_cli_writes_outputs_no_network(tmp_path):
    viable = pd.DataFrame([["market_price", "stock_zh_a_hist", "success", 1, "panel", "mixed", "", "date", "symbol", "ts", "low", False, "d", True, "r"]], columns=VIABLE_SOURCE_COLUMNS)
    v = tmp_path / "viable.csv"
    viable.to_csv(v, index=False)
    out = run_source_gap_audit(v, tmp_path)
    assert (tmp_path / "factor_test_source_gap_matrix.csv").exists()
    assert (tmp_path / "raw_coverage_registry_expansion_plan.csv").exists()
    assert out["gap_matrix_path"].endswith("factor_test_source_gap_matrix.csv")
