from __future__ import annotations

from collections.abc import Iterable

from .schemas import SourceCase

API_BY_FAMILY = {
    "market_price": ["stock_zh_a_hist", "stock_individual_info_em"],
    "index_market": ["index_zh_a_hist", "stock_zh_index_hist_csindex", "stock_zh_index_daily", "stock_zh_index_daily_em", "index_hist_cni", "index_detail_hist_cni", "index_component_sw", "index_hist_sw", "index_realtime_sw", "index_stock_cons_csindex", "index_stock_cons_weight_csindex"],
    "financial_fundamental": ["stock_financial_analysis_indicator", "stock_yjyg_em", "stock_yysj_em"],
    "margin_leverage": ["stock_margin_sse", "stock_margin_detail_sse", "stock_margin_szse", "stock_margin_detail_szse", "stock_margin_underlying_info_szse", "macro_china_market_margin_sz"],
    "industry_concept": ["stock_industry_category_cninfo", "stock_industry_change_cninfo", "stock_industry_clf_hist_sw", "sw_index_first_info", "sw_index_second_info", "sw_index_third_info", "sw_index_third_cons", "stock_board_industry_name_ths", "stock_board_industry_cons_ths", "stock_board_industry_index_ths", "stock_board_industry_info_ths", "stock_board_industry_summary_ths", "stock_board_concept_name_ths", "stock_board_concept_cons_ths", "stock_board_concept_index_ths", "stock_board_concept_info_ths", "stock_board_concept_summary_ths", "stock_board_industry_name_em", "stock_board_industry_cons_em", "stock_board_industry_hist_em", "stock_board_concept_name_em", "stock_board_concept_cons_em", "stock_board_concept_hist_em"],
    "event_ownership": ["stock_zh_a_gdhs", "stock_zh_a_gdhs_detail_em", "stock_gdfx_free_holding_analyse_em", "stock_gdfx_holding_analyse_em", "stock_gpzy_pledge_ratio_em", "stock_gpzy_pledge_ratio_detail_em", "stock_gpzy_industry_data_em", "stock_gpzy_profile_em", "stock_fhps_em", "stock_history_dividend", "stock_history_dividend_detail", "stock_restricted_release_queue_em", "stock_restricted_release_summary_em", "stock_restricted_release_detail_em", "stock_dzjy_sctj", "stock_dzjy_mrmx", "stock_dzjy_mrtj", "stock_dzjy_hyyybtj", "stock_lhb_detail_em", "stock_lhb_stock_statistic_em", "stock_lhb_jgmmtj_em", "stock_lhb_hyyyb_em", "stock_lhb_yybph_em"],
    "disclosure_ir": ["stock_zh_a_disclosure_relation_cninfo", "stock_jgdy_tj_em", "stock_jgdy_detail_em"],
    "trading_attention": ["stock_hot_rank_em", "stock_hot_up_em", "stock_hot_follow_xq", "stock_changes_em"],
    "corporate_action": ["stock_register_kcb", "stock_register_cyb"],
}

KWARGS_BY_API: dict[str, dict] = {
    "stock_zh_a_hist": {"symbol": "000001", "period": "daily", "start_date": "20240101", "end_date": "20240331", "adjust": "qfq"},
    "stock_individual_info_em": {"symbol": "000001"},
    "index_zh_a_hist": {"symbol": "000300", "period": "daily", "start_date": "20240101", "end_date": "20240331"},
    "stock_zh_index_hist_csindex": {"symbol": "000905", "start_date": "20240101", "end_date": "20240331"},
    "stock_zh_index_daily": {"symbol": "sh000001"},
    "stock_zh_index_daily_em": {"symbol": "000852"},
    "index_hist_cni": {"symbol": "000300", "start_date": "20240101", "end_date": "20240331"},
    "index_detail_hist_cni": {"symbol": "000905", "start_date": "20240101", "end_date": "20240331"},
    "index_component_sw": {"symbol": "801010"},
    "index_hist_sw": {"symbol": "801030", "period": "day"},
    "index_realtime_sw": {"symbol": "801080"},
    "index_stock_cons_csindex": {"symbol": "000300"},
    "index_stock_cons_weight_csindex": {"symbol": "000300"},
    "stock_financial_analysis_indicator": {"symbol": "000001"},
    "stock_yjyg_em": {"date": "20240331"},
    "stock_yysj_em": {"date": "20240331"},
    "stock_zh_a_disclosure_relation_cninfo": {"symbol": "000001", "start_date": "20240101", "end_date": "20240331"},
    "stock_jgdy_tj_em": {"date": "20240331"},
    "stock_jgdy_detail_em": {"date": "20240331"},
    "stock_margin_sse": {"start_date": "20240101", "end_date": "20240331"},
    "stock_margin_detail_sse": {"date": "20240329"},
    "stock_margin_szse": {"date": "20240329"},
    "stock_margin_detail_szse": {"date": "20240329"},
    "stock_margin_underlying_info_szse": {"date": "20240329"},
    "macro_china_market_margin_sz": {},
    "stock_industry_clf_hist_sw": {"symbol": "801120", "start_date": "20240101", "end_date": "20240331"},
    "sw_index_third_cons": {"symbol": "801780"},
    "stock_board_industry_cons_ths": {"symbol": "半导体"},
    "stock_board_industry_index_ths": {"symbol": "半导体", "start_date": "20240101", "end_date": "20240331"},
    "stock_board_industry_summary_ths": {"symbol": "半导体"},
    "stock_board_concept_cons_ths": {"symbol": "AI PC"},
    "stock_board_concept_index_ths": {"symbol": "AI PC", "start_date": "20240101", "end_date": "20240331"},
    "stock_board_concept_summary_ths": {"symbol": "AI PC"},
    "stock_board_industry_cons_em": {"symbol": "半导体"},
    "stock_board_industry_hist_em": {"symbol": "半导体", "start_date": "20240101", "end_date": "20240331", "period": "日k", "adjust": ""},
    "stock_board_concept_cons_em": {"symbol": "AI PC"},
    "stock_board_concept_hist_em": {"symbol": "AI PC", "start_date": "20240101", "end_date": "20240331", "period": "日k", "adjust": ""},
    "stock_zh_a_gdhs": {"symbol": "000001"},
    "stock_zh_a_gdhs_detail_em": {"symbol": "000001"},
    "stock_gdfx_free_holding_analyse_em": {"date": "20240331"},
    "stock_gdfx_holding_analyse_em": {"date": "20240331"},
    "stock_gpzy_pledge_ratio_detail_em": {"date": "20240331"},
    "stock_fhps_em": {"date": "20240331"},
    "stock_history_dividend": {"symbol": "000001"},
    "stock_history_dividend_detail": {"symbol": "000001", "date": "20240331"},
    "stock_restricted_release_detail_em": {"date": "20240331"},
    "stock_dzjy_mrmx": {"date": "20240329"},
    "stock_dzjy_mrtj": {"date": "20240329"},
    "stock_lhb_detail_em": {"date": "20240329"},
    "stock_hot_rank_em": {},
    "stock_hot_up_em": {},
    "stock_hot_follow_xq": {"symbol": "SZ000001"},
    "stock_changes_em": {"symbol": "火箭发射"},
    "stock_register_kcb": {},
    "stock_register_cyb": {},
}


def _kwargs_for_api(api_name: str) -> dict:
    return dict(KWARGS_BY_API.get(api_name, {}))


def build_registry() -> list[SourceCase]:
    cases: list[SourceCase] = []
    for fam, apis in API_BY_FAMILY.items():
        for api in apis:
            kwargs = _kwargs_for_api(api)
            identity = kwargs.get("symbol") or kwargs.get("date") or "default"
            case_id = f"{api}__{identity}__2024q1".replace(" ", "_")
            cases.append(SourceCase(case_id=case_id, source_family=fam, api_name=api, kwargs=kwargs, description="Migrated from Factor_test verified probe recipe", enabled=True))
    return cases


FACTOR_SOURCE_REGISTRY = build_registry()


def filter_source_cases(cases: Iterable[SourceCase], family: str | None = None, api_name: str | None = None, case_id: str | None = None, enabled_only: bool = False, max_cases: int | None = None) -> list[SourceCase]:
    selected = []
    for c in cases:
        if family and c.source_family != family:
            continue
        if api_name and c.api_name != api_name:
            continue
        if case_id and c.case_id != case_id:
            continue
        if enabled_only and not c.enabled:
            continue
        selected.append(c)
    return selected[:max_cases] if max_cases else selected
