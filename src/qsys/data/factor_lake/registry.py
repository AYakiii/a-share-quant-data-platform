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
}

COMMON_KWARGS = {"symbol": "000001", "start_date": "20240101", "end_date": "20240331", "adjust": "qfq", "period": "daily", "date": "20240331", "market": "全部", "indicator": "按报告期"}


def _kwargs_for_api(api_name: str) -> dict:
    kw = dict(COMMON_KWARGS)
    if api_name == "stock_board_industry_index_ths":
        kw.update({"symbol": "半导体"})
    elif api_name == "stock_board_concept_summary_ths":
        kw.update({"symbol": "AI PC"})
    elif api_name == "index_component_sw":
        kw.update({"symbol": "801010"})
    elif api_name == "stock_zh_a_hist":
        kw.update({"symbol": "000001"})
    return kw


def build_registry() -> list[SourceCase]:
    cases: list[SourceCase] = []
    for fam, apis in API_BY_FAMILY.items():
        for api in apis:
            kwargs = _kwargs_for_api(api)
            cid = f"{api}__{kwargs.get('symbol','default')}__2024q1".replace(" ", "_")
            cases.append(SourceCase(case_id=cid, source_family=fam, api_name=api, kwargs=kwargs, description="Migrated from Factor_test verified probe recipe", enabled=True))
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
