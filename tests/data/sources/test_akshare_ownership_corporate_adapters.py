from __future__ import annotations

import sys
import types

import pandas as pd

from qsys.data.sources import (
    SourceFetchResult,
    fetch_stock_fhps_em,
    fetch_stock_gdfx_free_holding_analyse_em,
    fetch_stock_gdfx_holding_analyse_em,
    fetch_stock_gpzy_industry_data_em,
    fetch_stock_gpzy_pledge_ratio_detail_em,
    fetch_stock_gpzy_pledge_ratio_em,
    fetch_stock_gpzy_profile_em,
    fetch_stock_history_dividend,
    fetch_stock_history_dividend_detail,
    fetch_stock_restricted_release_detail_em,
    fetch_stock_restricted_release_queue_em,
    fetch_stock_restricted_release_summary_em,
    fetch_stock_zh_a_gdhs,
    fetch_stock_zh_a_gdhs_detail_em,
)


def _install(monkeypatch, **fns):
    monkeypatch.setitem(sys.modules, "akshare", types.SimpleNamespace(**fns))


def _assert(r: SourceFetchResult, api: str, fam: str):
    assert isinstance(r, SourceFetchResult)
    assert r.metadata["api_name"] == api
    assert r.metadata["source_family"] == fam


def test_ownership_adapters(monkeypatch):
    _install(
        monkeypatch,
        stock_zh_a_gdhs=lambda: pd.DataFrame({"股票代码": ["000001"]}),
        stock_zh_a_gdhs_detail_em=lambda symbol: pd.DataFrame({"股票代码": [symbol]}),
        stock_gdfx_free_holding_analyse_em=lambda: pd.DataFrame({"股东名称": ["x"]}),
        stock_gdfx_holding_analyse_em=lambda: pd.DataFrame({"股东名称": ["y"]}),
        stock_gpzy_pledge_ratio_em=lambda: pd.DataFrame({"股票代码": ["000001"]}),
        stock_gpzy_pledge_ratio_detail_em=lambda: pd.DataFrame({"股票代码": ["000001"]}),
        stock_gpzy_industry_data_em=lambda: pd.DataFrame({"行业": ["银行"]}),
        stock_gpzy_profile_em=lambda: pd.DataFrame({"统计": [1]}),
    )
    _assert(fetch_stock_zh_a_gdhs(), "stock_zh_a_gdhs", "ownership_structure")
    r = fetch_stock_zh_a_gdhs_detail_em("000001")
    _assert(r, "stock_zh_a_gdhs_detail_em", "ownership_structure")
    assert r.metadata["request_params"]["symbol"] == "000001"
    _assert(fetch_stock_gdfx_free_holding_analyse_em(), "stock_gdfx_free_holding_analyse_em", "ownership_structure")
    _assert(fetch_stock_gdfx_holding_analyse_em(), "stock_gdfx_holding_analyse_em", "ownership_structure")
    _assert(fetch_stock_gpzy_pledge_ratio_em(), "stock_gpzy_pledge_ratio_em", "ownership_governance_risk")
    _assert(fetch_stock_gpzy_pledge_ratio_detail_em(), "stock_gpzy_pledge_ratio_detail_em", "ownership_governance_risk")
    _assert(fetch_stock_gpzy_industry_data_em(), "stock_gpzy_industry_data_em", "ownership_governance_risk")
    _assert(fetch_stock_gpzy_profile_em(), "stock_gpzy_profile_em", "ownership_governance_risk")


def test_corporate_action_adapters(monkeypatch):
    _install(
        monkeypatch,
        stock_fhps_em=lambda: pd.DataFrame({"预案公告日": ["2024-01-01"], "股权登记日": ["2024-01-05"], "除权除息日": ["2024-01-06"]}),
        stock_history_dividend=lambda: pd.DataFrame({"公告日期": ["2024-01-01"]}),
        stock_history_dividend_detail=lambda symbol: pd.DataFrame({"代码": [symbol]}),
        stock_restricted_release_queue_em=lambda: pd.DataFrame({"解禁时间": ["2024-01-01"]}),
        stock_restricted_release_summary_em=lambda: pd.DataFrame({"解禁数量": [10]}),
        stock_restricted_release_detail_em=lambda: pd.DataFrame({"解禁时间": ["2024-01-01"], "解禁后20日涨跌幅": [0.1]}),
    )
    r = fetch_stock_fhps_em()
    _assert(r, "stock_fhps_em", "corporate_action")
    assert {"预案公告日", "股权登记日", "除权除息日"}.issubset(r.raw.columns)
    _assert(fetch_stock_history_dividend(), "stock_history_dividend", "corporate_action")
    d = fetch_stock_history_dividend_detail("000001")
    _assert(d, "stock_history_dividend_detail", "corporate_action")
    assert d.metadata["request_params"]["symbol"] == "000001"
    _assert(fetch_stock_restricted_release_queue_em(), "stock_restricted_release_queue_em", "corporate_action")
    _assert(fetch_stock_restricted_release_summary_em(), "stock_restricted_release_summary_em", "corporate_action")
    rd = fetch_stock_restricted_release_detail_em()
    _assert(rd, "stock_restricted_release_detail_em", "corporate_action")
    assert {"解禁时间", "解禁后20日涨跌幅"}.issubset(rd.raw.columns)
