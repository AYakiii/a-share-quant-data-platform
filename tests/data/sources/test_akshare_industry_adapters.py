from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

from qsys.data.sources import (
    SourceFetchResult,
    fetch_index_component_sw,
    fetch_index_hist_sw,
    fetch_stock_board_concept_index_ths,
    fetch_stock_board_concept_summary_ths,
    fetch_stock_board_industry_index_ths,
    fetch_stock_industry_change_cninfo,
    fetch_stock_industry_clf_hist_sw,
    fetch_sw_index_first_info,
    fetch_sw_index_second_info,
    fetch_sw_index_third_info,
)


def _install_mock_akshare(monkeypatch, **fn_map):
    monkeypatch.setitem(sys.modules, "akshare", types.SimpleNamespace(**fn_map))


def _assert_result(result: SourceFetchResult, api_name: str, source_family: str) -> None:
    assert isinstance(result, SourceFetchResult)
    assert result.api_name == api_name
    assert result.source_family == source_family
    assert isinstance(result.raw, pd.DataFrame)
    assert result.metadata["api_name"] == api_name
    assert result.metadata["source_family"] == source_family
    assert "request_params" in result.metadata


def test_fetch_stock_industry_clf_hist_sw(monkeypatch) -> None:
    _install_mock_akshare(monkeypatch, stock_industry_clf_hist_sw=lambda: pd.DataFrame({"symbol": ["600000"], "start_date": ["2020-01-01"], "industry_code": ["801010"], "update_time": ["2024-01-01"]}))
    r = fetch_stock_industry_clf_hist_sw()
    _assert_result(r, "stock_industry_clf_hist_sw", "industry")


def test_index_component_sw_preserves_weight_and_date(monkeypatch) -> None:
    _install_mock_akshare(monkeypatch, index_component_sw=lambda symbol: pd.DataFrame({"证券代码": ["600000"], "证券名称": ["浦发银行"], "最新权重": [1.23], "计入日期": ["2024-01-01"]}))
    r = fetch_index_component_sw(symbol="801010")
    _assert_result(r, "index_component_sw", "industry")
    assert r.metadata["request_params"]["symbol"] == "801010"
    assert {"最新权重", "计入日期"}.issubset(r.raw.columns)


def test_index_hist_sw_preserves_date_and_code(monkeypatch) -> None:
    _install_mock_akshare(monkeypatch, index_hist_sw=lambda symbol, period: pd.DataFrame({"代码": [symbol], "日期": ["2024-01-01"], "收盘": [100]}))
    r = fetch_index_hist_sw(symbol="801010", period="day")
    _assert_result(r, "index_hist_sw", "industry")
    assert {"日期", "代码"}.issubset(r.raw.columns)


def test_stock_industry_change_cninfo_request_params(monkeypatch) -> None:
    _install_mock_akshare(monkeypatch, stock_industry_change_cninfo=lambda **kwargs: pd.DataFrame({"证券代码": ["600000"], "变更日期": ["2022-01-01"]}))
    r = fetch_stock_industry_change_cninfo(symbol="巨潮行业分类标准", start_date="20200101", end_date="20221231")
    _assert_result(r, "stock_industry_change_cninfo", "industry")
    assert r.metadata["request_params"]["symbol"] == "巨潮行业分类标准"
    assert r.metadata["request_params"]["start_date"] == "20200101"


def test_sw_index_info_family(monkeypatch) -> None:
    _install_mock_akshare(
        monkeypatch,
        sw_index_first_info=lambda: pd.DataFrame({"行业代码": ["801010"]}),
        sw_index_second_info=lambda: pd.DataFrame({"行业代码": ["801020"]}),
        sw_index_third_info=lambda: pd.DataFrame({"行业代码": ["801030"]}),
    )
    r1 = fetch_sw_index_first_info()
    r2 = fetch_sw_index_second_info()
    r3 = fetch_sw_index_third_info()
    _assert_result(r1, "sw_index_first_info", "industry_valuation")
    _assert_result(r2, "sw_index_second_info", "industry_valuation")
    _assert_result(r3, "sw_index_third_info", "industry_valuation")


def test_ths_index_and_concept_adapters(monkeypatch) -> None:
    _install_mock_akshare(
        monkeypatch,
        stock_board_industry_index_ths=lambda **kwargs: pd.DataFrame({"日期": ["2024-01-01"], "收盘价": [10.0]}),
        stock_board_concept_index_ths=lambda **kwargs: pd.DataFrame({"日期": ["2024-01-01"], "收盘价": [11.0]}),
    )
    r1 = fetch_stock_board_industry_index_ths(symbol="半导体", start_date="20200101", end_date="20221231")
    r2 = fetch_stock_board_concept_index_ths(symbol="芯片", start_date="20200101", end_date="20221231")
    _assert_result(r1, "stock_board_industry_index_ths", "ths_industry_theme")
    _assert_result(r2, "stock_board_concept_index_ths", "ths_concept_theme")


def test_concept_summary_preserves_columns(monkeypatch) -> None:
    _install_mock_akshare(monkeypatch, stock_board_concept_summary_ths=lambda: pd.DataFrame({"日期": ["2024-01-01"], "概念名称": ["芯片"], "驱动事件": ["事件"], "成分股数量": [50]}))
    r = fetch_stock_board_concept_summary_ths()
    _assert_result(r, "stock_board_concept_summary_ths", "theme_event")
    assert {"日期", "概念名称", "驱动事件", "成分股数量"}.issubset(r.raw.columns)


def test_sw_rescue_normalization_and_metadata(monkeypatch) -> None:
    import qsys.data.sources.akshare_industry as mod

    class _Resp:
        content = b"dummy"

        def raise_for_status(self):
            return None

    def _get(*args, **kwargs):
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", _get)
    monkeypatch.setattr(mod.pd, "read_excel", lambda _content: pd.DataFrame({"股票代码": [1], "计入日期": ["2026-01-01"], "行业代码": [1234], "更新日期": ["2026-01-02"]}))
    r = mod.fetch_sw_industry_membership_rescue()
    assert r.raw.loc[0, "stock_code"] == "000001"
    assert str(r.raw.loc[0, "effective_date"].date()) == "2026-01-01"
    assert r.raw.loc[0, "industry_l1_code"] == "00"
    assert r.raw.loc[0, "industry_l2_code"] == "0012"
    assert r.raw.loc[0, "industry_l3_code"] == "001234"
    assert "source_update_time" in r.metadata["metadata_only_fields"]


def test_sw_rescue_ssl_fallback_local_only(monkeypatch) -> None:
    import requests
    import qsys.data.sources.akshare_industry as mod

    calls = []

    class _Resp:
        content = b"dummy"

        def raise_for_status(self):
            return None

    def _get(*args, **kwargs):
        calls.append(kwargs.get("verify", None))
        if kwargs.get("verify", True):
            raise requests.exceptions.SSLError("ssl")
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", _get)
    monkeypatch.setattr(mod.pd, "read_excel", lambda _content: pd.DataFrame({"股票代码": [1], "计入日期": ["2026-01-01"], "行业代码": [123456], "更新日期": ["2026-01-02"]}))
    r = mod.fetch_sw_industry_membership_rescue()
    assert calls[:3] == [True, True, True]
    assert calls[-1] is False
    assert r.metadata["ssl_verify"] is False
    assert r.metadata["manual_review_required"] is True
    assert r.metadata["rescue_reason"]
    assert int(r.raw.duplicated(subset=["stock_code", "effective_date", "industry_code"]).sum()) == 0


def test_sw_rescue_headers_and_http_retry(monkeypatch) -> None:
    import requests
    import qsys.data.sources.akshare_industry as mod

    calls = []

    class _Resp:
        def __init__(self, status_code: int):
            self.status_code = status_code
            self.content = b"dummy"

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.exceptions.HTTPError("bad", response=self)

    seq = iter([_Resp(508), _Resp(200)])

    def _get(url, **kwargs):
        calls.append(kwargs)
        return next(seq)

    monkeypatch.setattr(mod.requests, "get", _get)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    monkeypatch.setattr(mod.pd, "read_excel", lambda _content: pd.DataFrame({"股票代码": [1], "计入日期": ["2026-01-01"], "行业代码": [123456], "更新日期": ["2026-01-02"]}))
    r = mod.fetch_sw_industry_membership_rescue()
    assert len(calls) == 2
    assert calls[0]["headers"]["Referer"] == "https://www.swsresearch.com/"
    assert "User-Agent" in calls[0]["headers"]
    assert r.metadata["source_mode"] == "remote_download"


def test_sw_rescue_local_file_fallback(monkeypatch, tmp_path) -> None:
    import qsys.data.sources.akshare_industry as mod

    xls = tmp_path / "sw.xls"
    pd.DataFrame({"股票代码": [1], "计入日期": ["2026-01-01"], "行业代码": [123456], "更新日期": ["2026-01-02"]}).to_excel(xls, index=False)

    def _boom(*args, **kwargs):
        raise RuntimeError("remote down")

    monkeypatch.setattr(mod.requests, "get", _boom)
    r = mod.fetch_sw_industry_membership_rescue(local_file=xls)
    assert r.metadata["source_mode"] == "local_file_fallback"
    assert r.metadata["manual_review_required"] is True
    assert "fallback" in r.metadata["rescue_reason"].lower()
    assert r.metadata["local_file_path"] == str(xls)
