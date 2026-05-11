from __future__ import annotations

import sys
import types

import pandas as pd

from qsys.data.sources import (
    SourceFetchResult,
    fetch_stock_dzjy_hyyybtj,
    fetch_stock_dzjy_mrmx,
    fetch_stock_dzjy_mrtj,
    fetch_stock_dzjy_sctj,
    fetch_stock_jgdy_tj_em,
    fetch_stock_lhb_detail_em,
    fetch_stock_lhb_hyyyb_em,
    fetch_stock_lhb_jgmmtj_em,
    fetch_stock_lhb_stock_statistic_em,
    fetch_stock_lhb_yybph_em,
    fetch_stock_yjyg_em,
)


def _install(monkeypatch, **fns):
    monkeypatch.setitem(sys.modules, "akshare", types.SimpleNamespace(**fns))


def _assert(r: SourceFetchResult, api: str, fam: str):
    assert isinstance(r, SourceFetchResult)
    assert r.metadata["api_name"] == api
    assert r.metadata["source_family"] == fam


def test_block_trade_adapters(monkeypatch):
    _install(
        monkeypatch,
        stock_dzjy_sctj=lambda: pd.DataFrame({"交易日期": ["2024-01-01"]}),
        stock_dzjy_mrmx=lambda **kwargs: pd.DataFrame({"交易日期": ["2024-01-02"]}),
        stock_dzjy_mrtj=lambda **kwargs: pd.DataFrame({"交易日期": ["2024-01-02"]}),
        stock_dzjy_hyyybtj=lambda: pd.DataFrame({"行业": ["银行"]}),
    )
    _assert(fetch_stock_dzjy_sctj(), "stock_dzjy_sctj", "block_trade")
    r1 = fetch_stock_dzjy_mrmx(date="20240102")
    _assert(r1, "stock_dzjy_mrmx", "block_trade")
    assert r1.metadata["request_params"]["date"] == "20240102"
    r2 = fetch_stock_dzjy_mrtj(date="20240102")
    _assert(r2, "stock_dzjy_mrtj", "block_trade")
    _assert(fetch_stock_dzjy_hyyybtj(), "stock_dzjy_hyyybtj", "block_trade")


def test_lhb_and_event_attention_adapters(monkeypatch):
    _install(
        monkeypatch,
        stock_lhb_detail_em=lambda **kwargs: pd.DataFrame({"上榜日": ["2024-01-03"], "上榜后1日": [0.1], "上榜后2日": [0.2], "上榜后5日": [0.3], "上榜后10日": [0.4]}),
        stock_lhb_stock_statistic_em=lambda **kwargs: pd.DataFrame({"代码": ["000001"]}),
        stock_lhb_jgmmtj_em=lambda **kwargs: pd.DataFrame({"代码": ["000001"]}),
        stock_lhb_hyyyb_em=lambda **kwargs: pd.DataFrame({"营业部": ["xx"]}),
        stock_lhb_yybph_em=lambda **kwargs: pd.DataFrame({"营业部": ["yy"]}),
        stock_jgdy_tj_em=lambda: pd.DataFrame({"接待日期": ["2024-01-04"], "公告日期": ["2024-01-05"]}),
        stock_yjyg_em=lambda date: pd.DataFrame({"公告日期": ["2024-01-06"], "日期参数": [date]}),
    )

    d = fetch_stock_lhb_detail_em(start_date="20240101", end_date="20240131")
    _assert(d, "stock_lhb_detail_em", "trading_attention")
    assert {"上榜日", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"}.issubset(d.raw.columns)

    _assert(fetch_stock_lhb_stock_statistic_em(), "stock_lhb_stock_statistic_em", "trading_attention")
    _assert(fetch_stock_lhb_jgmmtj_em(), "stock_lhb_jgmmtj_em", "trading_attention")
    _assert(fetch_stock_lhb_hyyyb_em(), "stock_lhb_hyyyb_em", "trading_attention")
    _assert(fetch_stock_lhb_yybph_em(), "stock_lhb_yybph_em", "trading_attention")

    j = fetch_stock_jgdy_tj_em()
    _assert(j, "stock_jgdy_tj_em", "institution_attention")
    assert {"接待日期", "公告日期"}.issubset(j.raw.columns)

    y = fetch_stock_yjyg_em(date="202401")
    _assert(y, "stock_yjyg_em", "fundamental_event")
    assert y.metadata["request_params"]["date"] == "202401"
    assert "公告日期" in y.raw.columns
