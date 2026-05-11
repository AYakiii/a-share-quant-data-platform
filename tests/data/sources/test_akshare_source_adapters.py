from __future__ import annotations

import json
import sys
import types

import pandas as pd

from qsys.data.sources import (
    SourceFetchResult,
    fetch_stock_margin_detail_sse,
    fetch_stock_margin_detail_szse,
    fetch_stock_yysj_em,
    fetch_stock_zh_a_hist,
    fetch_stock_zh_index_hist_csindex,
    write_source_fetch_result,
)


def _install_mock_akshare(monkeypatch, **fn_map):
    mock_ak = types.SimpleNamespace(**fn_map)
    monkeypatch.setitem(sys.modules, "akshare", mock_ak)


def _assert_common_metadata(result: SourceFetchResult, api_name: str, source_family: str) -> None:
    assert result.api_name == api_name
    assert result.source_family == source_family
    assert isinstance(result.raw, pd.DataFrame)
    assert result.metadata["api_name"] == api_name
    assert result.metadata["source_family"] == source_family
    assert "request_params" in result.metadata
    assert result.metadata["row_count"] == len(result.raw)
    assert result.metadata["column_count"] == result.raw.shape[1]
    assert result.metadata["raw_columns"] == [str(c) for c in result.raw.columns]


def test_fetch_stock_zh_a_hist_returns_result(monkeypatch) -> None:
    def fake(**kwargs):
        return pd.DataFrame({"日期": ["2025-01-02"], "收盘": [10.0]})

    _install_mock_akshare(monkeypatch, stock_zh_a_hist=fake)
    result = fetch_stock_zh_a_hist("000001", "20250101", "20250131")

    _assert_common_metadata(result, "stock_zh_a_hist", "market_price_volume")
    assert result.metadata["request_params"]["symbol"] == "000001"


def test_fetch_stock_zh_index_hist_csindex_returns_result(monkeypatch) -> None:
    def fake(**kwargs):
        return pd.DataFrame({"日期": ["2025-01-02"], "收盘": [5000.0]})

    _install_mock_akshare(monkeypatch, stock_zh_index_hist_csindex=fake)
    result = fetch_stock_zh_index_hist_csindex("000905", "20250101", "20250131")

    _assert_common_metadata(result, "stock_zh_index_hist_csindex", "market_index_regime")


def test_fetch_stock_yysj_em_returns_result(monkeypatch) -> None:
    def fake(**kwargs):
        return pd.DataFrame({"股票代码": ["000001"], "实际披露时间": ["2025-03-30"]})

    _install_mock_akshare(monkeypatch, stock_yysj_em=fake)
    result = fetch_stock_yysj_em(symbol="沪深A股", date="202503")

    _assert_common_metadata(result, "stock_yysj_em", "fundamental_disclosure")


def test_fetch_stock_margin_detail_sse_preserves_raw_columns(monkeypatch) -> None:
    def fake(**kwargs):
        return pd.DataFrame({"信用交易日期": ["2025-01-02"], "证券代码": ["600000"]})

    _install_mock_akshare(monkeypatch, stock_margin_detail_sse=fake)
    result = fetch_stock_margin_detail_sse(date="20250102")

    _assert_common_metadata(result, "stock_margin_detail_sse", "margin_leverage")
    assert "信用交易日期" in result.raw.columns


def test_fetch_stock_margin_detail_szse_injects_trade_date_when_missing(monkeypatch) -> None:
    def fake(**kwargs):
        return pd.DataFrame({"证券代码": ["000001"], "融资余额": [100.0]})

    _install_mock_akshare(monkeypatch, stock_margin_detail_szse=fake)
    result = fetch_stock_margin_detail_szse(date="20250102")

    _assert_common_metadata(result, "stock_margin_detail_szse", "margin_leverage")
    assert "trade_date" in result.raw.columns
    assert set(result.raw["trade_date"]) == {"20250102"}
    assert "trade_date" in result.metadata["normalized_columns"]


def test_write_source_fetch_result_writes_data_and_metadata(tmp_path) -> None:
    result = SourceFetchResult(
        api_name="stock_zh_a_hist",
        source_family="market_price_volume",
        raw=pd.DataFrame({"a": [1], "b": [2]}),
        metadata={
            "api_name": "stock_zh_a_hist",
            "source_family": "market_price_volume",
            "request_params": {"symbol": "000001"},
            "fetched_at_utc": "2026-01-01T00:00:00+00:00",
            "row_count": 1,
            "column_count": 2,
            "raw_columns": ["a", "b"],
            "normalized_columns": [],
            "source_inventory_version": "akshare_free_factor_source_inventory_v0",
        },
    )

    out = write_source_fetch_result(
        result,
        output_root=tmp_path,
        dataset_name="stock_zh_a_hist",
        partition_values={"date": "2025-01-02", "symbol": "000001"},
    )
    assert out["data"].exists()
    assert out["metadata"].exists()

    meta = json.loads(out["metadata"].read_text(encoding="utf-8"))
    assert list(meta.keys()) == sorted(meta.keys())
    assert meta["api_name"] == "stock_zh_a_hist"
