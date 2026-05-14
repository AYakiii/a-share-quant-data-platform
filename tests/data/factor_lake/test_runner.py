from __future__ import annotations

import sqlite3

import pandas as pd

from qsys.data.factor_lake.local_api import read_partition_metadata, read_raw_partition
from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.raw_ingest import run_raw_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def _ok_stock(symbol: str, start_date: str, end_date: str):
    return _Result(pd.DataFrame({"symbol": [symbol], "start_date": [start_date], "end_date": [end_date], "close": [10.0]}))


def _ok_index(symbol: str, start_date: str, end_date: str):
    return _Result(pd.DataFrame({"index": [symbol], "trade_date": [start_date], "close": [3000.0]}))


def _margin_sse(date: str):
    return _Result(pd.DataFrame({"trade_date": [date], "exchange": ["sse"], "value": [1]}))


def _margin_szse(date: str):
    return _Result(pd.DataFrame({"trade_date": [date], "exchange": ["szse"], "value": [2]}))


def test_raw_ingest_and_local_api_with_synthetic_adapters(tmp_path):
    ms = FactorLakeMetastore(tmp_path / "meta.sqlite")
    adapters = {
        "stock_zh_a_hist": _ok_stock,
        "stock_zh_index_hist_csindex": _ok_index,
        "stock_margin_detail_sse": _margin_sse,
        "stock_margin_detail_szse": _margin_szse,
    }

    r1 = run_raw_ingest("daily_bar_raw", str(tmp_path), ms, adapter_map=adapters, symbol="000001", year="2024")
    assert r1["status"] == "success"

    run_raw_ingest("index_bar_raw", str(tmp_path), ms, adapter_map=adapters, index_symbol="000300", year="2024")
    run_raw_ingest("margin_detail_raw", str(tmp_path), ms, adapter_map=adapters, exchanges=["sse", "szse"], trade_date="2024-03-29")

    ddf = read_raw_partition(tmp_path, "daily_bar_raw", "stock_zh_a_hist", {"symbol": "000001", "year": "2024"})
    assert list(ddf.columns) == ["symbol", "start_date", "end_date", "close"]

    meta = read_partition_metadata(tmp_path, "daily_bar_raw", "stock_zh_a_hist", {"symbol": "000001", "year": "2024"})
    assert meta["dataset"] == "daily_bar_raw"

    with sqlite3.connect(tmp_path / "meta.sqlite") as conn:
        inv = conn.execute("select count(*) from raw_dataset_inventory").fetchone()[0]
        logs = conn.execute("select count(*) from ingest_run_log").fetchone()[0]
    assert inv >= 4
    assert logs >= 4
