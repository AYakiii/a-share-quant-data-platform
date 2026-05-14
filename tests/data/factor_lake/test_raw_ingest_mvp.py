from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.local_api import read_partition_metadata, read_raw_partition
from qsys.data.factor_lake.raw_ingest import run_raw_ingest_mvp


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_dataset_centered_raw_ingest_mvp_writes_outputs(tmp_path):
    adapters = {
        "stock_zh_a_daily": lambda **kwargs: _Result(pd.DataFrame({"date": ["2024-01-02"], "close": [10.0]})),
        "stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"日期": ["2024-01-02"], "收盘": [10.0]})),
        "stock_zh_index_hist_csindex": lambda **kwargs: _Result(pd.DataFrame({"日期": ["2024-01-02"], "收盘": [3000.0]})),
        "stock_margin_detail_sse": lambda **kwargs: _Result(pd.DataFrame({"trade_date": ["20240329"], "v": [1]})),
        "stock_margin_detail_szse": lambda **kwargs: _Result(pd.DataFrame({"trade_date": ["20240329"], "v": [2]})),
    }

    out = run_raw_ingest_mvp(
        datasets=["daily_bar_raw", "index_bar_raw", "margin_detail_raw"],
        root=str(tmp_path),
        metastore_path=str(tmp_path / "meta.sqlite"),
        symbols=["000001"],
        index_symbols=["000300"],
        trade_dates=["20240329"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=True,
    )

    assert (tmp_path / "raw_ingest_catalog.csv").exists()
    assert (tmp_path / "raw_ingest_summary.csv").exists()

    df = read_raw_partition(tmp_path, "daily_bar_raw", "stock_zh_a_daily", {"symbol": "000001", "start_date": "20240101", "end_date": "20240331"})
    assert "close" in df.columns
    meta = read_partition_metadata(tmp_path, "daily_bar_raw", "stock_zh_a_daily", {"symbol": "000001", "start_date": "20240101", "end_date": "20240331"})
    assert meta["dataset"] == "daily_bar_raw"
    assert not (tmp_path / "outputs" / "factor_lake_raw_ingest_mvp").exists()


def test_continue_on_error_false_stops_early(tmp_path):
    def bad(**kwargs):
        raise ValueError("boom")

    adapters = {
        "stock_zh_a_daily": bad,
        "stock_zh_a_hist": bad,
        "stock_zh_index_hist_csindex": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
        "stock_margin_detail_sse": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
        "stock_margin_detail_szse": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
    }
    out = run_raw_ingest_mvp(
        datasets=["daily_bar_raw"],
        root=str(tmp_path),
        metastore_path=str(tmp_path / "meta.sqlite"),
        symbols=["000001", "600000"],
        index_symbols=["000300"],
        trade_dates=["20240329"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=False,
    )
    records = out["results"][0]["records"]
    assert len(records) == 1
    assert records[0]["status"] == "failed"
