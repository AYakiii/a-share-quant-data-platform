from __future__ import annotations

import sys
import types

import pandas as pd

from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY, SOURCE_CAPABILITY_REGISTRY, filter_source_cases
from qsys.data.sources.akshare_market import fetch_stock_zh_a_daily, to_akshare_stock_symbol


def test_symbol_conversion_logic():
    assert to_akshare_stock_symbol("000001") == "sz000001"
    assert to_akshare_stock_symbol("600000") == "sh600000"
    assert to_akshare_stock_symbol("sz000001") == "sz000001"


def test_fetch_stock_zh_a_daily_preserves_raw_columns(monkeypatch):
    captured = {}

    def fake_daily(symbol: str, start_date: str, end_date: str, adjust: str = ""):
        captured["symbol"] = symbol
        return pd.DataFrame({"date": ["2024-01-02"], "open": [10], "turnover": [0.1]})

    fake_ak = types.SimpleNamespace(stock_zh_a_daily=fake_daily)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    res = fetch_stock_zh_a_daily(symbol="000001", start_date="20240101", end_date="20240331", adjust="")
    assert captured["symbol"] == "sz000001"
    assert list(res.raw.columns) == ["date", "open", "turnover"]


def test_registry_has_daily_fallback_and_probe_case():
    daily_specs = [x for x in SOURCE_CAPABILITY_REGISTRY if x.dataset_name == "daily_bar_raw"]
    apis = {x.api_name for x in daily_specs}
    assert "stock_zh_a_hist" in apis
    assert "stock_zh_a_daily" in apis

    cases = filter_source_cases(FACTOR_SOURCE_REGISTRY, api_name="stock_zh_a_daily")
    assert len(cases) == 1
    assert cases[0].kwargs["start_date"] == "20240101"
