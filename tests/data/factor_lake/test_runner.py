from __future__ import annotations

import time

import pandas as pd

from qsys.data.factor_lake.runner import run_probe
from qsys.data.factor_lake.schemas import SourceCase


class FakeAk:
    def ok(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame({"日期": ["2024-01-01"], "代码": [symbol]})

    def empty(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame(columns=["日期", "代码"])

    def boom(self, symbol: str) -> pd.DataFrame:
        raise ValueError("boom")

    def slow(self, symbol: str) -> pd.DataFrame:
        time.sleep(0.2)
        return pd.DataFrame({"x": [1]})


def test_runner_statuses_and_catalog_count(monkeypatch, tmp_path):
    cases = [
        SourceCase("ok_case", "market_price", "ok", {"symbol": "000001", "extra": 1}, "ok"),
        SourceCase("empty_case", "market_price", "empty", {"symbol": "000001"}, "empty"),
        SourceCase("missing_case", "market_price", "missing_fn", {"symbol": "000001"}, "missing"),
        SourceCase("failed_case", "market_price", "boom", {"symbol": "000001"}, "failed"),
        SourceCase("timeout_case", "market_price", "slow", {"symbol": "000001"}, "timeout"),
    ]
    monkeypatch.setattr("qsys.data.factor_lake.runner.FACTOR_SOURCE_REGISTRY", cases)

    manifest = run_probe(FakeAk(), output_root=tmp_path, timeout_seconds=0.05)
    cat = pd.read_csv(tmp_path / "catalogs" / "api_call_catalog.csv")
    assert manifest["selected_cases"] == len(cat) == len(cases)
    assert set(cat["status"]) == {"success", "empty", "missing", "failed", "timeout"}
    ok = cat.loc[cat["case_id"] == "ok_case"].iloc[0]
    assert "extra" in ok["ignored_kwargs_json"]


def test_fallback_csv_when_parquet_write_fails(monkeypatch, tmp_path):
    class AkMixed:
        def mixed(self, symbol: str) -> pd.DataFrame:
            return pd.DataFrame({"item": ["证券代码", "证券简称"], "value": ["000001", 1.23]})

    def raise_arrow(*args, **kwargs):
        raise ValueError("ArrowInvalid")

    cases = [SourceCase("mixed_case", "market_price", "mixed", {"symbol": "000001"}, "mixed")]
    monkeypatch.setattr("qsys.data.factor_lake.runner.FACTOR_SOURCE_REGISTRY", cases)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", raise_arrow)
    run_probe(AkMixed(), output_root=tmp_path)
    cat = pd.read_csv(tmp_path / "catalogs" / "api_call_catalog.csv")
    row = cat.iloc[0]
    assert row["status"] == "success"
    assert row["output_format"] == "csv"
    assert "parquet_write_failed" in row["write_warning"]


def test_non_dataframe_and_filters(monkeypatch, tmp_path):
    class Ak2:
        def not_df(self, symbol: str):
            return {"x": 1}

    cases = [
        SourceCase("ndf", "industry_concept", "not_df", {"symbol": "x"}, "ndf"),
        SourceCase("other", "market_price", "not_df", {"symbol": "x"}, "ndf"),
    ]
    monkeypatch.setattr("qsys.data.factor_lake.runner.FACTOR_SOURCE_REGISTRY", cases)
    run_probe(Ak2(), output_root=tmp_path, family="industry_concept", max_cases=1)
    cat = pd.read_csv(tmp_path / "catalogs" / "api_call_catalog.csv")
    assert len(cat) == 1
    assert cat.iloc[0]["status"] == "non_dataframe"
