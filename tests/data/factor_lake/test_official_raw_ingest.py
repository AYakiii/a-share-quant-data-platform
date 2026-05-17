from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_official


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_official_catalog_contract(tmp_path):
    out = run_raw_ingest_official(
        output_root=str(tmp_path),
        families=["market_price"],
        symbols=["000001"],
        trade_dates=["20100104"],
        start_date="20100101",
        end_date="20100131",
        adapter_map={
            "stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]}))
        },
        include_disabled=True,
    )
    df = pd.read_csv(out["catalog_path"])
    required = {"run_id","dataset_name","source_family","api_name","partition_json","params_json","status","rows","error_type","error_message","output_path","metadata_path","started_at","finished_at","elapsed_sec"}
    assert required.issubset(df.columns)


def _write_universe(root, *, stock=True, index=False, industry=False, concept=False, calendar=True):
    root.mkdir(parents=True, exist_ok=True)
    if stock:
        (root / "stock_symbols.csv").write_text("symbol\n000001\n", encoding="utf-8")
    if index:
        (root / "index_symbols.csv").write_text("index_symbol\n000300\n", encoding="utf-8")
    if industry:
        (root / "industry_names.csv").write_text("industry_name\n半导体\n", encoding="utf-8")
    if concept:
        (root / "concept_names.csv").write_text("concept_name\nAI PC\n", encoding="utf-8")
    if calendar:
        (root / "trading_calendar.csv").write_text("trade_date\n20100104\n20100105\n", encoding="utf-8")


def test_market_price_does_not_require_index_industry_concept_universes(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, index=False, industry=False, concept=False, calendar=True)
    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]}))},
    )
    assert (tmp_path / "out" / "raw_ingest_catalog.csv").exists()
    assert out["rows"]


def test_index_market_requires_index_universe(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=False, index=False, industry=False, concept=False, calendar=False)
    try:
        run_raw_ingest_official(
            output_root=str(tmp_path / "out"),
            families=["index_market"],
            start_date="20100101",
            end_date="20100131",
            universe_root=uroot,
            include_disabled=True,
            adapter_map={"stock_zh_index_hist_csindex": lambda **kwargs: _Result(pd.DataFrame({"x": [1]}))},
        )
        assert False, "expected missing index_symbols.csv to fail"
    except FileNotFoundError as exc:
        assert "index_symbols.csv" in str(exc)
