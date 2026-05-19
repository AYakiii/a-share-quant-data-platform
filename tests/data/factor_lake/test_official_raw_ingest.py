from __future__ import annotations

import pandas as pd
import json

from qsys.data.factor_lake.raw_ingest import _to_akshare_daily_symbol, run_raw_ingest_official


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


def test_market_price_primary_hist_success(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)
    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]}))},
    )
    df = pd.read_csv(out["catalog_path"])
    hist_df = df[df["api_name"] == "stock_zh_a_hist"]
    assert not hist_df.empty
    assert hist_df["output_path"].str.contains("year=").all()
    assert hist_df["output_path"].str.contains("month=").all()
    assert not hist_df["output_path"].str.contains("start_date=").any()
    assert not hist_df["output_path"].str.contains("end_date=").any()


def test_market_price_primary_fail_fallback_daily_success(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def bad_hist(**kwargs):
        raise ConnectionError("RemoteDisconnected")

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={
            "stock_zh_a_hist": bad_hist,
            "stock_zh_a_daily": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
        },
    )
    df = pd.read_csv(out["catalog_path"])
    daily_df = df[df["api_name"] == "stock_zh_a_daily"]
    assert not daily_df.empty
    assert "success" in set(daily_df["status"])


def test_market_price_primary_and_fallback_fail(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def bad(**kwargs):
        raise ConnectionError("RemoteDisconnected")

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad, "stock_zh_a_daily": bad},
    )
    df = pd.read_csv(out["catalog_path"])
    assert "failed" in set(df["status"])
    assert "primary_error=" in str(df["error_message"].iloc[0])
    assert "fallback_error=" in str(df["error_message"].iloc[0])


def test_daily_symbol_mapping_rules():
    assert _to_akshare_daily_symbol("000001") == "sz000001"
    assert _to_akshare_daily_symbol("300001") == "sz300001"
    assert _to_akshare_daily_symbol("600000") == "sh600000"
    assert _to_akshare_daily_symbol("688001") == "sh688001"
    assert _to_akshare_daily_symbol("920000") == "bj920000"
    assert _to_akshare_daily_symbol(1) == "sz000001"
    assert _to_akshare_daily_symbol("sz000001") == "sz000001"


def test_market_price_primary_success_no_fallback_call(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)
    called = {"daily": False}

    def daily(**kwargs):
        called["daily"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})), "stock_zh_a_daily": daily},
    )
    df = pd.read_csv(out["catalog_path"])
    assert called["daily"] is False
    assert (df["fallback_from"].fillna("") == "").all()


def test_market_price_fallback_daily_params_and_filter_and_catalog(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)
    got_kwargs = {}

    def bad_hist(**kwargs):
        raise KeyError("date")

    def daily(**kwargs):
        got_kwargs.update(kwargs)
        return _Result(pd.DataFrame({"date": ["2009-12-31", "2010-01-04", "2010-02-01"], "x": [1, 2, 3]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000001"],
        start_date="20100101",
        end_date="20100131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad_hist, "stock_zh_a_daily": daily},
    )
    assert got_kwargs == {"symbol": "sz000001", "adjust": ""}
    df = pd.read_csv(out["catalog_path"])
    row = df.iloc[0]
    assert row["requested_api_name"] == "stock_zh_a_hist"
    assert row["actual_api_name"] == "stock_zh_a_daily"
    assert row["fallback_from"] == "stock_zh_a_hist"
    assert str(row["original_symbol"]).zfill(6) == "000001"
    assert row["akshare_symbol"] == "sz000001"
    assert row["rows"] == 1
    assert "month=" in row["output_path"]


def test_market_price_fallback_daily_writes_year_month_partition_and_metadata(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def bad_hist(**kwargs):
        raise KeyError("date")

    def daily(**kwargs):
        return _Result(pd.DataFrame({"date": ["2025-01-02", "2025-01-31"], "x": [1, 2]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000001"],
        start_date="20250101",
        end_date="20251231",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad_hist, "stock_zh_a_daily": daily},
    )
    df = pd.read_csv(out["catalog_path"])
    daily_df = df[df["api_name"] == "stock_zh_a_daily"]
    assert len(daily_df) == 1
    row = daily_df.iloc[0]
    assert int(row["year"]) == 2025
    assert int(row["month"]) == 1
    assert "year=2025" in row["output_path"]
    assert "month=01" in row["output_path"]
    assert "adjust=none" in row["output_path"]
    assert "pool_id=" not in row["output_path"]
    assert "batch_id=" not in row["output_path"]
    with open(row["metadata_path"], encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["requested_api_name"] == "stock_zh_a_hist"
    assert meta["actual_api_name"] == "stock_zh_a_daily"
    assert meta["fallback_from"] == "stock_zh_a_hist"
    assert meta["original_symbol"] == "000001"
    assert meta["akshare_symbol"] == "sz000001"
    assert str(meta["year"]) == "2025"
    assert str(meta["month"]) == "01"
    assert meta["min_date"] == "2025-01-02"
    assert meta["max_date"] == "2025-01-31"


def test_market_price_fallback_daily_spanning_two_months_writes_two_rows(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def bad_hist(**kwargs):
        raise KeyError("date")

    def daily(**kwargs):
        return _Result(pd.DataFrame({"date": ["2025-01-31", "2025-02-03"], "x": [1, 2]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000001"],
        start_date="20250101",
        end_date="20250210",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad_hist, "stock_zh_a_daily": daily},
    )
    df = pd.read_csv(out["catalog_path"])
    daily_df = df[df["api_name"] == "stock_zh_a_daily"]
    assert {int(y) for y in daily_df["year"].dropna().tolist()} == {2025}
    assert {int(m) for m in daily_df["month"].dropna().tolist()} == {1, 2}
    assert len(daily_df) == 2
    assert daily_df["output_path"].str.contains("year=2025/month=01").any()
    assert daily_df["output_path"].str.contains("year=2025/month=02").any()


def test_market_price_fallback_daily_spanning_two_years_writes_multi_month_partitions(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def bad_hist(**kwargs):
        raise KeyError("date")

    def daily(**kwargs):
        return _Result(pd.DataFrame({"date": ["2025-12-31", "2026-01-02", "2026-01-10"], "x": [1, 2, 3]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000001"],
        start_date="20251201",
        end_date="20260110",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad_hist, "stock_zh_a_daily": daily},
    )
    df = pd.read_csv(out["catalog_path"])
    daily_df = df[df["api_name"] == "stock_zh_a_daily"]
    keys = {(int(y), int(m)) for y, m in zip(daily_df["year"], daily_df["month"])}
    assert keys == {(2025, 12), (2026, 1)}
    assert len(daily_df) == 2


def test_market_price_hist_primary_spanning_months_writes_month_rows(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def hist(**kwargs):
        return _Result(pd.DataFrame({"date": ["2024-01-31", "2024-02-01", "2024-02-28"], "x": [1, 2, 3]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000009"],
        start_date="20240101",
        end_date="20240228",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": hist},
    )
    df = pd.read_csv(out["catalog_path"])
    hist_df = df[df["api_name"] == "stock_zh_a_hist"]
    assert len(hist_df) == 2
    assert {int(m) for m in hist_df["month"]} == {1, 2}
    assert hist_df["output_path"].str.contains("symbol=000009").any()
    assert hist_df["output_path"].str.contains("adjust=qfq").all()
    assert (~hist_df["output_path"].str.contains("pool_id=")).all()
    assert (~hist_df["output_path"].str.contains("batch_id=")).all()
    row = hist_df.iloc[0]
    with open(row["metadata_path"], encoding="utf-8") as f:
        meta = json.load(f)
    assert "year" in meta and "month" in meta
    assert "min_date" in meta and "max_date" in meta
    assert meta["original_symbol"] == "000009"


def test_market_price_existing_partition_not_overwritten(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    target = tmp_path / "out" / "data" / "raw" / "akshare" / "market_price" / "stock_zh_a_daily" / "symbol=sz000001" / "adjust=none" / "year=2025" / "month=01"
    target.mkdir(parents=True, exist_ok=True)
    (target / "data.parquet").write_bytes(b"dummy")

    def bad_hist(**kwargs):
        raise KeyError("date")

    def daily(**kwargs):
        return _Result(pd.DataFrame({"date": ["2025-01-02"], "x": [1]}))

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000001"],
        start_date="20250101",
        end_date="20250131",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": bad_hist, "stock_zh_a_daily": daily},
    )
    df = pd.read_csv(out["catalog_path"])
    assert "already_exists" in set(df["status"])


def test_market_price_hist_primary_chinese_date_column_partition_contract(tmp_path):
    uroot = tmp_path / "u"
    _write_universe(uroot, stock=True, calendar=True)

    def hist(**kwargs):
        return _Result(
            pd.DataFrame(
                {
                    "日期": ["2022-01-04", "2022-02-07", "2023-01-03", "2023-01-30"],
                    "开盘": [10.1, 10.2, 10.3, 10.4],
                    "收盘": [10.5, 10.6, 10.7, 10.8],
                }
            )
        )

    out = run_raw_ingest_official(
        output_root=str(tmp_path / "out"),
        families=["market_price"],
        symbols=["000009"],
        start_date="20220101",
        end_date="20231231",
        universe_root=uroot,
        include_disabled=True,
        adapter_map={"stock_zh_a_hist": hist},
    )

    catalog = pd.read_csv(out["catalog_path"])
    hist_df = catalog[catalog["api_name"] == "stock_zh_a_hist"].copy()
    assert len(hist_df) == 3
    assert not hist_df["output_path"].str.contains("start_date=").any()
    assert not hist_df["output_path"].str.contains("end_date=").any()

    expected = {(2022, 1): 1, (2022, 2): 1, (2023, 1): 2}
    actual_keys = {(int(y), int(m)) for y, m in zip(hist_df["year"], hist_df["month"])}
    assert actual_keys == set(expected.keys())

    for _, row in hist_df.iterrows():
        year = int(row["year"])
        month = int(row["month"])
        part_df = pd.read_parquet(row["output_path"])
        part_dates = pd.to_datetime(part_df["日期"], errors="coerce")
        assert part_dates.notna().all()
        assert set(part_dates.dt.year.unique().tolist()) == {year}
        assert set(part_dates.dt.month.unique().tolist()) == {month}

        assert int(row["rows"]) == expected[(year, month)]
        assert int(row["rows"]) == len(part_df)
        assert str(row["min_date"]) == str(part_dates.min().date())
        assert str(row["max_date"]) == str(part_dates.max().date())

        with open(row["metadata_path"], encoding="utf-8") as f:
            meta = json.load(f)
        assert int(meta["year"]) == year
        assert int(meta["month"]) == month
        assert int(meta["rows"]) == len(part_df)
        assert meta["min_date"] == str(part_dates.min().date())
        assert meta["max_date"] == str(part_dates.max().date())
