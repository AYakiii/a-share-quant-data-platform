from __future__ import annotations

import json

import pandas as pd
import pytest

from qsys.data.sources.base import SourceFetchResult
from qsys.utils import build_margin_leverage_panel as mod


def _res(df: pd.DataFrame) -> SourceFetchResult:
    return SourceFetchResult(api_name="x", source_family="margin_leverage", raw=df, metadata={})


def test_build_margin_panel_symbols_dedupe_and_outputs(tmp_path, monkeypatch) -> None:
    sf = tmp_path / "symbols.txt"
    sf.write_text("sz000001\nsh600000\n", encoding="utf-8")

    raw = pd.DataFrame(
        {
            "证券代码": ["000001", "600000"],
            "融资余额": [10.0, 20.0],
            "融资买入额": [1.0, 2.0],
            "融资融券余额": [11.0, 22.0],
            "融券余额": [0.5, 0.7],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(raw))

    out = mod.build_margin_leverage_panel(
        symbols=["sz000001", "sh600000", "sz000001"],
        symbols_file=sf,
        start_date="2025-01-01",
        end_date="2025-01-01",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="r1",
        request_sleep=0,
    )

    assert out["panel_manifest"].exists()
    assert out["warnings"].exists()
    assert out["symbols"].exists()
    part = tmp_path / "panel" / "trade_date=2025-01-01" / "data.parquet"
    assert part.exists()
    df = pd.read_parquet(part)
    assert {"financing_balance", "financing_buy_amount", "margin_total_balance"}.issubset(df.columns)

    symbols = out["symbols"].read_text(encoding="utf-8").strip().splitlines()
    assert symbols == ["sz000001", "sh600000"]


def test_build_margin_panel_progress_output(tmp_path, monkeypatch, capsys) -> None:
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10.0],
            "融资买入额": [1.0],
            "融资融券余额": [11.0],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(raw))

    mod.build_margin_leverage_panel(
        symbols=["sz000001", "sh600000"],
        start_date="2025-01-01",
        end_date="2025-01-01",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="r2",
        request_sleep=0,
        show_progress=True,
        progress_every=1,
    )
    out = capsys.readouterr().out
    assert "[SSE 1/1] START 20250101" in out
    assert "[SSE 1/1] FAIL 20250101" in out
    assert "[SZSE 1/1] START 20250101" in out
    assert "[SZSE 1/1] OK 20250101" in out
    assert "rows_raw=" in out and "rows_selected=" in out and "total_elapsed=" in out
    assert out.index("[SZSE 1/1] START 20250101") < out.index("[SZSE 1/1] OK 20250101")
    assert "Done: fetched=1 failed=1" in out


def test_build_margin_panel_progress_every_respected(tmp_path, monkeypatch, capsys) -> None:
    raw = pd.DataFrame(
        {
            "证券代码": ["000001", "000002", "000003", "000004", "000005"],
            "融资余额": [10, 11, 12, 13, 14],
            "融资买入额": [1, 1, 1, 1, 1],
            "融资融券余额": [11, 12, 13, 14, 15],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(raw))

    mod.build_margin_leverage_panel(
        symbols=["sz000001", "sz000002", "sz000003", "sz000004", "sz000005"],
        start_date="2025-01-01",
        end_date="2025-01-01",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="r4",
        request_sleep=0,
        show_progress=True,
        progress_every=5,
    )
    out = capsys.readouterr().out
    assert "[SZSE 1/1] START 20250101" in out


def test_build_margin_panel_skips_weekends_by_default(tmp_path, monkeypatch) -> None:
    called_dates: list[str] = []
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10],
            "融资买入额": [1],
            "融资融券余额": [11],
        }
    )

    def fake_sse(date: str):
        called_dates.append(date)
        return _res(raw)

    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", fake_sse)
    mod.build_margin_leverage_panel(
        symbols=["sz000001"],
        start_date="2025-01-03",
        end_date="2025-01-06",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="wk1",
        request_sleep=0,
    )
    assert called_dates == ["20250103", "20250106"]


def test_build_margin_panel_include_calendar_days(tmp_path, monkeypatch) -> None:
    called_dates: list[str] = []
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10],
            "融资买入额": [1],
            "融资融券余额": [11],
        }
    )

    def fake_sse(date: str):
        called_dates.append(date)
        return _res(raw)

    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", fake_sse)
    mod.build_margin_leverage_panel(
        symbols=["sz000001"],
        start_date="2025-01-03",
        end_date="2025-01-06",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="wk2",
        request_sleep=0,
        include_calendar_days=True,
    )
    assert called_dates == ["20250103", "20250104", "20250105", "20250106"]


def test_build_margin_panel_aggregates_empty_warnings(tmp_path, monkeypatch) -> None:
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10],
            "融资买入额": [1],
            "融资融券余额": [11],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(raw))
    out = mod.build_margin_leverage_panel(
        symbols=["sh600000", "sz000001"],
        start_date="2025-01-01",
        end_date="2025-01-02",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="wk3",
        request_sleep=0,
    )
    warnings_text = out["warnings"].read_text(encoding="utf-8")
    assert "SSE empty responses skipped: 2 dates" in warnings_text


def test_build_margin_panel_no_progress_output_by_default(tmp_path, monkeypatch, capsys) -> None:
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10.0],
            "融资买入额": [1.0],
            "融资融券余额": [11.0],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(pd.DataFrame()))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(raw))

    mod.build_margin_leverage_panel(
        symbols=["sz000001"],
        start_date="2025-01-01",
        end_date="2025-01-01",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="r3",
        request_sleep=0,
    )
    out = capsys.readouterr().out
    assert out == ""


def test_build_margin_panel_no_symbols_fails() -> None:
    with pytest.raises(ValueError, match="No symbols provided"):
        mod.build_margin_leverage_panel(start_date="2025-01-01", end_date="2025-01-02")


def test_exchange_date_fetch_called_at_most_once_per_exchange_date(tmp_path, monkeypatch) -> None:
    calls = {"SSE": 0, "SZSE": 0}
    raw_sse = pd.DataFrame({"证券代码": ["600000", "600519"], "融资余额": [10, 20], "融资买入额": [1, 2], "融资融券余额": [11, 22]})
    raw_sz = pd.DataFrame({"证券代码": ["000001", "000002"], "融资余额": [10, 20], "融资买入额": [1, 2], "融资融券余额": [11, 22]})

    def fake_sse(date: str):
        calls["SSE"] += 1
        return _res(raw_sse)

    def fake_sz(date: str):
        calls["SZSE"] += 1
        return _res(raw_sz)

    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", fake_sse)
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", fake_sz)
    mod.build_margin_leverage_panel(
        symbols=["sh600000", "sh600519", "sz000001", "sz000002"],
        start_date="2025-01-01",
        end_date="2025-01-01",
        output_root=tmp_path / "panel",
        output_dir=tmp_path / "art",
        run_name="once",
        request_sleep=0,
    )
    assert calls["SSE"] == 1
    assert calls["SZSE"] == 1
