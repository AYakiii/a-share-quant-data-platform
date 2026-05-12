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
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(raw))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(pd.DataFrame()))

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
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(raw))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(pd.DataFrame()))

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
    assert "[1/2] START sz000001" in out
    assert "[1/2] OK sz000001" in out
    assert out.index("[1/2] START sz000001") < out.index("[1/2] OK sz000001")
    assert "[2/2] START sh600000" in out
    assert "[2/2] FAIL sh600000" in out
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
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(raw))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(pd.DataFrame()))

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
    assert "[1/5] START sz000001" in out
    assert "[5/5] START sz000005" in out
    assert "[2/5] START" not in out


def test_build_margin_panel_no_progress_output_by_default(tmp_path, monkeypatch, capsys) -> None:
    raw = pd.DataFrame(
        {
            "证券代码": ["000001"],
            "融资余额": [10.0],
            "融资买入额": [1.0],
            "融资融券余额": [11.0],
        }
    )
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_sse", lambda date: _res(raw))
    monkeypatch.setattr(mod, "fetch_stock_margin_detail_szse", lambda date: _res(pd.DataFrame()))

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
