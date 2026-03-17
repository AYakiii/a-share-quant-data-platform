from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from qsys.data.panel.daily_panel import load_daily_panel


def _write_partition(root: Path, trade_date: str, rows: list[dict]) -> None:
    part_dir = root / f"trade_date={trade_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(part_dir / "data.parquet", index=False)


def test_load_daily_panel_contract(tmp_path: Path) -> None:
    root = tmp_path / "standardized/market/daily_bars"

    _write_partition(
        root,
        "2024-01-02",
        [
            {
                "trade_date": "2024-01-02",
                "ts_code": "000001.SZ",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "vol": 1000,
            },
            {
                "trade_date": "2024-01-02",
                "ts_code": "000002.SZ",
                "open": 8.0,
                "high": 8.5,
                "low": 7.9,
                "close": 8.1,
                "vol": 2000,
            },
        ],
    )

    panel = load_daily_panel(
        dataset_root=root,
        start_date="2024-01-02",
        end_date="2024-01-02",
        symbols=["000001.SZ"],
        columns=["open", "close", "volume", "is_tradable"],
    )

    assert panel.index.names == ["date", "asset"]
    assert list(panel.columns) == ["open", "close", "volume", "is_tradable"]
    assert len(panel) == 1
    idx = panel.index[0]
    assert str(idx[0].date()) == "2024-01-02"
    assert idx[1] == "000001.SZ"
    assert panel.iloc[0]["volume"] == 1000
    assert bool(panel.iloc[0]["is_tradable"]) is True
