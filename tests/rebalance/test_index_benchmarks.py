from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance.index_benchmarks import build_index_return_curve, normalize_index_price_frame


def test_normalize_index_price_frame_chinese() -> None:
    df = pd.DataFrame({"日期": ["2024-01-01", "2024-01-02"], "收盘": [100, 102]})
    out = normalize_index_price_frame(df)
    assert list(out.columns) == ["date", "close"]
    assert len(out) == 2


def test_normalize_index_price_frame_english() -> None:
    df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "close": [100, 101]})
    out = normalize_index_price_frame(df)
    assert out["close"].iloc[1] == 101


def test_build_index_return_curve_synthetic() -> None:
    df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02", "2024-01-03"], "close": [100, 110, 121]})
    out = build_index_return_curve(df, policy="CSI300")
    assert out["gross_return"].iloc[0] == pytest.approx(0.0)
    assert out["gross_return"].iloc[1] == pytest.approx(0.10)
    assert out["cumulative_net_return"].iloc[-1] == pytest.approx(0.21)
