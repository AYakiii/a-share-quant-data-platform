from __future__ import annotations

import pandas as pd

from qsys.utils.build_real_feature_store import REQUIRED_COLUMNS, _normalize_daily_frame, build_real_feature_store


def test_build_real_feature_store_writes_partitions(tmp_path, monkeypatch) -> None:
    sample = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]),
            "open": [10, 11, 12, 13, 14, 15],
            "high": [11, 12, 13, 14, 15, 16],
            "low": [9, 10, 11, 12, 13, 14],
            "close": [10, 11, 12, 13, 14, 15],
            "volume": [100, 120, 90, 110, 130, 140],
            "amount": [1000, 1100, 900, 1200, 1300, 1400],
            "turnover": [1.0, 1.1, 0.9, 1.2, 1.3, 1.4],
            "outstanding_share": [1_000_000] * 6,
        }
    )

    def _mock_fetch(symbol: str, retries: int, retry_wait: float) -> pd.DataFrame:
        _ = (symbol, retries, retry_wait)
        return sample

    monkeypatch.setattr("qsys.utils.build_real_feature_store._safe_fetch_daily", _mock_fetch)

    out_root = build_real_feature_store(
        feature_root=tmp_path / "feature_store",
        symbols=["sh600000", "sz000001"],
        request_sleep=0,
    )

    parts = sorted(out_root.glob("trade_date=*/data.parquet"))
    assert len(parts) == len(sample)

    one = pd.read_parquet(parts[0])
    assert one.shape[0] == 2
    assert list(one.columns) == REQUIRED_COLUMNS
    assert one["ts_code"].isin(["600000.SH", "000001.SZ"]).all()
    assert one["market_cap"].notna().all()


def test_normalize_daily_frame_chinese_hist_columns() -> None:
    raw = pd.DataFrame(
        {
            "日期": ["2025-01-02", "2025-01-03"],
            "开盘": [10, 11],
            "最高": [10.5, 11.5],
            "最低": [9.8, 10.8],
            "收盘": [10.2, 11.2],
            "成交量": [1000, 1100],
            "成交额": [10000, 11000],
            "换手率": [1.2, 1.3],
            "代码": ["600000", "600000"],
        }
    )
    out = _normalize_daily_frame(raw, "sh600000")
    assert list(out.columns) == REQUIRED_COLUMNS
    assert out["trade_date"].iloc[0] == "2025-01-02"
    assert out["ts_code"].str.endswith(".SH").all()


def test_start_date_filter_with_datetime_handling(tmp_path, monkeypatch) -> None:
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-12-31", "2025-01-02", "2025-01-03"]),
            "open": [10, 11, 12],
            "high": [11, 12, 13],
            "low": [9, 10, 11],
            "close": [10, 11, 12],
            "volume": [100, 120, 140],
            "amount": [1000, 1100, 1200],
            "turnover": [1.0, 1.1, 1.2],
            "outstanding_share": [1_000_000, 1_000_000, 1_000_000],
        }
    )

    monkeypatch.setattr("qsys.utils.build_real_feature_store._safe_fetch_daily", lambda **_: raw)

    out_root = build_real_feature_store(
        feature_root=tmp_path / "feature_store",
        symbols=["sh600000"],
        start_date="2025-01-01",
        request_sleep=0,
    )

    parts = sorted(out_root.glob("trade_date=*/data.parquet"))
    assert [p.parent.name for p in parts] == ["trade_date=2025-01-02", "trade_date=2025-01-03"]


def test_skip_failed_symbols_continues(tmp_path, monkeypatch) -> None:
    ok = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "open": [10, 11],
            "high": [11, 12],
            "low": [9, 10],
            "close": [10, 11],
            "volume": [100, 120],
            "amount": [1000, 1100],
            "turnover": [1.0, 1.1],
            "outstanding_share": [1_000_000, 1_000_000],
        }
    )

    def _mock_fetch(symbol: str, retries: int, retry_wait: float) -> pd.DataFrame:
        _ = (retries, retry_wait)
        if symbol == "bad":
            return pd.DataFrame()
        return ok

    monkeypatch.setattr("qsys.utils.build_real_feature_store._safe_fetch_daily", _mock_fetch)

    out_root = build_real_feature_store(
        feature_root=tmp_path / "feature_store",
        symbols=["bad", "sh600000"],
        skip_failed_symbols=True,
        request_sleep=0,
        verbose=True,
    )

    parts = sorted(out_root.glob("trade_date=*/data.parquet"))
    assert len(parts) == 2
    one = pd.read_parquet(parts[0])
    assert list(one.columns) == REQUIRED_COLUMNS
