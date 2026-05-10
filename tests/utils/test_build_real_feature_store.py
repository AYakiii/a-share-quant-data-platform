from __future__ import annotations

import pandas as pd
import pytest

from qsys.utils.build_real_feature_store import REQUIRED_COLUMNS, build_real_feature_store


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


def test_build_real_feature_store_skip_failed_symbols_continues(tmp_path, monkeypatch) -> None:
    sample = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
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
        if symbol == "sh600000":
            raise RuntimeError("mock fetch failed")
        return sample

    monkeypatch.setattr("qsys.utils.build_real_feature_store._safe_fetch_daily", _mock_fetch)

    out_root = build_real_feature_store(
        feature_root=tmp_path / "feature_store",
        symbols=["sh600000", "sz000001"],
        request_sleep=0,
        skip_failed_symbols=True,
    )
    assert (out_root / "failed_symbols.csv").exists()
    failed = pd.read_csv(out_root / "failed_symbols.csv")
    assert "sh600000" in failed["symbol"].tolist()


def test_build_real_feature_store_strict_mode_raises_on_failed_symbol(tmp_path, monkeypatch) -> None:
    def _mock_fetch(symbol: str, retries: int, retry_wait: float) -> pd.DataFrame:
        _ = (symbol, retries, retry_wait)
        raise RuntimeError("mock fetch failed")

    monkeypatch.setattr("qsys.utils.build_real_feature_store._safe_fetch_daily", _mock_fetch)

    with pytest.raises(RuntimeError, match="mock fetch failed"):
        build_real_feature_store(
            feature_root=tmp_path / "feature_store",
            symbols=["sh600000"],
            request_sleep=0,
            skip_failed_symbols=False,
        )
