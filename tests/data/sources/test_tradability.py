from __future__ import annotations

import pandas as pd
import pytest

from qsys.data.sources.tradability import build_tradability_mask_v0_from_daily


def test_tradability_proxy_rules_with_symbol_from_path(tmp_path):
    fp = tmp_path / "stock_zh_a_daily" / "v1" / "symbol='000001" / "start_date=2026-01-01_end_date=2026-01-05" / "data.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
            "open": [1.0, 1.0, 1.0, None, 1.0],
            "high": [1.2, 1.2, 0.8, 1.1, 1.2],
            "low": [0.9, 0.9, 0.9, 0.8, 0.9],
            "close": [1.1, 1.1, 1.0, 1.0, 1.1],
            "volume": [10, 0, 10, 10, 10],
            "amount": [100, 100, 100, 100, 0],
        }
    )
    df.to_parquet(fp, index=False)
    out = build_tradability_mask_v0_from_daily(tmp_path)
    assert out.loc[0, "stock_code"] == "000001"
    assert bool(out.loc[0, "tradable_proxy"]) is True
    assert bool(out.loc[1, "tradable_proxy"]) is False
    assert bool(out.loc[2, "tradable_proxy"]) is False
    assert bool(out.loc[3, "tradable_proxy"]) is False
    assert bool(out.loc[4, "tradable_proxy"]) is False


def test_tradability_error_includes_columns_and_path(tmp_path):
    fp = tmp_path / "stock_zh_a_daily" / "v1" / "symbol=abc" / "start_date=2026-01-01_end_date=2026-01-05" / "data.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": ["2026-01-01"], "open": [1]}).to_parquet(fp, index=False)
    with pytest.raises(ValueError) as e:
        build_tradability_mask_v0_from_daily(tmp_path)
    msg = str(e.value)
    assert "columns=" in msg
    assert "scanned_parquet" in msg
