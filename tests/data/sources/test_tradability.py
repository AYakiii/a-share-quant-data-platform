from __future__ import annotations

import pandas as pd

from qsys.data.sources.tradability import build_tradability_mask_v0_from_daily


def test_tradability_proxy_rules(tmp_path):
    fp = tmp_path / "stock_zh_a_daily" / "v1" / "symbol=000001" / "start_date=2026-01-01_end_date=2026-01-05" / "data.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
            "symbol": ["000001"] * 4,
            "open": [1.0, 1.0, 1.0, None],
            "high": [1.2, 1.2, 0.8, 1.1],
            "low": [0.9, 0.9, 0.9, 0.8],
            "close": [1.1, 1.1, 1.0, 1.0],
            "volume": [10, 0, 10, 10],
            "amount": [100, 100, 100, 100],
        }
    )
    df.to_parquet(fp, index=False)
    out = build_tradability_mask_v0_from_daily(tmp_path)
    assert bool(out.loc[0, "tradable_proxy"]) is True
    assert bool(out.loc[1, "tradable_proxy"]) is False
    assert bool(out.loc[2, "tradable_proxy"]) is False
    assert bool(out.loc[3, "tradable_proxy"]) is False
