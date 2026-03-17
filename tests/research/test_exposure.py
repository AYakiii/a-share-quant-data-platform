from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.exposure import exposure_summary, size_exposure_daily


def test_exposure_contract() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
        ],
        names=["date", "asset"],
    )

    signal = pd.Series([1.0, 2.0, 2.0, 1.0], index=idx)
    market_cap = pd.Series([100.0, 200.0, 200.0, 100.0], index=idx)
    feats = pd.DataFrame({"ret_20d": [0.1, 0.2, 0.2, 0.1], "vol_20d": [1.0, 2.0, 2.0, 1.0]}, index=idx)

    d = size_exposure_daily(signal, market_cap)
    assert d.index.name == "date"
    assert len(d) == 2

    out = exposure_summary(signal, market_cap=market_cap, features=feats)
    assert "size_exposure_daily" in out
    assert "size_exposure_agg" in out
    assert "feature_corr_daily" in out
    assert "feature_corr_agg" in out
