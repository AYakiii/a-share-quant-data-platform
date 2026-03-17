from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.features.compute import compute_features


def test_feature_compute_contract() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"])
    idx = pd.MultiIndex.from_product([dates, ["000001.SZ"]], names=["date", "asset"])

    panel = pd.DataFrame(
        {
            "close": [10.0, 10.5, 10.0, 11.0, 11.0, 12.0],
            "amount": [100, 110, 90, 120, 130, 140],
            "market_cap": [1000, 1010, 990, 1050, 1050, 1080],
        },
        index=idx,
    )

    features = compute_features(panel, ["ret_1d", "turnover_5d", "market_cap", "fwd_ret_5d"])

    assert features.index.names == ["date", "asset"]
    assert list(features.columns) == ["ret_1d", "turnover_5d", "market_cap", "fwd_ret_5d"]
    assert round(float(features.iloc[1]["ret_1d"]), 6) == 0.05
    assert round(float(features.iloc[4]["turnover_5d"]), 6) == 110.0
    assert float(features.iloc[0]["market_cap"]) == 1000.0
    assert round(float(features.iloc[0]["fwd_ret_5d"]), 6) == 0.2
