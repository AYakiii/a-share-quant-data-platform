from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils.run_buffered_rebalance_from_feature_store import build_demo_signal_and_returns


def test_build_demo_signal_and_returns_smoke() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-01"), "A"),
            (pd.Timestamp("2024-01-01"), "B"),
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
        ],
        names=["date", "asset"],
    )
    features = pd.DataFrame(
        {
            "ret_20d": [0.1, 0.2, 0.15, 0.05],
            "vol_20d": [0.3, 0.1, 0.2, 0.4],
            "ret_1d": [0.01, -0.01, 0.02, -0.02],
            "is_tradable": [True, True, True, False],
        },
        index=idx,
    )

    signal_df, returns_df = build_demo_signal_and_returns(features)

    assert signal_df.index.names == ["date", "asset"]
    assert returns_df.index.names == ["date", "asset"]
    assert set(signal_df.columns) == {"score", "rank", "is_tradable"}
    assert set(returns_df.columns) == {"ret_1d"}
