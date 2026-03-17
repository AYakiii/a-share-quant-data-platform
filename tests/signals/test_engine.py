from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.signals.engine import SignalEngine, demo_alpha_signal


def test_demo_alpha_combination_behavior() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
        ],
        names=["date", "asset"],
    )

    features = pd.DataFrame(
        {
            "ret_20d": [0.1, 0.2, 0.4, 0.1],
            "vol_20d": [0.5, 1.0, 0.8, 0.8],
        },
        index=idx,
    )

    alpha = demo_alpha_signal(features)
    assert alpha.index.names == ["date", "asset"]

    # On 2024-01-02: B should beat A due to higher return rank and higher vol penalty offset scale.
    assert float(alpha.loc[(pd.Timestamp("2024-01-02"), "B")]) > float(
        alpha.loc[(pd.Timestamp("2024-01-02"), "A")]
    )


def test_engine_linear_combine_explicit() -> None:
    engine = SignalEngine()
    idx = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "A"), (pd.Timestamp("2024-01-02"), "B")],
        names=["date", "asset"],
    )
    signals = {
        "s1": pd.Series([1.0, 2.0], index=idx),
        "s2": pd.Series([2.0, 1.0], index=idx),
    }
    combined = engine.combine(signals, {"s1": 1.0, "s2": -0.5})
    assert round(float(combined.iloc[0]), 6) == 0.0
    assert round(float(combined.iloc[1]), 6) == 1.5
