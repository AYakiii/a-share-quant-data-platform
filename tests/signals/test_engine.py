from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.signals.engine import SignalEngine, baseline_momentum_signal, demo_alpha_signal


def test_baseline_momentum_signal_rank_behavior() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
        ],
        names=["date", "asset"],
    )
    features = pd.DataFrame({"ret_20d": [0.1, 0.2, 0.4, 0.1]}, index=idx)
    sig = baseline_momentum_signal(features)
    assert sig.index.names == ["date", "asset"]
    assert float(sig.loc[(pd.Timestamp("2024-01-02"), "A")]) == pytest.approx(0.5)
    assert float(sig.loc[(pd.Timestamp("2024-01-02"), "B")]) == pytest.approx(1.0)
    assert float(sig.loc[(pd.Timestamp("2024-01-03"), "A")]) == pytest.approx(1.0)
    assert float(sig.loc[(pd.Timestamp("2024-01-03"), "B")]) == pytest.approx(0.5)


def test_demo_alpha_combination_behavior_experimental() -> None:
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

    # On 2024-01-02:
    # rank(ret): A=0.5, B=1.0; z(vol): A=-1, B=+1
    # alpha = rank(ret) - 0.5*z(vol) => A=1.0, B=0.5
    # Experimental behavior check only:
    # confirms the formula is internally consistent and volatility is penalized.
    assert float(alpha.loc[(pd.Timestamp("2024-01-02"), "A")]) == pytest.approx(1.0)
    assert float(alpha.loc[(pd.Timestamp("2024-01-02"), "B")]) == pytest.approx(0.5)
    assert float(alpha.loc[(pd.Timestamp("2024-01-02"), "A")]) > float(
        alpha.loc[(pd.Timestamp("2024-01-02"), "B")]
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
