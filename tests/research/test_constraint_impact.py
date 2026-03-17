from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.constraint_impact import compare_constraint_impact


def test_constraint_impact_contract() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    idx = pd.MultiIndex.from_product([dates, ["A", "B", "C"]], names=["date", "asset"])

    signal = pd.Series(
        [0.9, 0.5, 0.1, 0.8, 0.4, 0.2, 0.7, 0.6, 0.1, 0.9, 0.2, 0.3],
        index=idx,
    )
    ret1 = pd.Series(
        [0.01, 0.0, -0.01, 0.02, -0.01, 0.0, 0.01, 0.0, -0.01, 0.02, -0.01, 0.0],
        index=idx,
    )
    fwd5 = pd.Series(
        [0.02, 0.01, -0.01, 0.03, -0.02, 0.0, 0.01, 0.0, -0.01, 0.02, -0.01, 0.0],
        index=idx,
    )
    amount = pd.Series([100, 20, 5, 100, 20, 5, 100, 20, 5, 100, 20, 5], index=idx, dtype=float)
    mcap = pd.Series([300, 150, 50, 300, 150, 50, 300, 150, 50, 300, 150, 50], index=idx, dtype=float)

    out = compare_constraint_impact(
        signal,
        asset_returns=ret1,
        label_forward_return=fwd5,
        market_cap=mcap,
        unconstrained_kwargs={},
        constrained_kwargs={
            "max_single_weight": 0.6,
            "liquidity": amount,
            "min_liquidity": 10.0,
            "market_cap": mcap,
            "size_aware_scaling": True,
        },
    )

    assert set(out.keys()) == {"summary", "per_date"}
    assert set(out["summary"].columns) >= {
        "return_diff",
        "sharpe_diff",
        "turnover_diff",
        "ic_diff",
        "size_exposure_diff",
    }
    assert "return_diff" in out["per_date"].columns
    assert "ic_diff" in out["per_date"].columns
