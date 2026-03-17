from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.research.ic import daily_ic, daily_rank_ic


def test_daily_ic_rank_ic_contract_and_anti_lookahead() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
        ],
        names=["date", "asset"],
    )

    signal = pd.Series([1.0, 2.0, 1.0, 2.0], index=idx)
    fwd = pd.Series([0.1, 0.2, -0.1, -0.2], index=idx)

    ic = daily_ic(signal, fwd)
    ric = daily_rank_ic(signal, fwd)

    assert list(ic.index) == [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]
    assert round(float(ic.loc[pd.Timestamp("2024-01-02")]), 6) == 1.0
    assert round(float(ic.loc[pd.Timestamp("2024-01-03")]), 6) == -1.0
    assert round(float(ric.loc[pd.Timestamp("2024-01-02")]), 6) == 1.0

    # Anti-lookahead/alignment guard: mismatched index must fail fast.
    shifted_label = fwd.copy()
    shifted_label.index = pd.MultiIndex.from_tuples(
        [(d + pd.Timedelta(days=1), a) for d, a in shifted_label.index], names=["date", "asset"]
    )
    with pytest.raises(ValueError):
        daily_ic(signal, shifted_label)
