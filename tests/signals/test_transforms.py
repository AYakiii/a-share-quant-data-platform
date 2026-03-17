from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.signals.transforms import rank_cross_section, zscore_cross_section


def test_cross_section_transform_contract() -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
        ],
        names=["date", "asset"],
    )
    s = pd.Series([1.0, 3.0, 2.0, 2.0], index=idx, name="x")

    ranked = rank_cross_section(s)
    zed = zscore_cross_section(s)

    assert ranked.index.names == ["date", "asset"]
    assert zed.index.names == ["date", "asset"]
    assert float(ranked.loc[(pd.Timestamp("2024-01-02"), "A")]) < float(
        ranked.loc[(pd.Timestamp("2024-01-02"), "B")]
    )
    assert round(float(zed.loc[(pd.Timestamp("2024-01-02"), "A")]), 6) == -1.0
