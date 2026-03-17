"""Signal decay diagnostics across forward return horizons."""

from __future__ import annotations

import pandas as pd

from qsys.research.ic import daily_ic, daily_rank_ic


def decay_analysis(
    signal: pd.Series,
    forward_returns_by_horizon: dict[str, pd.Series],
) -> pd.DataFrame:
    """Compute IC/RankIC summary per forward-return horizon.

    Parameters
    ----------
    signal
        Signal indexed by [date, asset].
    forward_returns_by_horizon
        Mapping horizon name -> forward return series aligned on [date, asset].
    """

    rows: list[dict[str, object]] = []
    for horizon, label in forward_returns_by_horizon.items():
        ic = daily_ic(signal, label)
        ric = daily_rank_ic(signal, label)
        rows.append(
            {
                "horizon": horizon,
                "ic_mean": float(ic.mean()),
                "rank_ic_mean": float(ric.mean()),
                "n_days": int(ic.notna().sum()),
            }
        )

    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
