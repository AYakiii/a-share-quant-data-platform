"""Pairwise signal correlation diagnostics."""

from __future__ import annotations

import itertools

import pandas as pd


def pairwise_signal_correlation(signals: dict[str, pd.Series], method: str = "pearson") -> pd.DataFrame:
    """Compute pairwise correlation using strictly intersected observations only."""

    if method not in {"pearson", "spearman"}:
        raise ValueError("method must be one of {'pearson', 'spearman'}")
    if not signals:
        return pd.DataFrame(columns=["signal_x", "signal_y", "correlation", "n_obs"])

    rows: list[dict[str, object]] = []
    for a, b in itertools.combinations(sorted(signals.keys()), 2):
        sa = signals[a]
        sb = signals[b]

        if not isinstance(sa.index, pd.MultiIndex) or sa.index.names != ["date", "asset"]:
            raise ValueError(f"signal {a} must be MultiIndex [date, asset]")
        if not isinstance(sb.index, pd.MultiIndex) or sb.index.names != ["date", "asset"]:
            raise ValueError(f"signal {b} must be MultiIndex [date, asset]")

        inter = sa.index.intersection(sb.index)
        if len(inter) == 0:
            rows.append({"signal_x": a, "signal_y": b, "correlation": float("nan"), "n_obs": 0})
            continue

        aa = sa.loc[inter]
        bb = sb.loc[inter]
        tmp = pd.concat([aa.rename("a"), bb.rename("b")], axis=1).dropna()

        corr = float(tmp["a"].corr(tmp["b"], method=method)) if len(tmp) >= 2 else float("nan")
        rows.append({"signal_x": a, "signal_y": b, "correlation": corr, "n_obs": int(len(tmp))})

    return pd.DataFrame(rows).sort_values(["signal_x", "signal_y"]).reset_index(drop=True)
