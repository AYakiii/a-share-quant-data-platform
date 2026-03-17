"""Signal combination helpers."""

from __future__ import annotations

import pandas as pd


def linear_combine(signals: dict[str, pd.Series], weights: dict[str, float]) -> pd.Series:
    """Linearly combine named signals with provided weights."""

    if not signals:
        raise ValueError("signals is empty")

    missing_weights = [k for k in signals.keys() if k not in weights]
    if missing_weights:
        raise ValueError(f"Missing weights for signals: {missing_weights}")

    base_index = None
    combined = None

    for name, sig in signals.items():
        if base_index is None:
            base_index = sig.index
            combined = sig.astype(float) * float(weights[name])
        else:
            combined = combined.add(sig.reindex(base_index).astype(float) * float(weights[name]), fill_value=0.0)

    combined.name = "combined_signal"
    return combined.sort_index()
