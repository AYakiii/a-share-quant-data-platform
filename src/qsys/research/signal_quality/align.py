"""Data alignment for signal quality diagnostics.

Anti-lookahead contract:
- signal at date t is evaluated against forward returns already aligned at date t.
- this module does not shift signal or labels.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd

from qsys.signals.engine import load_feature_store_frame


def prepare_signal_quality_frame(
    feature_root: str | Path,
    signal_col: str,
    fwd_ret_cols: Sequence[str],
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Load and filter feature-store frame for signal quality analysis."""

    features = load_feature_store_frame(
        feature_root=feature_root,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
    )
    if signal_col not in features.columns:
        raise KeyError(f"signal column not found: {signal_col}")

    missing_fwd = [c for c in fwd_ret_cols if c not in features.columns]
    if missing_fwd:
        raise KeyError(f"forward return columns not found: {missing_fwd}")

    keep_cols = [signal_col, *list(fwd_ret_cols)]
    raw = features[keep_cols].copy()
    raw = raw.rename(columns={signal_col: "signal"})

    before = len(raw)
    clean = raw.dropna(subset=["signal", *list(fwd_ret_cols)]).sort_index()
    after = len(clean)

    stats = {
        "n_rows_before": float(before),
        "n_rows_after": float(after),
        "coverage_ratio": float(after / before) if before > 0 else 0.0,
        "n_dates": float(clean.index.get_level_values("date").nunique()) if len(clean) else 0.0,
        "n_assets": float(clean.index.get_level_values("asset").nunique()) if len(clean) else 0.0,
    }
    return clean, stats
