"""Conditioned Rank IC diagnostics by risk-state buckets."""

from __future__ import annotations

import math

import pandas as pd

from qsys.research.signal_quality.ic import compute_ic_by_date


def _validate_multiindex(df: pd.DataFrame, arg_name: str) -> None:
    if not isinstance(df.index, pd.MultiIndex) or df.index.names != ["date", "asset"]:
        raise ValueError(f"{arg_name} must be MultiIndex ['date', 'asset']")


def assign_condition_buckets(
    exposures: pd.DataFrame,
    *,
    cols: tuple[str, ...] = ("vol_20d_z", "liquidity_z", "size_z"),
    n_buckets: int = 3,
) -> pd.DataFrame:
    """Assign cross-sectional condition buckets by date for selected exposure columns."""

    _validate_multiindex(exposures, "exposures")
    if n_buckets <= 0:
        raise ValueError("n_buckets must be > 0")

    missing = [c for c in cols if c not in exposures.columns]
    if missing:
        raise ValueError(f"exposures missing required columns: {missing}")

    out = pd.DataFrame(index=exposures.index)

    for col in cols:
        out_col = f"{col}_bucket"

        def _assign_one_date(g: pd.Series) -> pd.Series:
            valid = g.dropna()
            if len(valid) < n_buckets:
                return pd.Series([pd.NA] * len(g), index=g.index, dtype="Int64")
            try:
                ranked = valid.rank(method="first")
                buckets = pd.qcut(ranked, q=n_buckets, labels=False) + 1
            except ValueError:
                return pd.Series([pd.NA] * len(g), index=g.index, dtype="Int64")

            row = pd.Series(pd.NA, index=g.index, dtype="Int64")
            row.loc[valid.index] = buckets.astype("Int64")
            return row

        bucket_series = exposures[col].groupby(level="date", group_keys=False).apply(_assign_one_date)
        out[out_col] = bucket_series.astype("Int64")

    return out


def compute_conditioned_rank_ic(
    frame: pd.DataFrame,
    *,
    signal_col: str = "signal",
    fwd_ret_col: str = "fwd_ret_5d",
    bucket_col: str,
    condition_name: str | None = None,
) -> pd.DataFrame:
    """Compute conditioned daily Spearman Rank IC summary by bucket."""

    _validate_multiindex(frame, "frame")

    required = [signal_col, fwd_ret_col, bucket_col]
    missing = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"frame missing required columns: {missing}")

    cond = condition_name or bucket_col
    horizon = fwd_ret_col

    work = frame[[signal_col, fwd_ret_col, bucket_col]].copy()
    work = work.dropna(subset=[bucket_col])

    if work.empty:
        return pd.DataFrame(
            columns=[
                "condition_name",
                "bucket",
                "horizon",
                "mean_rank_ic",
                "median_rank_ic",
                "std_rank_ic",
                "icir",
                "positive_rate",
                "n_dates",
                "avg_n_assets",
            ]
        )

    rows: list[dict[str, object]] = []
    for bucket in sorted(work[bucket_col].dropna().unique()):
        sub = work[work[bucket_col] == bucket]

        # average cross-sectional sample size per date (after signal/return non-null filtering)
        aligned = sub[[signal_col, fwd_ret_col]].dropna()
        if len(aligned):
            avg_n_assets = float(aligned.groupby(level="date").size().mean())
        else:
            avg_n_assets = float("nan")

        ic_series = compute_ic_by_date(sub, signal_col=signal_col, return_col=fwd_ret_col, method="spearman")
        s = ic_series.dropna().astype(float)
        n = len(s)
        mean_ic = float(s.mean()) if n else float("nan")
        median_ic = float(s.median()) if n else float("nan")
        std_ic = float(s.std(ddof=1)) if n > 1 else float("nan")
        icir = float(mean_ic / std_ic) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")
        positive_rate = float((s > 0).mean()) if n else float("nan")

        rows.append(
            {
                "condition_name": cond,
                "bucket": int(bucket),
                "horizon": horizon,
                "mean_rank_ic": mean_ic,
                "median_rank_ic": median_ic,
                "std_rank_ic": std_ic,
                "icir": icir,
                "positive_rate": positive_rate,
                "n_dates": float(n),
                "avg_n_assets": avg_n_assets,
            }
        )

    return pd.DataFrame(rows).sort_values("bucket", kind="mergesort").reset_index(drop=True)
