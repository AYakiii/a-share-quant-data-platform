"""Exposure analysis diagnostics for Research v1."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qsys.research.ic import _validate_strict_alignment


def size_exposure_daily(signal: pd.Series, market_cap: pd.Series) -> pd.Series:
    """Daily cross-sectional correlation between signal and log(market_cap)."""

    df = _validate_strict_alignment(signal, market_cap).rename(columns={"label": "market_cap"})
    df["log_market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce").clip(lower=1e-12).map(
        lambda x: pd.NA if pd.isna(x) else float(np.log(x))
    )

    def _corr(g: pd.DataFrame) -> float:
        gg = g.dropna(subset=["signal", "log_market_cap"])
        if len(gg) < 2:
            return float("nan")
        return float(gg["signal"].corr(gg["log_market_cap"], method="pearson"))

    out = df.groupby(level="date").apply(_corr)
    out.name = "size_exposure"
    return out.sort_index()


def group_exposure_daily(signal: pd.Series, group_labels: pd.Series) -> pd.DataFrame:
    """Daily group exposure as mean(signal) by group within each date."""

    df = _validate_strict_alignment(signal, group_labels).rename(columns={"label": "group"})
    df = df.dropna(subset=["signal", "group"])
    out = (
        df.groupby([df.index.get_level_values("date"), df["group"]])["signal"]
        .mean()
        .rename("group_mean_signal")
        .reset_index()
        .rename(columns={"level_0": "date"})
    )
    return out.sort_values(["date", "group"]).reset_index(drop=True)


def signal_feature_correlation_daily(signal: pd.Series, features: pd.DataFrame) -> pd.DataFrame:
    """Daily correlation between signal and each provided feature column.

    Strict alignment is enforced against each feature column.
    """

    if not isinstance(features.index, pd.MultiIndex) or features.index.names != ["date", "asset"]:
        raise ValueError("features must be MultiIndex [date, asset]")
    if not signal.index.equals(features.index):
        raise ValueError("signal and features must have identical [date, asset] index")

    rows: list[dict[str, object]] = []
    for col in features.columns:
        df = _validate_strict_alignment(signal, features[col]).rename(columns={"label": col})

        def _corr(g: pd.DataFrame) -> float:
            gg = g.dropna(subset=["signal", col])
            if len(gg) < 2:
                return float("nan")
            return float(gg["signal"].corr(gg[col], method="pearson"))

        c = df.groupby(level="date").apply(_corr)
        for d, v in c.items():
            rows.append({"date": pd.Timestamp(d), "feature": col, "correlation": float(v)})

    return pd.DataFrame(rows).sort_values(["date", "feature"]).reset_index(drop=True)


def exposure_summary(
    signal: pd.Series,
    *,
    market_cap: pd.Series | None = None,
    group_labels: pd.Series | None = None,
    features: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame | pd.Series]:
    """Return per-date and aggregated exposure diagnostics."""

    out: dict[str, pd.DataFrame | pd.Series] = {}

    if market_cap is not None:
        daily_size = size_exposure_daily(signal, market_cap)
        out["size_exposure_daily"] = daily_size
        out["size_exposure_agg"] = pd.Series(
            {"mean": float(daily_size.mean()), "std": float(daily_size.std(ddof=0)), "n_days": int(daily_size.notna().sum())}
        )

    if group_labels is not None:
        g = group_exposure_daily(signal, group_labels)
        out["group_exposure_daily"] = g
        if not g.empty:
            out["group_exposure_agg"] = (
                g.groupby("group")["group_mean_signal"].mean().rename("avg_group_mean_signal").reset_index()
            )
        else:
            out["group_exposure_agg"] = pd.DataFrame(columns=["group", "avg_group_mean_signal"])

    if features is not None:
        c = signal_feature_correlation_daily(signal, features)
        out["feature_corr_daily"] = c
        if not c.empty:
            out["feature_corr_agg"] = (
                c.groupby("feature")["correlation"].mean().rename("mean_correlation").reset_index()
            )
        else:
            out["feature_corr_agg"] = pd.DataFrame(columns=["feature", "mean_correlation"])

    return out
