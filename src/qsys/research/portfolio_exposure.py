"""Portfolio exposure aggregation utilities."""

from __future__ import annotations

import pandas as pd


def _validate_multiindex_date_asset(obj: pd.Series | pd.DataFrame, *, arg_name: str) -> None:
    if not isinstance(obj.index, pd.MultiIndex) or obj.index.names != ["date", "asset"]:
        raise ValueError(f"{arg_name} must be MultiIndex ['date', 'asset']")


def _as_weight_series(weights: pd.DataFrame | pd.Series, *, weight_col: str) -> pd.Series:
    if isinstance(weights, pd.Series):
        w = weights.copy()
    else:
        if weight_col not in weights.columns:
            raise ValueError(f"weights missing required column: {weight_col}")
        w = weights[weight_col].copy()

    _validate_multiindex_date_asset(w, arg_name="weights")
    return pd.to_numeric(w, errors="coerce")


def compute_portfolio_exposure(
    weights: pd.DataFrame | pd.Series,
    exposures: pd.DataFrame,
    *,
    weight_col: str = "target_weight",
) -> pd.DataFrame:
    """Compute time-series portfolio exposures from weights and per-asset exposure matrix."""

    w = _as_weight_series(weights, weight_col=weight_col)
    _validate_multiindex_date_asset(exposures, arg_name="exposures")

    # normalize date level for deterministic alignment/reindex
    w = w.copy()
    w.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(w.index.get_level_values("date")), w.index.get_level_values("asset")],
        names=["date", "asset"],
    )
    ex = exposures.copy()
    ex.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(ex.index.get_level_values("date")), ex.index.get_level_values("asset")],
        names=["date", "asset"],
    )

    if len(ex.columns) == 0:
        raise ValueError("exposures must include at least one exposure column")

    all_dates = pd.Index(w.index.get_level_values("date")).union(ex.index.get_level_values("date")).unique().sort_values()
    out = pd.DataFrame(index=all_dates)
    out.index.name = "date"

    # Holdings/weight diagnostics from weights only.
    w_nonzero = w[w != 0]
    out["n_holdings"] = w_nonzero.groupby(level="date").size().reindex(all_dates)
    out["gross_weight"] = w.abs().groupby(level="date").sum().reindex(all_dates)
    out["net_weight"] = w.groupby(level="date").sum().reindex(all_dates)

    # Strict [date, asset] join; per-exposure column computed independently.
    for col in ex.columns:
        merged = pd.concat([w.rename("weight"), pd.to_numeric(ex[col], errors="coerce").rename("exposure")], axis=1, join="inner")
        valid = merged.dropna(subset=["weight", "exposure"])
        expo = (valid["weight"] * valid["exposure"]).groupby(level="date").sum()
        out[f"portfolio_{col}"] = expo.reindex(all_dates)

    return out


def summarize_exposure_stability(exposure_ts: pd.DataFrame) -> pd.DataFrame:
    """Summarize numeric exposure time-series columns with robust distribution stats."""

    numeric = exposure_ts.select_dtypes(include=["number"]).copy()
    if numeric.empty:
        return pd.DataFrame(columns=["mean", "std", "min", "p25", "median", "p75", "max"])

    rows: list[dict[str, float | str]] = []
    for col in numeric.columns:
        s = pd.to_numeric(numeric[col], errors="coerce").dropna()
        rows.append(
            {
                "name": col,
                "mean": float(s.mean()) if len(s) else float("nan"),
                "std": float(s.std(ddof=0)) if len(s) else float("nan"),
                "min": float(s.min()) if len(s) else float("nan"),
                "p25": float(s.quantile(0.25)) if len(s) else float("nan"),
                "median": float(s.median()) if len(s) else float("nan"),
                "p75": float(s.quantile(0.75)) if len(s) else float("nan"),
                "max": float(s.max()) if len(s) else float("nan"),
            }
        )

    return pd.DataFrame(rows).set_index("name").sort_index()
