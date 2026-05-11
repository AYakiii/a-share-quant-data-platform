"""Lightweight diagnostics for candidate factor outputs."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from qsys.factors.factor_output import validate_factor_output


def _numeric_factors(factors: pd.DataFrame) -> pd.DataFrame:
    return factors.select_dtypes(include=[np.number]).copy()


def summarize_factor_coverage(factors: pd.DataFrame) -> pd.DataFrame:
    """Summarize coverage/missingness for numeric factor columns."""

    num = _numeric_factors(factors)
    n_total = len(num)
    n_dates = factors.index.get_level_values("date").nunique() if isinstance(factors.index, pd.MultiIndex) else 0
    n_assets = factors.index.get_level_values("asset").nunique() if isinstance(factors.index, pd.MultiIndex) else 0

    rows = []
    for c in sorted(num.columns):
        n_non_null = int(num[c].notna().sum())
        n_missing = int(n_total - n_non_null)
        coverage = float(n_non_null / n_total) if n_total else float("nan")
        rows.append(
            {
                "factor_name": c,
                "n_total": int(n_total),
                "n_non_null": n_non_null,
                "coverage": coverage,
                "n_missing": n_missing,
                "missing_rate": float(n_missing / n_total) if n_total else float("nan"),
                "n_dates": int(n_dates),
                "n_assets": int(n_assets),
            }
        )
    return pd.DataFrame(rows)


def summarize_factor_distribution(factors: pd.DataFrame) -> pd.DataFrame:
    """Summarize numeric factor distribution statistics."""

    num = _numeric_factors(factors)
    rows = []
    for c in sorted(num.columns):
        s = pd.to_numeric(num[c], errors="coerce").dropna()
        rows.append(
            {
                "factor_name": c,
                "mean": float(s.mean()) if len(s) else float("nan"),
                "std": float(s.std(ddof=1)) if len(s) > 1 else float("nan"),
                "min": float(s.min()) if len(s) else float("nan"),
                "p01": float(s.quantile(0.01)) if len(s) else float("nan"),
                "p05": float(s.quantile(0.05)) if len(s) else float("nan"),
                "p25": float(s.quantile(0.25)) if len(s) else float("nan"),
                "median": float(s.median()) if len(s) else float("nan"),
                "p75": float(s.quantile(0.75)) if len(s) else float("nan"),
                "p95": float(s.quantile(0.95)) if len(s) else float("nan"),
                "p99": float(s.quantile(0.99)) if len(s) else float("nan"),
                "max": float(s.max()) if len(s) else float("nan"),
                "n_non_null": int(len(s)),
            }
        )
    return pd.DataFrame(rows)


def compute_factor_correlation(
    factors: pd.DataFrame,
    method: str = "spearman",
    min_periods: int = 20,
) -> pd.DataFrame:
    """Compute square numeric-factor correlation matrix."""

    if method not in {"pearson", "spearman"}:
        raise ValueError("method must be 'pearson' or 'spearman'")
    num = _numeric_factors(factors)
    return num.corr(method=method, min_periods=min_periods)


def find_highly_correlated_factors(corr: pd.DataFrame, threshold: float = 0.90) -> pd.DataFrame:
    """Find highly correlated factor pairs from upper triangle only."""

    rows: list[dict[str, float | str]] = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            v = corr.loc[a, b]
            if pd.isna(v):
                continue
            av = abs(float(v))
            if av >= threshold:
                rows.append({"factor_a": a, "factor_b": b, "correlation": float(v), "abs_correlation": av})
    out = pd.DataFrame(rows)
    if len(out) == 0:
        return pd.DataFrame(columns=["factor_a", "factor_b", "correlation", "abs_correlation"])
    return out.sort_values(["abs_correlation", "factor_a", "factor_b"], ascending=[False, True, True]).reset_index(drop=True)


def compute_factor_ic_by_date(
    factors: pd.DataFrame,
    labels: pd.DataFrame,
    label_col: str,
    method: str = "spearman",
    min_assets: int = 10,
) -> pd.DataFrame:
    """Compute cross-sectional IC per date for each numeric factor column.

    Deterministic rule: if aligned assets < min_assets, emit NaN ic row (do not skip).
    """

    if method not in {"pearson", "spearman"}:
        raise ValueError("method must be 'pearson' or 'spearman'")
    if label_col not in labels.columns:
        raise ValueError(f"label_col not found in labels: {label_col}")

    num = _numeric_factors(factors)
    label_s = pd.to_numeric(labels[label_col], errors="coerce")
    idx = num.index.intersection(label_s.index)
    num = num.loc[idx]
    label_s = label_s.loc[idx]

    rows: list[dict[str, object]] = []
    dates = pd.Index(num.index.get_level_values("date")).unique()
    for d in dates:
        f_d = num.xs(d, level="date", drop_level=False)
        y_d = label_s.xs(d, level="date", drop_level=False)
        for c in sorted(f_d.columns):
            x = pd.to_numeric(f_d[c], errors="coerce")
            frame = pd.concat([x.rename("x"), y_d.rename("y")], axis=1).dropna()
            n_assets = int(len(frame))
            if n_assets < min_assets:
                ic = float("nan")
            else:
                ic = float(frame["x"].corr(frame["y"], method=method))
            rows.append({"date": pd.Timestamp(d), "factor_name": c, "label_col": label_col, "ic": ic, "n_assets": n_assets})

    return pd.DataFrame(rows).sort_values(["date", "factor_name"]).reset_index(drop=True)


def summarize_ic(ic_by_date: pd.DataFrame) -> pd.DataFrame:
    """Summarize IC statistics by factor_name x label_col."""

    rows: list[dict[str, float | str]] = []
    for (factor_name, label_col), g in ic_by_date.groupby(["factor_name", "label_col"], sort=True):
        s = pd.to_numeric(g["ic"], errors="coerce").dropna()
        n = int(len(s))
        mean_ic = float(s.mean()) if n else float("nan")
        median_ic = float(s.median()) if n else float("nan")
        std_ic = float(s.std(ddof=1)) if n > 1 else float("nan")
        if n > 1 and std_ic > 0 and not math.isnan(std_ic):
            icir = float(mean_ic / std_ic)
            t_stat = float(mean_ic / (std_ic / math.sqrt(n)))
        else:
            icir = float("nan")
            t_stat = float("nan")
        positive_rate = float((s > 0).mean()) if n else float("nan")
        rows.append(
            {
                "factor_name": str(factor_name),
                "label_col": str(label_col),
                "mean_ic": mean_ic,
                "median_ic": median_ic,
                "std_ic": std_ic,
                "icir": icir,
                "t_stat": t_stat,
                "positive_rate": positive_rate,
                "n_dates": n,
            }
        )
    return pd.DataFrame(rows)


def run_basic_factor_diagnostics(
    factors: pd.DataFrame,
    labels: pd.DataFrame | None = None,
    label_cols: list[str] | None = None,
    method: str = "spearman",
    corr_threshold: float = 0.90,
    min_assets: int = 10,
) -> dict[str, pd.DataFrame]:
    """Run lightweight factor diagnostics without writing files."""

    msgs = validate_factor_output(factors, allow_all_nan_columns=True)
    bad = [m for m in msgs if "forbidden field present" in m or "raw input column" in m or "MultiIndex" in m]
    if bad:
        raise ValueError("invalid factor output: " + "; ".join(bad))

    coverage = summarize_factor_coverage(factors)
    distribution = summarize_factor_distribution(factors)
    corr = compute_factor_correlation(factors, method=method)
    high_corr = find_highly_correlated_factors(corr, threshold=corr_threshold)

    out: dict[str, pd.DataFrame] = {
        "coverage": coverage,
        "distribution": distribution,
        "correlation": corr,
        "high_correlation_pairs": high_corr,
    }

    if labels is not None and label_cols:
        ic_frames = [compute_factor_ic_by_date(factors, labels, c, method=method, min_assets=min_assets) for c in label_cols]
        ic_by_date = pd.concat(ic_frames, ignore_index=True) if ic_frames else pd.DataFrame(
            columns=["date", "factor_name", "label_col", "ic", "n_assets"]
        )
        out["ic_by_date"] = ic_by_date
        out["ic_summary"] = summarize_ic(ic_by_date)

    return out
