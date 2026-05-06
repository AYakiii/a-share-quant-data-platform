"""Run minimal signal quality diagnostics MVP from feature store."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qsys.research.signal_quality.align import prepare_signal_quality_frame
from qsys.research.signal_quality.ic import compute_ic_by_date, summarize_ic
from qsys.research.signal_quality.quantile import (
    assign_quantiles_by_date,
    compute_quantile_forward_returns,
    compute_quantile_spread,
)
from qsys.signals.engine import load_feature_store_frame


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Signal quality MVP")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--signal-col", default=None)
    p.add_argument("--signal-preset", choices=["momentum_vol"], default=None)
    p.add_argument("--fwd-cols", nargs="+", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--quantiles", type=int, default=5)
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def _compute_momentum_vol_signal(features: pd.DataFrame) -> pd.Series:
    required = ["ret_20d", "vol_20d"]
    missing = [c for c in required if c not in features.columns]
    if missing:
        raise KeyError(f"signal preset 'momentum_vol' requires columns: {missing}")

    base = features[required].dropna(subset=required).copy()
    ret_rank = base.groupby(level="date")["ret_20d"].rank(pct=True)
    vol_mean = base.groupby(level="date")["vol_20d"].transform("mean")
    vol_std = base.groupby(level="date")["vol_20d"].transform("std").replace(0.0, pd.NA)
    vol_z = ((base["vol_20d"] - vol_mean) / vol_std).fillna(0.0)
    return (ret_rank - 0.5 * vol_z).rename("signal")


def build_signal_quality_input(
    feature_root: str,
    signal_col: str | None,
    signal_preset: str | None,
    fwd_cols: list[str],
    start_date: str | None,
    end_date: str | None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Build aligned signal-quality frame from raw feature store for either mode."""

    if signal_preset is None:
        if signal_col is None:
            raise ValueError("either --signal-col or --signal-preset must be provided")
        return prepare_signal_quality_frame(
            feature_root=feature_root,
            signal_col=signal_col,
            fwd_ret_cols=fwd_cols,
            start_date=start_date,
            end_date=end_date,
        )

    features = load_feature_store_frame(feature_root=feature_root, start_date=start_date, end_date=end_date)
    missing_fwd = [c for c in fwd_cols if c not in features.columns]
    if missing_fwd:
        raise KeyError(f"forward return columns not found: {missing_fwd}")

    if signal_preset == "momentum_vol":
        signal = _compute_momentum_vol_signal(features)
    else:
        raise KeyError(f"unsupported signal preset: {signal_preset}")

    out = pd.concat([signal, features[fwd_cols]], axis=1)
    before = len(out)
    out = out.dropna(subset=["signal", *fwd_cols]).sort_index()
    after = len(out)
    stats = {
        "n_rows_before": float(before),
        "n_rows_after": float(after),
        "coverage_ratio": float(after / before) if before > 0 else 0.0,
        "n_dates": float(out.index.get_level_values("date").nunique()) if len(out) else 0.0,
        "n_assets": float(out.index.get_level_values("asset").nunique()) if len(out) else 0.0,
    }
    return out, stats


def main() -> None:
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    df, coverage = build_signal_quality_input(
        feature_root=args.feature_root,
        signal_col=args.signal_col,
        signal_preset=args.signal_preset,
        fwd_cols=args.fwd_cols,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    pd.DataFrame([coverage]).to_csv(out / "coverage_stats.csv", index=False)

    qdf = assign_quantiles_by_date(df, signal_col="signal", q=args.quantiles)

    for fwd_col in args.fwd_cols:
        ic_by_date = compute_ic_by_date(df, signal_col="signal", return_col=fwd_col, method="spearman")
        ic_summary = summarize_ic(ic_by_date)

        qret = compute_quantile_forward_returns(qdf, quantile_col="quantile", return_col=fwd_col)
        qspread_df, qspread_summary = compute_quantile_spread(
            qret,
            top_quantile=args.quantiles,
            bottom_quantile=1,
        )

        ic_by_date.rename("rank_ic").to_frame().reset_index().to_csv(out / f"ic_by_date_{fwd_col}.csv", index=False)
        pd.DataFrame([ic_summary]).to_csv(out / f"ic_summary_{fwd_col}.csv", index=False)
        qret.to_csv(out / f"quantile_return_{fwd_col}.csv", index=False)
        qspread_df.to_csv(out / f"quantile_spread_{fwd_col}.csv", index=False)

        q5_mean = float(qret[qret["quantile"] == args.quantiles]["mean_forward_return"].mean()) if len(qret) else float("nan")
        q1_mean = float(qret[qret["quantile"] == 1]["mean_forward_return"].mean()) if len(qret) else float("nan")
        print(f"[{fwd_col}] mean Rank IC={ic_summary['mean_ic']:.6f}, ICIR={ic_summary['icir']:.6f}, positive_rate={ic_summary['positive_rate']:.3f}")
        print(f"[{fwd_col}] top-minus-bottom={qspread_summary['mean_top_minus_bottom']:.6f}, Q{args.quantiles}>Q1={q5_mean > q1_mean}")


if __name__ == "__main__":
    main()
