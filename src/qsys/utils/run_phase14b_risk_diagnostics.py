"""Run Phase 14B end-to-end risk diagnostics from feature store."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qsys.research.signal_quality.conditioned_ic import assign_condition_buckets, compute_conditioned_rank_ic
from qsys.risk.exposure import build_risk_exposure_matrix
from qsys.signals.engine import load_feature_store_frame
from qsys.universe.eligibility import apply_eligibility_mask, build_eligibility_mask

DEFAULT_REQUIRE_COLUMNS = (
    "ret_20d",
    "vol_20d",
    "amount_20d",
    "market_cap",
    "fwd_ret_5d",
    "fwd_ret_20d",
)


def _build_alpha_ret20d_rank(features: pd.DataFrame) -> pd.Series:
    ret = pd.to_numeric(features["ret_20d"], errors="coerce")
    return ret.groupby(level="date").rank(pct=True).rename("signal")


def run_phase14b_risk_diagnostics(
    *,
    feature_root: str,
    output_dir: str,
    start_date: str | None = None,
    end_date: str | None = None,
    n_buckets: int = 3,
) -> dict[str, Path]:
    """Execute Phase 14B diagnostics pipeline and persist outputs to CSV."""

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    features = load_feature_store_frame(
        feature_root=feature_root,
        start_date=start_date,
        end_date=end_date,
    )

    n_rows_loaded = int(len(features))

    eligible = build_eligibility_mask(
        features,
        require_columns=DEFAULT_REQUIRE_COLUMNS,
        require_tradable=True,
    )
    eligible_features = apply_eligibility_mask(features, eligible)

    alpha = _build_alpha_ret20d_rank(eligible_features)
    exposures = build_risk_exposure_matrix(eligible_features)
    buckets = assign_condition_buckets(exposures, n_buckets=n_buckets)

    base = pd.concat(
        [
            alpha.rename("signal"),
            eligible_features[["fwd_ret_5d", "fwd_ret_20d"]],
            exposures,
            buckets,
        ],
        axis=1,
    )

    saved: dict[str, Path] = {}
    configs = [
        ("vol_20d_z_bucket", "vol_20d_z"),
        ("liquidity_z_bucket", "liquidity_z"),
        ("size_z_bucket", "size_z"),
    ]
    horizons = ["fwd_ret_5d", "fwd_ret_20d"]

    for bucket_col, condition_label in configs:
        for horizon in horizons:
            result = compute_conditioned_rank_ic(
                base,
                signal_col="signal",
                fwd_ret_col=horizon,
                bucket_col=bucket_col,
                condition_name=condition_label,
            )
            fp = out / f"conditioned_ic_{condition_label}_{horizon}.csv"
            result.to_csv(fp, index=False)
            saved[f"conditioned_ic_{condition_label}_{horizon}"] = fp

    n_dates = int(eligible_features.index.get_level_values("date").nunique()) if len(eligible_features) else 0
    n_assets = int(eligible_features.index.get_level_values("asset").nunique()) if len(eligible_features) else 0
    avg_assets_per_date = (
        float(eligible_features.groupby(level="date").size().mean()) if len(eligible_features) else 0.0
    )

    coverage = {
        "n_rows_loaded": n_rows_loaded,
        "n_rows_after_eligibility": int(len(eligible_features)),
        "n_dates": n_dates,
        "n_assets": n_assets,
        "avg_assets_per_date": avg_assets_per_date,
        "alpha_non_null_count": int(alpha.notna().sum()),
        "fwd_ret_5d_non_null_count": int(eligible_features["fwd_ret_5d"].notna().sum()) if "fwd_ret_5d" in eligible_features.columns else 0,
        "fwd_ret_20d_non_null_count": int(eligible_features["fwd_ret_20d"].notna().sum()) if "fwd_ret_20d" in eligible_features.columns else 0,
        "vol_20d_z_non_null_count": int(exposures["vol_20d_z"].notna().sum()) if "vol_20d_z" in exposures.columns else 0,
        "liquidity_z_non_null_count": int(exposures["liquidity_z"].notna().sum()) if "liquidity_z" in exposures.columns else 0,
        "size_z_non_null_count": int(exposures["size_z"].notna().sum()) if "size_z" in exposures.columns else 0,
    }

    coverage_fp = out / "phase14b_coverage_summary.csv"
    pd.DataFrame([coverage]).to_csv(coverage_fp, index=False)
    saved["phase14b_coverage_summary"] = coverage_fp
    return saved


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Phase 14B risk diagnostics")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--n-buckets", type=int, default=3)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    saved = run_phase14b_risk_diagnostics(
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        n_buckets=args.n_buckets,
    )
    print(saved)


if __name__ == "__main__":
    main()
