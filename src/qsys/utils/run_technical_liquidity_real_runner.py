"""Real-source technical/liquidity factor runner (Phase 18A-1)."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qsys.factors.factor_diagnostics import run_basic_factor_diagnostics
from qsys.factors.factor_output import summarize_factor_output, validate_factor_output
from qsys.factors.technical_liquidity import REQUIRED_COLUMNS, build_technical_liquidity_factors
from qsys.reporting.artifacts import write_run_manifest, write_warnings
from qsys.signals.engine import load_feature_store_frame

DEFAULT_LABEL_COLS = ["fwd_ret_5d", "fwd_ret_20d"]
CANDIDATE_INPUT_COLS = [
    "close",
    "high",
    "low",
    "amount",
    "turnover",
    "volume",
    "ret_1d",
    "ret_5d",
    "ret_20d",
    "vol_20d",
    "amount_20d",
    "turnover_20d",
]


def _parse_label_cols(raw: str | None) -> list[str]:
    if raw is None:
        return DEFAULT_LABEL_COLS
    return [x.strip() for x in raw.split(",") if x.strip()]


def run_technical_liquidity_real_runner(
    feature_root: str | Path,
    output_dir: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
    run_name: str = "technical_liquidity_real_runner",
    label_cols: list[str] | None = None,
    source_panel_version: str = "feature_store_v1",
    data_source_type: str = "real",
) -> dict[str, Path]:
    """Run technical/liquidity factor diagnostics on real feature-store source."""

    labels_requested = DEFAULT_LABEL_COLS if label_cols is None else label_cols
    out_root = Path(output_dir) / run_name
    out_root.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []

    panel = load_feature_store_frame(feature_root=feature_root, start_date=start_date, end_date=end_date)
    if panel.empty:
        raise ValueError("No rows loaded from feature store for the requested date range")

    input_cols = [c for c in CANDIDATE_INPUT_COLS if c in panel.columns]
    missing_required = [c for c in REQUIRED_COLUMNS if c not in panel.columns]
    if missing_required:
        raise ValueError(f"Missing required columns for technical liquidity builder: {missing_required}")

    build_panel = panel[input_cols].copy()
    factors = build_technical_liquidity_factors(build_panel)

    msgs = validate_factor_output(factors, allow_all_nan_columns=True)
    serious = [
        m
        for m in msgs
        if "forbidden field present" in m
        or "raw input column" in m
        or "MultiIndex" in m
        or "duplicate" in m
        or "inf/-inf" in m
    ]
    if serious:
        raise ValueError("factor output validation failed: " + "; ".join(serious))

    warnings.extend([m for m in msgs if m not in serious])

    factors_fp = out_root / "factors.csv"
    summary_fp = out_root / "summary.csv"
    factors.to_csv(factors_fp, encoding="utf-8")
    summarize_factor_output(factors).to_csv(summary_fp, index=False, encoding="utf-8")

    label_columns_used = [c for c in labels_requested if c in panel.columns]
    if labels_requested and not label_columns_used:
        warnings.append(
            "Requested label columns were not found; IC diagnostics skipped. "
            f"requested={labels_requested}"
        )

    labels_df = panel[label_columns_used].copy() if label_columns_used else None
    diags = run_basic_factor_diagnostics(
        factors=factors,
        labels=labels_df,
        label_cols=label_columns_used,
        method="spearman",
        corr_threshold=0.90,
        min_assets=10,
    )

    diag_dir = out_root / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)
    coverage_fp = diag_dir / "coverage.csv"
    distribution_fp = diag_dir / "distribution.csv"
    correlation_fp = diag_dir / "correlation.csv"
    high_corr_fp = diag_dir / "high_correlation_pairs.csv"

    diags["coverage"].to_csv(coverage_fp, index=False)
    diags["distribution"].to_csv(distribution_fp, index=False)
    diags["correlation"].to_csv(correlation_fp)
    diags["high_correlation_pairs"].to_csv(high_corr_fp, index=False)

    result: dict[str, Path] = {
        "factors": factors_fp,
        "summary": summary_fp,
        "coverage": coverage_fp,
        "distribution": distribution_fp,
        "correlation": correlation_fp,
        "high_correlation_pairs": high_corr_fp,
    }

    if "ic_by_date" in diags and "ic_summary" in diags:
        ic_by_date_fp = diag_dir / "ic_by_date.csv"
        ic_summary_fp = diag_dir / "ic_summary.csv"
        diags["ic_by_date"].to_csv(ic_by_date_fp, index=False)
        diags["ic_summary"].to_csv(ic_summary_fp, index=False)
        result["ic_by_date"] = ic_by_date_fp
        result["ic_summary"] = ic_summary_fp

    n_dates = int(panel.index.get_level_values("date").nunique())
    n_assets = int(panel.index.get_level_values("asset").nunique())
    manifest = {
        "run_name": run_name,
        "phase": "18A-1",
        "factor_family": "technical_liquidity",
        "builder": "build_technical_liquidity_factors",
        "data_source_type": data_source_type,
        "source_panel_version": source_panel_version,
        "feature_root": str(feature_root),
        "output_dir": str(out_root),
        "start_date": start_date,
        "end_date": end_date,
        "input_columns": input_cols,
        "factor_columns": [str(c) for c in factors.columns],
        "label_columns_used": label_columns_used,
        "n_dates": n_dates,
        "n_assets": n_assets,
        "n_rows": int(len(panel)),
        "important_assumptions": [
            "Input panel uses MultiIndex [date, asset] loaded from feature store partitions.",
            "Technical liquidity factors require close/high/low/amount/turnover columns.",
            "Diagnostics are descriptive only and do not imply tradability.",
        ],
        "warning": "This run is factor diagnostics only, not alpha evidence and not a tradable strategy.",
        "warnings": warnings,
    }
    manifest_fp = write_run_manifest(out_root, manifest)
    warnings_fp = write_warnings(out_root, warnings)

    result["run_manifest"] = manifest_fp
    result["warnings"] = warnings_fp
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run real-source technical/liquidity factor diagnostics")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--run-name", default="technical_liquidity_real_runner")
    p.add_argument("--label-cols", default=",".join(DEFAULT_LABEL_COLS))
    p.add_argument("--source-panel-version", default="feature_store_v1")
    p.add_argument("--data-source-type", default="real")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = run_technical_liquidity_real_runner(
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        run_name=args.run_name,
        label_cols=_parse_label_cols(args.label_cols),
        source_panel_version=args.source_panel_version,
        data_source_type=args.data_source_type,
    )
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()
