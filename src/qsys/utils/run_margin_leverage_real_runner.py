"""Real-source margin leverage factor diagnostics runner (Phase 18A-2)."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qsys.factors.factor_diagnostics import run_basic_factor_diagnostics
from qsys.factors.factor_output import summarize_factor_output, validate_factor_output, write_factor_output
from qsys.factors.margin_leverage import REQUIRED_COLUMNS, build_margin_leverage_factors
from qsys.reporting.artifacts import write_run_manifest, write_warnings

OPTIONAL_INPUT_COLUMNS = ["short_balance", "short_sell_amount", "margin_eligible", "financing_repay_amount", "short_sell_volume"]


def load_margin_panel_frame(panel_root: str | Path, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    root = Path(panel_root)
    files = sorted(root.glob("trade_date=*/data.parquet"))
    frames: list[pd.DataFrame] = []
    for fp in files:
        trade_date = fp.parent.name.split("=", 1)[1]
        if start_date and trade_date < start_date:
            continue
        if end_date and trade_date > end_date:
            continue
        df = pd.read_parquet(fp)
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame(index=pd.MultiIndex.from_arrays([[], []], names=["date", "asset"]))
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"trade_date": "date", "ts_code": "asset"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "asset"]).set_index(["date", "asset"]).sort_index()
    return out


def run_margin_leverage_real_runner(
    panel_root: str | Path = "data/processed/margin_panel/v1",
    output_dir: str | Path = "outputs/factor_research",
    run_name: str = "margin_leverage_real_runner",
    start_date: str | None = None,
    end_date: str | None = None,
    source_panel_version: str = "margin_panel_v1",
    data_source_type: str = "real",
) -> dict[str, Path]:
    out_root = Path(output_dir) / run_name
    out_root.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []

    panel = load_margin_panel_frame(panel_root=panel_root, start_date=start_date, end_date=end_date)
    if panel.empty:
        raise ValueError("No rows loaded from margin panel for requested date range")

    missing_required = [c for c in REQUIRED_COLUMNS if c not in panel.columns]
    if missing_required:
        raise ValueError(f"Missing required columns for margin leverage builder: {missing_required}")

    missing_optional = [c for c in OPTIONAL_INPUT_COLUMNS if c not in panel.columns]
    if missing_optional:
        warnings.append("Missing optional input columns: " + ", ".join(missing_optional))

    input_cols = [c for c in panel.columns if c in REQUIRED_COLUMNS or c in OPTIONAL_INPUT_COLUMNS]
    factors = build_margin_leverage_factors(panel[input_cols].copy())

    msgs = validate_factor_output(factors, allow_all_nan_columns=True)
    serious = [m for m in msgs if "forbidden field present" in m or "raw input column" in m or "MultiIndex" in m or "inf/-inf" in m]
    if serious:
        raise ValueError("factor output validation failed: " + "; ".join(serious))
    warnings.extend([m for m in msgs if m not in serious])

    factor_paths = write_factor_output(factors, output_root=out_root, dataset_name="margin_leverage_real")
    factors_fp = factor_paths["factors"]
    summary_fp = out_root / "summary.csv"
    summary = summarize_factor_output(factors)
    summary.to_csv(summary_fp, index=False)

    zero_coverage_cols = summary.loc[summary["coverage"] == 0.0, "factor_name"].astype(str).tolist()
    if zero_coverage_cols:
        warnings.append("Factor columns with zero coverage: " + ", ".join(sorted(zero_coverage_cols)))
    low = summary.loc[(summary["coverage"] > 0.0) & (summary["coverage"] < 0.2), "factor_name"].astype(str).tolist()
    if low:
        warnings.append("Factor columns with low coverage below 0.2: " + ", ".join(sorted(low)))
    inf_cols = summary.loc[summary["n_inf"] > 0, "factor_name"].astype(str).tolist()
    if inf_cols:
        warnings.append("Factor columns with inf/-inf values: " + ", ".join(sorted(inf_cols)))

    diags = run_basic_factor_diagnostics(factors)
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

    manifest = {
        "run_name": run_name,
        "phase": "18A-2",
        "factor_family": "margin_leverage",
        "builder": "build_margin_leverage_factors",
        "data_source_type": data_source_type,
        "source_panel_version": source_panel_version,
        "panel_root": str(panel_root),
        "output_dir": str(out_root),
        "start_date": start_date,
        "end_date": end_date,
        "input_columns": input_cols,
        "factor_columns": [str(c) for c in factors.columns],
        "n_dates": int(panel.index.get_level_values("date").nunique()),
        "n_assets": int(panel.index.get_level_values("asset").nunique()),
        "n_rows": int(len(panel)),
        "assumptions": [
            "Input panel is normalized to MultiIndex [date, asset] with numeric margin columns.",
            "Diagnostics are descriptive only and do not imply tradability or alpha evidence.",
        ],
        "note": "Diagnostics only, not alpha evidence, not a tradable strategy.",
        "warnings": warnings,
    }
    manifest_fp = write_run_manifest(out_root, manifest)
    warnings_fp = write_warnings(out_root, warnings)

    return {
        "factors": factors_fp,
        "summary": summary_fp,
        "coverage": coverage_fp,
        "distribution": distribution_fp,
        "correlation": correlation_fp,
        "high_correlation_pairs": high_corr_fp,
        "run_manifest": manifest_fp,
        "warnings": warnings_fp,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run real-source margin leverage factor diagnostics")
    p.add_argument("--panel-root", default="data/processed/margin_panel/v1")
    p.add_argument("--output-dir", default="outputs/factor_research")
    p.add_argument("--run-name", default="margin_leverage_real_runner")
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--source-panel-version", default="margin_panel_v1")
    p.add_argument("--data-source-type", default="real")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = run_margin_leverage_real_runner(
        panel_root=args.panel_root,
        output_dir=args.output_dir,
        run_name=args.run_name,
        start_date=args.start_date,
        end_date=args.end_date,
        source_panel_version=args.source_panel_version,
        data_source_type=args.data_source_type,
    )
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()
