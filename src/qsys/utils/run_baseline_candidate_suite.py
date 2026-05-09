"""Run minimal baseline candidate signal-quality suite."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd

from qsys.reporting import write_run_manifest, write_warnings
from qsys.research.signal_quality.ic import compute_ic_by_date
from qsys.research.signal_quality.quantile import (
    assign_quantiles_by_date,
    compute_quantile_forward_returns,
    compute_quantile_spread,
)
from qsys.signals.engine import load_feature_store_frame

CANDIDATES: dict[str, tuple[str, float]] = {
    "ret_1d_momentum": ("ret_1d", 1.0),
    "ret_1d_reversal": ("ret_1d", -1.0),
    "ret_5d_momentum": ("ret_5d", 1.0),
    "ret_5d_reversal": ("ret_5d", -1.0),
    "ret_20d_momentum": ("ret_20d", 1.0),
    "ret_20d_reversal": ("ret_20d", -1.0),
}

HORIZONS = ("fwd_ret_5d", "fwd_ret_20d")


def _rank_by_date(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.groupby(level="date").rank(pct=True)


def _summarize(ic: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(ic, errors="coerce").dropna()
    n = len(s)
    mean_ic = float(s.mean()) if n else float("nan")
    median_ic = float(s.median()) if n else float("nan")
    std_ic = float(s.std(ddof=1)) if n > 1 else float("nan")
    icir = float(mean_ic / std_ic) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")
    t_stat = float(mean_ic / (std_ic / (n**0.5))) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")
    positive_rate = float((s > 0).mean()) if n else float("nan")
    return {
        "mean_rank_ic": mean_ic,
        "median_rank_ic": median_ic,
        "ic_std": std_ic,
        "icir": icir,
        "t_stat": t_stat,
        "positive_rate": positive_rate,
        "n_dates": float(n),
    }


def run_baseline_candidate_suite(
    *,
    feature_root: str,
    output_dir: str,
    start_date: str | None = None,
    end_date: str | None = None,
    quantiles: int = 5,
    min_dates_warning: int = 20,
    min_assets_warning: int = 20,
    data_source_type: str | None = None,
) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    features = load_feature_store_frame(feature_root=feature_root, start_date=start_date, end_date=end_date)

    prov_fp = Path(feature_root) / "_feature_store_provenance.json"
    metadata_source: str | None = None
    if prov_fp.exists():
        try:
            metadata_source = str(json.loads(prov_fp.read_text(encoding="utf-8")).get("data_source_type"))
        except Exception:
            metadata_source = None

    if data_source_type is None:
        data_source_type = metadata_source if metadata_source in {"synthetic", "real", "sample", "unknown"} else "unknown"

    if data_source_type not in {"synthetic", "real", "sample", "unknown"}:
        raise ValueError("data_source_type must be one of {'synthetic', 'real', 'sample', 'unknown'}")

    warnings: list[str] = []
    is_synthetic = data_source_type in {"synthetic", "sample"}
    research_evidence = data_source_type == "real"
    if data_source_type == "real" and metadata_source in {"synthetic", "sample"}:
        warnings.append(
            "Conflict detected: CLI data_source_type=real but feature-store provenance metadata indicates synthetic/sample. Treating run as non-research evidence."
        )
        is_synthetic = True
        research_evidence = False
    if is_synthetic:
        warnings.append(
            "This run uses synthetic/sample feature-store data. Results are for pipeline validation only, not tradable-alpha evidence."
        )
    available_horizons = [h for h in HORIZONS if h in features.columns]
    missing_horizons = [h for h in HORIZONS if h not in features.columns]
    for h in missing_horizons:
        warnings.append(f"missing label column: {h}")

    rows: list[dict[str, float | str]] = []

    for name, (col, sign) in CANDIDATES.items():
        if col not in features.columns:
            warnings.append(f"missing required feature column: {col} (candidate skipped: {name})")
            continue

        signal = _rank_by_date(pd.to_numeric(features[col], errors="coerce") * float(sign)).rename("signal")

        for horizon in available_horizons:
            frame = pd.concat([signal, pd.to_numeric(features[horizon], errors="coerce").rename(horizon)], axis=1)
            aligned = frame.dropna(subset=["signal", horizon]).sort_index()

            if len(aligned) == 0:
                warnings.append(f"insufficient aligned rows: {name} vs {horizon}")
                continue

            avg_assets = float(aligned.groupby(level="date").size().mean()) if len(aligned) else float("nan")
            if not math.isnan(avg_assets) and avg_assets < min_assets_warning:
                warnings.append(f"insufficient cross-sectional assets: {name} vs {horizon} avg_assets={avg_assets:.2f}")

            ic = compute_ic_by_date(aligned, signal_col="signal", return_col=horizon, method="spearman")
            ic_summary = _summarize(ic)
            if ic_summary["n_dates"] < float(min_dates_warning):
                warnings.append(f"small sample size: {name} vs {horizon} n_dates={int(ic_summary['n_dates'])}")

            quantile_spread = float("nan")
            top_minus_bottom = float("nan")
            try:
                qdf = assign_quantiles_by_date(aligned, signal_col="signal", q=quantiles)
                qret = compute_quantile_forward_returns(qdf, quantile_col="quantile", return_col=horizon)
                _, qsum = compute_quantile_spread(qret, top_quantile=quantiles, bottom_quantile=1)
                quantile_spread = float(qsum.get("mean_top_minus_universe", float("nan")))
                top_minus_bottom = float(qsum.get("mean_top_minus_bottom", float("nan")))
            except Exception:
                warnings.append(f"unavailable diagnostics: quantile spread for {name} vs {horizon}")

            rows.append(
                {
                    "signal_name": name,
                    "horizon": horizon,
                    **ic_summary,
                    "quantile_spread": quantile_spread,
                    "top_minus_bottom": top_minus_bottom,
                    "notes": "baseline candidate suite v0",
                }
            )

    report = pd.DataFrame(rows)
    report_fp = out_dir / "signal_quality_report.csv"
    report.to_csv(report_fp, index=False)

    manifest = {
        "run_id": out_dir.name,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "code_commit": None,
        "feature_root": feature_root,
        "data_range": {"start": start_date, "end": end_date},
        "universe": "from_feature_store",
        "signal_recipe": "rank_based_baseline_candidates_only",
        "portfolio_rule": None,
        "rebalance_rule": None,
        "execution_assumption": "signal_quality_only_no_execution",
        "cost_model": None,
        "benchmark": [],
        "diagnostics_requested": ["rank_ic", "quantile_spread"],
        "known_limitations": [
            "no_volatility_penalty_variants",
            "no_ml_models",
            "no_risk_optimizer",
        ],
        "warnings": warnings,
        "data_source_type": data_source_type,
        "is_synthetic": is_synthetic,
        "research_evidence": research_evidence,
    }
    write_run_manifest(out_dir, manifest)
    write_warnings(out_dir, warnings)
    return report_fp


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run baseline candidate suite")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--quantiles", type=int, default=5)
    p.add_argument(
        "--data-source-type",
        choices=["synthetic", "real", "sample", "unknown"],
        default=None,
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    fp = run_baseline_candidate_suite(
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        quantiles=args.quantiles,
        data_source_type=args.data_source_type,
    )
    print(fp)


if __name__ == "__main__":
    main()
