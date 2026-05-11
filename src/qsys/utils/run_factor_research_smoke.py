"""Synthetic end-to-end factor research smoke pipeline (Phase 17I-17K)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from qsys.factors.factor_diagnostics import run_basic_factor_diagnostics
from qsys.factors.factor_output import validate_factor_output, write_factor_output
from qsys.factors.technical_liquidity import build_technical_liquidity_factors


def generate_synthetic_ohlcv_panel(
    n_assets: int = 20,
    n_dates: int = 90,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate deterministic synthetic OHLCV-like panel with MultiIndex [date, asset]."""

    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-01-01", periods=n_dates)
    assets = [f"A{i:04d}.SZ" for i in range(1, n_assets + 1)]

    rows = []
    for asset in assets:
        base = rng.uniform(20.0, 120.0)
        noise = rng.normal(0.0005, 0.02, size=n_dates)
        close = base * np.cumprod(1.0 + noise)
        close = np.maximum(close, 0.01)
        open_ = close * (1.0 + rng.normal(0.0, 0.005, size=n_dates))
        high = np.maximum(open_, close) * (1.0 + np.abs(rng.normal(0.0, 0.01, size=n_dates)))
        low = np.minimum(open_, close) * (1.0 - np.abs(rng.normal(0.0, 0.01, size=n_dates)))
        low = np.maximum(low, 0.001)
        volume = rng.lognormal(mean=11.0, sigma=0.4, size=n_dates)
        amount = np.maximum(close * volume, 1.0)
        turnover = np.maximum(rng.lognormal(mean=-3.6, sigma=0.35, size=n_dates), 1e-6)

        df = pd.DataFrame(
            {
                "date": dates,
                "asset": asset,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
                "turnover": turnover,
            }
        )
        rows.append(df)

    out = pd.concat(rows, ignore_index=True).set_index(["date", "asset"]).sort_index()
    return out


def generate_synthetic_labels(
    panel: pd.DataFrame,
    horizons: list[int] = [5, 20],
) -> pd.DataFrame:
    """Generate forward-return labels from close (labels only)."""

    if not isinstance(panel.index, pd.MultiIndex) or list(panel.index.names) != ["date", "asset"]:
        raise ValueError("panel must have MultiIndex ['date', 'asset']")
    if "close" not in panel.columns:
        raise ValueError("panel must contain close column")

    close = pd.to_numeric(panel["close"], errors="coerce")
    out = pd.DataFrame(index=panel.index)
    for h in horizons:
        out[f"fwd_ret_{h}d"] = close.groupby(level="asset", sort=False).shift(-h) / close - 1.0
    return out


def run_factor_research_smoke(
    output_dir: str | Path,
    n_assets: int = 20,
    n_dates: int = 90,
    seed: int = 42,
) -> dict[str, Path]:
    """Run synthetic end-to-end factor research smoke pipeline and write artifacts."""

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    panel = generate_synthetic_ohlcv_panel(n_assets=n_assets, n_dates=n_dates, seed=seed)
    labels = generate_synthetic_labels(panel, horizons=[5, 20])

    factors = build_technical_liquidity_factors(panel)
    msgs = validate_factor_output(factors, allow_all_nan_columns=True)
    serious = [m for m in msgs if "forbidden field present" in m or "raw input column" in m or "MultiIndex" in m or "inf/-inf" in m]
    if serious:
        raise ValueError("factor output validation failed: " + "; ".join(serious))

    factor_paths = write_factor_output(factors, output_root=out_root, dataset_name="factor_research_smoke")
    diags = run_basic_factor_diagnostics(
        factors,
        labels=labels,
        label_cols=["fwd_ret_5d", "fwd_ret_20d"],
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
    ic_by_date_fp = diag_dir / "ic_by_date.csv"
    ic_summary_fp = diag_dir / "ic_summary.csv"

    diags["coverage"].to_csv(coverage_fp, index=False)
    diags["distribution"].to_csv(distribution_fp, index=False)
    diags["correlation"].to_csv(correlation_fp)
    diags["high_correlation_pairs"].to_csv(high_corr_fp, index=False)
    diags["ic_by_date"].to_csv(ic_by_date_fp, index=False)
    diags["ic_summary"].to_csv(ic_summary_fp, index=False)

    manifest = {
        "run_name": "factor_research_smoke",
        "data_source_type": "synthetic",
        "n_assets": int(n_assets),
        "n_dates": int(n_dates),
        "seed": int(seed),
        "factor_builder": "technical_liquidity",
        "diagnostics": "basic_factor_diagnostics",
        "labels": ["fwd_ret_5d", "fwd_ret_20d"],
        "warning": "synthetic pipeline only; not alpha evidence; not tradable strategy",
    }
    manifest_fp = out_root / "run_manifest.json"
    manifest_fp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "factors": factor_paths["factors"],
        "factor_metadata": factor_paths["metadata"],
        "factor_summary": factor_paths["summary"],
        "coverage": coverage_fp,
        "distribution": distribution_fp,
        "correlation": correlation_fp,
        "high_correlation_pairs": high_corr_fp,
        "ic_by_date": ic_by_date_fp,
        "ic_summary": ic_summary_fp,
        "run_manifest": manifest_fp,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run synthetic factor research smoke pipeline")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--n-assets", type=int, default=20)
    p.add_argument("--n-dates", type=int, default=90)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = run_factor_research_smoke(
        output_dir=args.output_dir,
        n_assets=args.n_assets,
        n_dates=args.n_dates,
        seed=args.seed,
    )
    print(out)


if __name__ == "__main__":
    main()
