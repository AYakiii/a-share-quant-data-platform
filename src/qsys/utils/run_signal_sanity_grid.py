"""Run signal sanity grid diagnostics using daily cross-sectional Rank IC."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd

from qsys.research.signal_quality.ic import compute_ic_by_date
from qsys.signals.engine import load_feature_store_frame
from qsys.universe.eligibility import apply_eligibility_mask, build_eligibility_mask

DEFAULT_REQUIRE_COLUMNS = (
    "ret_1d",
    "ret_5d",
    "ret_20d",
    "vol_20d",
    "amount_20d",
    "market_cap",
    "fwd_ret_5d",
    "fwd_ret_20d",
)


SIGNAL_DEFS: dict[str, tuple[str, float]] = {
    "mom_1d": ("ret_1d", 1.0),
    "mom_5d": ("ret_5d", 1.0),
    "mom_20d": ("ret_20d", 1.0),
    "rev_1d": ("ret_1d", -1.0),
    "rev_5d": ("ret_5d", -1.0),
    "rev_20d": ("ret_20d", -1.0),
    "low_vol": ("vol_20d", -1.0),
    "liquidity": ("amount_20d", 1.0),
    "size": ("market_cap", 1.0),
}


HORIZONS = ("fwd_ret_5d", "fwd_ret_20d")


def _rank_by_date(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.groupby(level="date").rank(pct=True)


def _summarize_rank_ic(ic: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(ic, errors="coerce").dropna()
    n = len(s)
    mean_ic = float(s.mean()) if n else float("nan")
    median_ic = float(s.median()) if n else float("nan")
    std_ic = float(s.std(ddof=1)) if n > 1 else float("nan")
    icir = float(mean_ic / std_ic) if n > 1 and std_ic and not math.isnan(std_ic) else float("nan")
    positive_rate = float((s > 0).mean()) if n else float("nan")
    return {
        "mean_rank_ic": mean_ic,
        "median_rank_ic": median_ic,
        "std_rank_ic": std_ic,
        "icir": icir,
        "positive_rate": positive_rate,
        "n_dates": float(n),
    }


def run_signal_sanity_grid(
    *,
    feature_root: str,
    output_dir: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Path:
    """Compute signal sanity grid and save to CSV."""

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    features = load_feature_store_frame(
        feature_root=feature_root,
        start_date=start_date,
        end_date=end_date,
    )

    eligible = build_eligibility_mask(
        features,
        require_columns=DEFAULT_REQUIRE_COLUMNS,
        require_tradable=True,
    )
    f = apply_eligibility_mask(features, eligible)

    rows: list[dict[str, float | str]] = []

    for signal_name, (source_col, direction) in SIGNAL_DEFS.items():
        raw = pd.to_numeric(f[source_col], errors="coerce") * float(direction)
        signal = _rank_by_date(raw).rename("signal")

        for horizon in HORIZONS:
            frame = pd.concat([signal, pd.to_numeric(f[horizon], errors="coerce").rename(horizon)], axis=1)

            # avg per-date assets after signal + horizon non-null
            aligned = frame.dropna(subset=["signal", horizon])
            avg_n_assets = float(aligned.groupby(level="date").size().mean()) if len(aligned) else float("nan")

            ic = compute_ic_by_date(frame, signal_col="signal", return_col=horizon, method="spearman")
            summary = _summarize_rank_ic(ic)
            rows.append(
                {
                    "signal": signal_name,
                    "horizon": horizon,
                    **summary,
                    "avg_n_assets": avg_n_assets,
                }
            )

    result = pd.DataFrame(rows)
    output_fp = out_dir / "signal_sanity_grid.csv"
    result.to_csv(output_fp, index=False)
    return output_fp


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run signal sanity grid diagnostics")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    fp = run_signal_sanity_grid(
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(fp)


if __name__ == "__main__":
    main()
