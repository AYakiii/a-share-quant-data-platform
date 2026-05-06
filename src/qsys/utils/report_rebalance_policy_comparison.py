"""Generate summary tables and charts from saved rebalance policy comparison outputs."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


REQUIRED_FILES = {
    "comparison": "comparison.csv",
    "strict_daily_returns": "strict_daily_returns.csv",
    "buffered_daily_returns": "buffered_daily_returns.csv",
    "strict_turnover": "strict_turnover.csv",
    "buffered_turnover": "buffered_turnover.csv",
}

OPTIONAL_FILES = {
    "equal_weight_daily_returns": "equal_weight_daily_returns.csv",
    "equal_weight_turnover": "equal_weight_turnover.csv",
    "strict_trades": "strict_trades.csv",
    "buffered_trades": "buffered_trades.csv",
    "strict_weights": "strict_weights.csv",
    "buffered_weights": "buffered_weights.csv",
    "equal_weight_weights": "equal_weight_weights.csv",
}


METRICS = [
    "total_return",
    "annualized_return",
    "annualized_vol",
    "sharpe",
    "max_drawdown",
    "average_turnover",
    "total_cost",
    "n_trades",
    "n_buy",
    "n_sell",
    "average_holding_days",
    "median_holding_days",
]


def load_comparison_outputs(output_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Load required/optional CSV outputs from a comparison run directory."""

    root = Path(output_dir)
    out: dict[str, pd.DataFrame] = {}

    for key, fn in REQUIRED_FILES.items():
        fp = root / fn
        if not fp.exists():
            raise FileNotFoundError(f"Missing required file: {fp}")
        out[key] = pd.read_csv(fp)

    for key, fn in OPTIONAL_FILES.items():
        fp = root / fn
        if fp.exists():
            out[key] = pd.read_csv(fp)

    return out


def build_summary_metrics(comparison: pd.DataFrame) -> pd.DataFrame:
    """Build long-form summary table with one row per metric."""

    if "policy" not in comparison.columns:
        raise ValueError("comparison must contain policy column")

    by_policy = comparison.set_index("policy")

    rows = []
    for m in METRICS:
        rows.append(
            {
                "metric": m,
                "buffered_top_n": by_policy.loc["buffered_top_n", m] if "buffered_top_n" in by_policy.index and m in by_policy.columns else np.nan,
                "strict_top_n": by_policy.loc["strict_top_n", m] if "strict_top_n" in by_policy.index and m in by_policy.columns else np.nan,
                "equal_weight": by_policy.loc["equal_weight", m] if "equal_weight" in by_policy.index and m in by_policy.columns else np.nan,
            }
        )

    return pd.DataFrame(rows)


def _safe_div(numer: float, denom: float) -> float:
    if pd.isna(numer) or pd.isna(denom) or denom == 0:
        return np.nan
    return float(numer) / float(denom)


def build_policy_diff_metrics(comparison: pd.DataFrame) -> pd.DataFrame:
    """Build key policy-difference metrics from comparison rows."""

    byp = comparison.set_index("policy") if "policy" in comparison.columns else pd.DataFrame()

    b = byp.loc["buffered_top_n"] if "buffered_top_n" in byp.index else pd.Series(dtype=float)
    s = byp.loc["strict_top_n"] if "strict_top_n" in byp.index else pd.Series(dtype=float)
    e = byp.loc["equal_weight"] if "equal_weight" in byp.index else pd.Series(dtype=float)

    def g(row: pd.Series, col: str) -> float:
        return float(row[col]) if col in row.index and not pd.isna(row[col]) else np.nan

    b_total, s_total, e_total = g(b, "total_return"), g(s, "total_return"), g(e, "total_return")
    b_sharpe, s_sharpe, e_sharpe = g(b, "sharpe"), g(s, "sharpe"), g(e, "sharpe")
    b_dd, e_dd = g(b, "max_drawdown"), g(e, "max_drawdown")
    b_to, s_to = g(b, "average_turnover"), g(s, "average_turnover")
    b_cost, s_cost = g(b, "total_cost"), g(s, "total_cost")
    b_buy, s_buy = g(b, "n_buy"), g(s, "n_buy")
    b_sell, s_sell = g(b, "n_sell"), g(s, "n_sell")

    rows = [
        {"metric": "buffered_vs_strict_total_return_diff", "value": b_total - s_total if not pd.isna(b_total) and not pd.isna(s_total) else np.nan},
        {"metric": "buffered_vs_strict_sharpe_diff", "value": b_sharpe - s_sharpe if not pd.isna(b_sharpe) and not pd.isna(s_sharpe) else np.nan},
        {"metric": "buffered_vs_strict_turnover_reduction", "value": _safe_div(s_to - b_to, s_to)},
        {"metric": "buffered_vs_strict_cost_reduction", "value": _safe_div(s_cost - b_cost, s_cost)},
        {"metric": "buffered_vs_strict_buy_reduction", "value": _safe_div(s_buy - b_buy, s_buy)},
        {"metric": "buffered_vs_strict_sell_reduction", "value": _safe_div(s_sell - b_sell, s_sell)},
        {"metric": "buffered_vs_equal_weight_total_return_diff", "value": b_total - e_total if not pd.isna(b_total) and not pd.isna(e_total) else np.nan},
        {"metric": "buffered_vs_equal_weight_sharpe_diff", "value": b_sharpe - e_sharpe if not pd.isna(b_sharpe) and not pd.isna(e_sharpe) else np.nan},
        {"metric": "buffered_vs_equal_weight_max_drawdown_diff", "value": b_dd - e_dd if not pd.isna(b_dd) and not pd.isna(e_dd) else np.nan},
    ]

    return pd.DataFrame(rows)


def plot_cumulative_net_return(outputs: dict[str, pd.DataFrame], output_path: str | Path) -> None:
    """Plot cumulative net return curves and save to PNG."""

    import matplotlib.pyplot as plt

    plt.figure(figsize=(8, 4))
    for key, label in [
        ("strict_daily_returns", "strict_top_n"),
        ("buffered_daily_returns", "buffered_top_n"),
        ("equal_weight_daily_returns", "equal_weight"),
    ]:
        if key not in outputs:
            continue
        df = outputs[key].copy()
        if "date" not in df.columns or "cumulative_net_return" not in df.columns:
            continue
        x = pd.to_datetime(df["date"])
        y = pd.to_numeric(df["cumulative_net_return"], errors="coerce")
        plt.plot(x, y, label=label)

    plt.title("Cumulative Net Return")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=120)
    plt.close()


def plot_turnover(outputs: dict[str, pd.DataFrame], output_path: str | Path) -> None:
    """Plot turnover time series and save to PNG."""

    import matplotlib.pyplot as plt

    plt.figure(figsize=(8, 4))
    for key, label in [
        ("strict_turnover", "strict_top_n"),
        ("buffered_turnover", "buffered_top_n"),
        ("equal_weight_turnover", "equal_weight"),
    ]:
        if key not in outputs:
            continue
        df = outputs[key].copy()
        if "date" not in df.columns or "turnover" not in df.columns:
            continue
        x = pd.to_datetime(df["date"])
        y = pd.to_numeric(df["turnover"], errors="coerce")
        plt.plot(x, y, label=label)

    plt.title("Turnover")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=120)
    plt.close()


def generate_report(output_dir: str | Path) -> dict[str, Path]:
    """Generate summary CSV/PNG report files from saved experiment outputs."""

    root = Path(output_dir)
    outputs = load_comparison_outputs(root)

    summary = build_summary_metrics(outputs["comparison"])
    diff = build_policy_diff_metrics(outputs["comparison"])

    summary_path = root / "summary_metrics.csv"
    diff_path = root / "policy_diff_metrics.csv"
    curve_path = root / "cumulative_net_return.png"
    turnover_path = root / "turnover.png"

    summary.to_csv(summary_path, index=False)
    diff.to_csv(diff_path, index=False)
    plot_cumulative_net_return(outputs, curve_path)
    plot_turnover(outputs, turnover_path)

    return {
        "summary_metrics": summary_path,
        "policy_diff_metrics": diff_path,
        "cumulative_net_return_plot": curve_path,
        "turnover_plot": turnover_path,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate report from rebalance policy comparison outputs")
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    saved = generate_report(args.output_dir)
    print("=== Rebalance Policy Comparison Report ===")
    print(saved)


if __name__ == "__main__":
    main()
