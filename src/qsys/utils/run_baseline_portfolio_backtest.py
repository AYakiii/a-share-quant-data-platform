"""Run portfolio-level backtests for baseline signals."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd

from qsys.backtest.simulator import BacktestConfig, run_backtest_from_signal
from qsys.rebalance.benchmarks import build_equal_weight_benchmark
from qsys.reporting import write_run_manifest, write_warnings
from qsys.signals.engine import load_feature_store_frame
from qsys.universe.index_members import apply_pit_index_universe_mask


def _rank_signal(features: pd.DataFrame, col: str, sign: float) -> pd.Series:
    s = pd.to_numeric(features[col], errors="coerce") * float(sign)
    return s.groupby(level="date").rank(pct=True).rename(col)


def _compute_summary_from_returns(returns: pd.Series) -> dict[str, float]:
    r = pd.to_numeric(returns, errors="coerce").dropna()
    r = r.sort_index()
    n = len(r)
    if n == 0:
        return {"total_return": float("nan"), "annualized_return": float("nan"), "annualized_vol": float("nan"), "sharpe": float("nan")}

    total = float((1.0 + r).prod() - 1.0)
    ann = float((1.0 + total) ** (252.0 / n) - 1.0)
    vol = float(r.std(ddof=0) * math.sqrt(252.0))
    sharpe = float(ann / vol) if vol > 0 else float("nan")
    return {"total_return": total, "annualized_return": ann, "annualized_vol": vol, "sharpe": sharpe}


def _compute_benchmark_comparison(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> dict[str, float]:
    aligned = pd.concat(
        [
            pd.to_numeric(strategy_returns, errors="coerce").rename("strategy"),
            pd.to_numeric(benchmark_returns, errors="coerce").rename("benchmark"),
        ],
        axis=1,
    ).dropna()
    if aligned.empty:
        return {"excess_return": float("nan"), "return_correlation": float("nan"), "active_return_volatility": float("nan")}

    active = aligned["strategy"] - aligned["benchmark"]
    return {
        "excess_return": float((1.0 + aligned["strategy"]).prod() - (1.0 + aligned["benchmark"]).prod()),
        "return_correlation": float(aligned["strategy"].corr(aligned["benchmark"])),
        "active_return_volatility": float(active.std(ddof=0) * math.sqrt(252.0)),
    }


def run_baseline_portfolio_backtest(
    *,
    feature_root: str,
    output_dir: str,
    top_n: int = 50,
    rebalance: str = "weekly",
    cost_bps_list: list[float] | None = None,
    include_momentum_comparison: bool = False,
    universe_root: str | None = None,
    index_name: str = "csi500",
    use_pit_universe: bool = False,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    features = load_feature_store_frame(feature_root=feature_root)
    if use_pit_universe:
        if not universe_root:
            raise ValueError("universe_root is required when use_pit_universe=True")
        features = apply_pit_index_universe_mask(features, universe_root=universe_root, index_name=index_name)
    warnings: list[str] = []

    missing = [c for c in ["ret_5d", "ret_20d"] if c not in features.columns]
    if missing:
        raise KeyError(f"required baseline feature columns missing: {missing}")

    signals: dict[str, pd.Series] = {
        "ret_20d_reversal": _rank_signal(features, "ret_20d", sign=-1.0),
        "ret_5d_reversal": _rank_signal(features, "ret_5d", sign=-1.0),
    }
    if include_momentum_comparison:
        signals["ret_20d_momentum"] = _rank_signal(features, "ret_20d", sign=1.0)

    if "ret_1d" not in features.columns:
        warnings.append("feature store missing ret_1d required for portfolio return simulation")
        raise KeyError("feature store missing ret_1d required for portfolio return simulation")

    asset_returns = pd.to_numeric(features["ret_1d"], errors="coerce").rename("ret_1d")

    costs = cost_bps_list if cost_bps_list is not None else [5.0, 10.0]
    summary_rows: list[dict[str, object]] = []
    daily_rows: list[dict[str, object]] = []
    turnover_rows: list[dict[str, object]] = []
    benchmark_rows: list[dict[str, object]] = []
    benchmark_daily_rows: list[dict[str, object]] = []

    returns_df = features[["ret_1d"]].copy()
    for signal_name, sig in signals.items():
        for cost_bps in costs:
            strategy_name = f"{signal_name}_top{top_n}_{rebalance}"
            cfg = BacktestConfig(
                top_n=int(top_n),
                long_only=True,
                rebalance=rebalance,
                transaction_cost_bps=float(cost_bps),
                slippage_bps=0.0,
            )
            res = run_backtest_from_signal(sig, asset_returns, config=cfg)
            summary = dict(res["summary"])
            ret_series = pd.to_numeric(res["returns"], errors="coerce").dropna().sort_index()
            calc = _compute_summary_from_returns(ret_series)

            summary_rows.append(
                {
                    "strategy_name": strategy_name,
                    "signal_name": signal_name,
                    "cost_bps": float(cost_bps),
                    "total_return": calc["total_return"],
                    "annualized_return": calc["annualized_return"],
                    "annualized_vol": calc["annualized_vol"],
                    "sharpe": calc["sharpe"],
                    "max_drawdown": summary.get("max_drawdown"),
                    "average_turnover": summary.get("turnover"),
                    "total_cost": float(res["cost"].sum()) if "cost" in res else None,
                    "n_rebalance_dates": int(res["turnover"].shape[0]) if "turnover" in res else None,
                    "notes": "cost model: turnover × bps",
                }
            )

            r = pd.DataFrame(
                {
                    "date": pd.to_datetime(res["returns"].index),
                    "strategy_name": strategy_name,
                    "cost_bps": float(cost_bps),
                    "strategy_return": pd.to_numeric(res["returns"], errors="coerce").values,
                    "gross_return": pd.to_numeric(res["gross_returns"].reindex(res["returns"].index), errors="coerce").values,
                    "cost": pd.to_numeric(res["cost"].reindex(res["returns"].index), errors="coerce").values,
                }
            )
            daily_rows.extend(r.to_dict(orient="records"))

            t = pd.DataFrame(
                {
                    "date": pd.to_datetime(res["turnover"].index),
                    "strategy_name": strategy_name,
                    "cost_bps": float(cost_bps),
                    "turnover": pd.to_numeric(res["turnover"], errors="coerce").values,
                }
            )
            turnover_rows.extend(t.to_dict(orient="records"))

            bench = build_equal_weight_benchmark(
                returns_df=returns_df,
                rebalance=rebalance,
                return_col="ret_1d",
                cost_bps=float(cost_bps),
            )
            bench_ret = pd.to_numeric(bench["daily_returns"]["net_return"], errors="coerce").rename("benchmark_return")
            rel = _compute_benchmark_comparison(ret_series, bench_ret)
            bench_sum = bench["summary"]
            benchmark_rows.append(
                {
                    "strategy_name": strategy_name,
                    "signal_name": signal_name,
                    "cost_bps": float(cost_bps),
                    "benchmark_name": "equal_weight",
                    "total_return": calc["total_return"],
                    "annualized_return": calc["annualized_return"],
                    "annualized_vol": calc["annualized_vol"],
                    "sharpe": calc["sharpe"],
                    "max_drawdown": summary.get("max_drawdown"),
                    "benchmark_total_return": bench_sum["total_return"],
                    "benchmark_annualized_return": bench_sum["annualized_return"],
                    "benchmark_annualized_vol": bench_sum["annualized_vol"],
                    "benchmark_sharpe": bench_sum["sharpe"],
                    "benchmark_max_drawdown": bench_sum["max_drawdown"],
                    "excess_return": rel["excess_return"],
                    "return_correlation": rel["return_correlation"],
                    "active_return_volatility": rel["active_return_volatility"],
                }
            )

            br = bench["daily_returns"].reset_index()[["date", "net_return"]].rename(columns={"net_return": "benchmark_return"})
            br["strategy_name"] = strategy_name
            br["cost_bps"] = float(cost_bps)
            br["benchmark_name"] = "equal_weight"
            benchmark_daily_rows.extend(br.to_dict(orient="records"))

    summary_df = pd.DataFrame(summary_rows)
    daily_df = pd.DataFrame(daily_rows)
    turnover_df = pd.DataFrame(turnover_rows)
    benchmark_df = pd.DataFrame(benchmark_rows)
    benchmark_daily_df = pd.DataFrame(benchmark_daily_rows)

    summary_fp = out / "portfolio_summary.csv"
    daily_fp = out / "daily_returns.csv"
    turnover_fp = out / "turnover.csv"
    benchmark_fp = out / "benchmark_comparison.csv"
    benchmark_daily_fp = out / "benchmark_daily_returns.csv"
    summary_df.to_csv(summary_fp, index=False)
    daily_df.to_csv(daily_fp, index=False)
    turnover_df.to_csv(turnover_fp, index=False)
    benchmark_df.to_csv(benchmark_fp, index=False)
    benchmark_daily_df.to_csv(benchmark_daily_fp, index=False)

    manifest = {
        "run_id": out.name,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "feature_root": feature_root,
        "signal_recipe": list(signals.keys()),
        "portfolio_rule": f"long_only_top_n_{top_n}",
        "rebalance_rule": rebalance,
        "execution_assumption": "next_close_realized_returns",
        "cost_model": "turnover_times_bps",
        "benchmark": ["optional_equal_weight"],
        "diagnostics_requested": ["portfolio_summary", "daily_returns", "turnover", "benchmark_comparison", "benchmark_daily_returns"],
        "known_limitations": [
            "portfolio-level validation only",
            "simplified turnover cost model",
            "no risk optimizer",
        ],
        "warnings": warnings,
    }
    manifest_fp = write_run_manifest(out, manifest)
    warnings_fp = write_warnings(out, warnings)

    return {
        "portfolio_summary": summary_fp,
        "daily_returns": daily_fp,
        "turnover": turnover_fp,
        "benchmark_comparison": benchmark_fp,
        "benchmark_daily_returns": benchmark_daily_fp,
        "run_manifest": manifest_fp,
        "warnings": warnings_fp,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run baseline portfolio-level backtest")
    p.add_argument("--feature-root", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--top-n", type=int, default=50)
    p.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="weekly")
    p.add_argument("--cost-bps", nargs="+", type=float, default=[5.0, 10.0])
    p.add_argument("--include-momentum-comparison", action="store_true")
    p.add_argument("--universe-root", default=None)
    p.add_argument("--index-name", default="csi500")
    p.add_argument("--use-pit-universe", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    saved = run_baseline_portfolio_backtest(
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        top_n=args.top_n,
        rebalance=args.rebalance,
        cost_bps_list=args.cost_bps,
        include_momentum_comparison=args.include_momentum_comparison,
        universe_root=args.universe_root,
        index_name=args.index_name,
        use_pit_universe=args.use_pit_universe,
    )
    print(saved)


if __name__ == "__main__":
    main()
