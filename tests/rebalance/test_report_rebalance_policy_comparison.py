from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("matplotlib")

from qsys.utils.report_rebalance_policy_comparison import (
    build_policy_diff_metrics,
    build_summary_metrics,
    generate_report,
    load_comparison_outputs,
    build_market_benchmark_metrics,
)


def _write_minimal_outputs(tmp_path, include_equal: bool = True) -> None:
    comparison = pd.DataFrame(
        [
            {
                "policy": "buffered_top_n",
                "total_return": 0.12,
                "annualized_return": 0.20,
                "annualized_vol": 0.10,
                "sharpe": 2.0,
                "max_drawdown": -0.08,
                "average_turnover": 0.30,
                "total_cost": 0.010,
                "n_trades": 10,
                "n_buy": 40,
                "n_sell": 35,
                "average_holding_days": 6.0,
                "median_holding_days": 5.0,
            },
            {
                "policy": "strict_top_n",
                "total_return": 0.10,
                "annualized_return": 0.17,
                "annualized_vol": 0.11,
                "sharpe": 1.5,
                "max_drawdown": -0.09,
                "average_turnover": 0.50,
                "total_cost": 0.020,
                "n_trades": 12,
                "n_buy": 50,
                "n_sell": 45,
                "average_holding_days": 4.0,
                "median_holding_days": 3.0,
            },
        ]
    )
    if include_equal:
        comparison = pd.concat(
            [
                comparison,
                pd.DataFrame(
                    [
                        {
                            "policy": "equal_weight",
                            "total_return": 0.08,
                            "annualized_return": 0.12,
                            "annualized_vol": 0.09,
                            "sharpe": 1.3,
                            "max_drawdown": -0.07,
                            "average_turnover": 0.10,
                            "total_cost": 0.005,
                            "n_trades": 6,
                            "n_buy": pd.NA,
                            "n_sell": pd.NA,
                            "average_holding_days": pd.NA,
                            "median_holding_days": pd.NA,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    comparison.to_csv(tmp_path / "comparison.csv", index=False)

    dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
    dr = pd.DataFrame({"date": dates, "gross_return": [0.0, 0.01], "cost": [0.0, 0.001], "net_return": [0.0, 0.009], "cumulative_net_return": [0.0, 0.009]})
    to = pd.DataFrame({"date": dates, "turnover": [0.0, 1.0]})

    dr.to_csv(tmp_path / "strict_daily_returns.csv", index=False)
    dr.to_csv(tmp_path / "buffered_daily_returns.csv", index=False)
    to.to_csv(tmp_path / "strict_turnover.csv", index=False)
    to.to_csv(tmp_path / "buffered_turnover.csv", index=False)

    if include_equal:
        dr.to_csv(tmp_path / "equal_weight_daily_returns.csv", index=False)
        to.to_csv(tmp_path / "equal_weight_turnover.csv", index=False)


def test_load_comparison_outputs_reads_required_and_optional(tmp_path) -> None:
    _write_minimal_outputs(tmp_path, include_equal=True)
    out = load_comparison_outputs(tmp_path)
    for k in ["comparison", "strict_daily_returns", "buffered_daily_returns", "strict_turnover", "buffered_turnover"]:
        assert k in out
    assert "equal_weight_daily_returns" in out


def test_build_summary_metrics_shape_and_values() -> None:
    cmp = pd.DataFrame(
        [
            {"policy": "buffered_top_n", "total_return": 0.12},
            {"policy": "strict_top_n", "total_return": 0.10},
            {"policy": "equal_weight", "total_return": 0.08},
        ]
    )
    summary = build_summary_metrics(cmp)
    assert {"metric", "buffered_top_n", "strict_top_n", "equal_weight"}.issubset(summary.columns)
    row = summary[summary["metric"] == "total_return"].iloc[0]
    assert row["buffered_top_n"] == pytest.approx(0.12)
    assert row["strict_top_n"] == pytest.approx(0.10)
    assert row["equal_weight"] == pytest.approx(0.08)


def test_build_policy_diff_metrics_values_and_div_zero() -> None:
    cmp = pd.DataFrame(
        [
            {"policy": "buffered_top_n", "total_return": 0.12, "sharpe": 2.0, "average_turnover": 0.3, "total_cost": 0.01, "n_buy": 40, "n_sell": 35, "max_drawdown": -0.08},
            {"policy": "strict_top_n", "total_return": 0.10, "sharpe": 1.5, "average_turnover": 0.0, "total_cost": 0.02, "n_buy": 50, "n_sell": 45, "max_drawdown": -0.09},
            {"policy": "equal_weight", "total_return": 0.08, "sharpe": 1.3, "average_turnover": 0.1, "total_cost": 0.005, "n_buy": pd.NA, "n_sell": pd.NA, "max_drawdown": -0.07},
        ]
    )
    diff = build_policy_diff_metrics(cmp).set_index("metric")
    assert diff.loc["buffered_vs_strict_total_return_diff", "value"] == pytest.approx(0.02)
    assert pd.isna(diff.loc["buffered_vs_strict_turnover_reduction", "value"])  # denom zero


def test_missing_equal_weight_row_returns_nan_equal_metrics() -> None:
    cmp = pd.DataFrame(
        [
            {"policy": "buffered_top_n", "total_return": 0.12, "sharpe": 2.0, "average_turnover": 0.3, "total_cost": 0.01, "n_buy": 40, "n_sell": 35, "max_drawdown": -0.08},
            {"policy": "strict_top_n", "total_return": 0.10, "sharpe": 1.5, "average_turnover": 0.5, "total_cost": 0.02, "n_buy": 50, "n_sell": 45, "max_drawdown": -0.09},
        ]
    )
    summary = build_summary_metrics(cmp)
    assert summary["equal_weight"].isna().all()

    diff = build_policy_diff_metrics(cmp).set_index("metric")
    assert pd.isna(diff.loc["buffered_vs_equal_weight_total_return_diff", "value"])


def test_generate_report_writes_outputs(tmp_path) -> None:
    _write_minimal_outputs(tmp_path, include_equal=True)
    saved = generate_report(tmp_path)
    assert set(saved.keys()) == {
        "summary_metrics",
        "policy_diff_metrics",
        "cumulative_net_return_plot",
        "turnover_plot",
        "run_manifest",
        "warnings",
    }
    for p in saved.values():
        assert p.exists()
        assert p.stat().st_size > 0


def test_market_benchmark_metrics_calculation() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
    outputs = {
        "buffered_daily_returns": pd.DataFrame({"date": dates, "net_return": [0.0, 0.01], "cumulative_net_return": [0.0, 0.01]}),
        "equal_weight_daily_returns": pd.DataFrame({"date": dates, "net_return": [0.0, 0.005], "cumulative_net_return": [0.0, 0.005]}),
        "csi300_daily_returns": pd.DataFrame({"date": dates, "net_return": [0.0, 0.008], "cumulative_net_return": [0.0, 0.008]}),
    }
    m = build_market_benchmark_metrics(outputs)
    assert {"policy", "total_return", "annualized_return", "annualized_vol", "sharpe", "max_drawdown"}.issubset(m.columns)


def test_generate_report_without_market_files_keeps_backward_outputs(tmp_path) -> None:
    _write_minimal_outputs(tmp_path, include_equal=False)
    saved = generate_report(tmp_path)
    assert "summary_metrics" in saved and "policy_diff_metrics" in saved
    assert "market_benchmark_metrics" not in saved


def test_generate_report_with_market_files_writes_market_outputs(tmp_path) -> None:
    _write_minimal_outputs(tmp_path, include_equal=True)
    dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
    mdf = pd.DataFrame({"date": dates, "gross_return": [0.0, 0.01], "net_return": [0.0, 0.01], "cumulative_net_return": [0.0, 0.01], "policy": ["CSI300", "CSI300"]})
    mdf.to_csv(tmp_path / "csi300_daily_returns.csv", index=False)
    mdf.assign(policy="CSI500").to_csv(tmp_path / "csi500_daily_returns.csv", index=False)
    mdf.assign(policy="SHANGHAI_COMPOSITE").to_csv(tmp_path / "shanghai_composite_daily_returns.csv", index=False)

    saved = generate_report(tmp_path)
    for k in ["market_benchmark_metrics", "buffered_excess_return_vs_benchmarks", "market_benchmark_comparison_plot"]:
        assert k in saved
        assert saved[k].exists()
        assert saved[k].stat().st_size > 0
