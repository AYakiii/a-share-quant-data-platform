from __future__ import annotations

import argparse

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils.compare_rebalance_policies_from_feature_store import (
    build_policy_configs,
    compare_policy_results,
    save_policy_comparison_outputs,
)


def test_build_policy_configs_defaults_target_10() -> None:
    args = argparse.Namespace(
        target_n=10,
        buy_rank=None,
        sell_rank=None,
        min_holding_n=None,
        max_holding_n=None,
        rebalance="weekly",
        min_trade_weight=0.003,
        max_single_weight=0.025,
        cost_bps=20.0,
        output_dir=None,
        run_name=None,
    )

    strict, buffered = build_policy_configs(args)

    assert strict.buy_rank == 10
    assert strict.sell_rank == 10
    assert strict.min_holding_n == 10
    assert strict.max_holding_n == 10
    assert strict.min_trade_weight == 0.0

    assert buffered.buy_rank == 10
    assert buffered.sell_rank == 20
    assert buffered.min_holding_n == 9
    assert buffered.max_holding_n == 12


def _fake_topn_result() -> dict:
    idx = pd.MultiIndex.from_tuples([(pd.Timestamp("2024-01-01"), "A")], names=["date", "asset"])
    fake_weights = pd.DataFrame({"target_weight": [1.0]}, index=idx)
    fake_trades = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-01")],
            "asset": ["A"],
            "prev_weight": [0.0],
            "target_weight": [1.0],
            "trade_weight": [1.0],
            "action": ["buy"],
            "reason": [""],
            "rank": [1],
            "score": [1.0],
            "is_tradable": [True],
        }
    )
    fake_daily = pd.DataFrame(
        {
            "gross_return": [0.0],
            "cost": [0.0],
            "net_return": [0.0],
            "cumulative_net_return": [0.0],
        },
        index=pd.Index([pd.Timestamp("2024-01-01")], name="date"),
    )
    fake_turnover = pd.DataFrame({"turnover": [1.0]}, index=pd.Index([pd.Timestamp("2024-01-01")], name="date"))
    return {
        "summary": {
            "total_return": 0.1,
            "annualized_return": 0.2,
            "annualized_vol": 0.15,
            "sharpe": 1.33,
            "max_drawdown": -0.05,
            "average_turnover": 0.3,
            "total_cost": 0.01,
        },
        "trades": fake_trades,
        "weights": fake_weights,
        "daily_returns": fake_daily,
        "turnover": fake_turnover,
    }


def _fake_equal_weight_result() -> dict:
    idx = pd.MultiIndex.from_tuples([(pd.Timestamp("2024-01-01"), "A")], names=["date", "asset"])
    weights = pd.DataFrame({"target_weight": [1.0]}, index=idx)
    daily = pd.DataFrame(
        {
            "gross_return": [0.0],
            "cost": [0.0],
            "net_return": [0.0],
            "cumulative_net_return": [0.0],
        },
        index=pd.Index([pd.Timestamp("2024-01-01")], name="date"),
    )
    turnover = pd.DataFrame({"turnover": [1.0]}, index=pd.Index([pd.Timestamp("2024-01-01")], name="date"))
    return {
        "summary": {
            "total_return": 0.05,
            "annualized_return": 0.1,
            "annualized_vol": 0.1,
            "sharpe": 1.0,
            "max_drawdown": -0.02,
            "average_turnover": 0.2,
            "total_cost": 0.005,
        },
        "weights": weights,
        "daily_returns": daily,
        "turnover": turnover,
    }


def test_compare_policy_results_handles_equal_weight() -> None:
    topn = _fake_topn_result()
    eq = _fake_equal_weight_result()

    out = compare_policy_results({"strict_top_n": topn, "buffered_top_n": topn, "equal_weight": eq})
    assert len(out) == 3
    row = out[out["policy"] == "equal_weight"].iloc[0]
    assert pd.isna(row["n_buy"])
    assert pd.isna(row["n_sell"])


def test_save_policy_comparison_outputs_writes_files_and_resets_index(tmp_path) -> None:
    topn = _fake_topn_result()
    eq = _fake_equal_weight_result()
    results = {"strict_top_n": topn, "buffered_top_n": topn, "equal_weight": eq}
    comparison = compare_policy_results(results)

    saved = save_policy_comparison_outputs(tmp_path, comparison, results)

    expected_keys = {
        "comparison",
        "strict_daily_returns",
        "buffered_daily_returns",
        "strict_turnover",
        "buffered_turnover",
        "strict_trades",
        "buffered_trades",
        "strict_weights",
        "buffered_weights",
        "equal_weight_daily_returns",
        "equal_weight_turnover",
        "equal_weight_weights",
    }
    assert set(saved.keys()) == expected_keys
    for p in saved.values():
        assert p.exists()

    weights_csv = pd.read_csv(saved["strict_weights"])
    assert {"date", "asset", "target_weight"}.issubset(weights_csv.columns)

    eq_weights_csv = pd.read_csv(saved["equal_weight_weights"])
    assert {"date", "asset", "target_weight"}.issubset(eq_weights_csv.columns)

    assert not (tmp_path / "equal_weight_trades.csv").exists()


def test_script_module_importable() -> None:
    import qsys.utils.compare_rebalance_policies_from_feature_store as m

    assert hasattr(m, "build_policy_configs")
    assert hasattr(m, "compare_policy_results")
    assert hasattr(m, "save_policy_comparison_outputs")
