from __future__ import annotations

import json

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils import run_baseline_portfolio_backtest as mod


def _sample_features() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=30, freq="B")
    assets = [f"{i:06d}.SZ" for i in range(1, 61)]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])
    rng = pd.Series(range(len(idx)), index=idx).astype(float)
    return pd.DataFrame(
        {
            "ret_1d": (rng % 11) / 1000.0,
            "ret_5d": (rng % 17) / 500.0,
            "ret_20d": (rng % 23) / 300.0,
        },
        index=idx,
    )


def test_runner_writes_outputs_and_summary_fields(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_feature_store_frame", lambda feature_root: _sample_features())

    saved = mod.run_baseline_portfolio_backtest(
        feature_root="ignored",
        output_dir=str(tmp_path),
        top_n=50,
        rebalance="weekly",
        cost_bps_list=[5.0, 10.0],
        include_momentum_comparison=False,
    )

    for key in ["portfolio_summary", "daily_returns", "turnover", "run_manifest", "warnings"]:
        assert key in saved
        assert saved[key].exists()

    summary = pd.read_csv(saved["portfolio_summary"])
    assert {"ret_20d_reversal", "ret_5d_reversal"}.issubset(set(summary["signal_name"]))
    assert {5.0, 10.0}.issubset(set(summary["cost_bps"]))

    manifest = json.loads(saved["run_manifest"].read_text(encoding="utf-8"))
    assert manifest["portfolio_rule"] == "long_only_top_n_50"


def test_runner_include_momentum_adds_signal(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_feature_store_frame", lambda feature_root: _sample_features())

    saved = mod.run_baseline_portfolio_backtest(
        feature_root="ignored",
        output_dir=str(tmp_path),
        include_momentum_comparison=True,
    )
    summary = pd.read_csv(saved["portfolio_summary"])
    assert "ret_20d_momentum" in set(summary["signal_name"])
