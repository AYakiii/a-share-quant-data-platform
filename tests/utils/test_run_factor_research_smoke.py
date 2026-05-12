from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

from qsys.utils.run_factor_research_smoke import (
    generate_synthetic_labels,
    generate_synthetic_ohlcv_panel,
    run_factor_research_smoke,
)


def test_synthetic_panel_has_multiindex_and_required_columns() -> None:
    panel = generate_synthetic_ohlcv_panel(n_assets=5, n_dates=30, seed=7)
    assert isinstance(panel.index, pd.MultiIndex)
    assert list(panel.index.names) == ["date", "asset"]
    required = {"close", "high", "low", "amount", "turnover"}
    assert required.issubset(panel.columns)
    assert (panel["close"] > 0).all()
    assert (panel["amount"] > 0).all()
    assert (panel["turnover"] > 0).all()


def test_synthetic_labels_include_required_columns() -> None:
    panel = generate_synthetic_ohlcv_panel(n_assets=5, n_dates=40, seed=8)
    labels = generate_synthetic_labels(panel)
    assert isinstance(labels.index, pd.MultiIndex)
    assert {"fwd_ret_5d", "fwd_ret_20d"}.issubset(labels.columns)


def test_run_factor_research_smoke_writes_artifacts(tmp_path) -> None:
    out = run_factor_research_smoke(output_dir=tmp_path, n_assets=8, n_dates=70, seed=9)
    expected = {
        "factors", "factor_metadata", "factor_summary", "coverage", "distribution", "correlation",
        "high_correlation_pairs", "ic_by_date", "ic_summary", "run_manifest",
    }
    assert expected.issubset(out.keys())
    for k in expected:
        assert out[k].exists()

    manifest = json.loads(out["run_manifest"].read_text(encoding="utf-8"))
    assert manifest["data_source_type"] == "synthetic"
    assert "not alpha evidence" in manifest["warning"]

    factors = pd.read_csv(out["factors"])
    assert "ret_5d" in factors.columns

    for k in ["coverage", "distribution", "correlation", "ic_by_date", "ic_summary"]:
        df = pd.read_csv(out[k])
        assert len(df) > 0


def test_cli_smoke_runs(tmp_path) -> None:
    cmd = [
        sys.executable,
        "-m",
        "qsys.utils.run_factor_research_smoke",
        "--output-dir",
        str(tmp_path),
        "--n-assets",
        "6",
        "--n-dates",
        "60",
        "--seed",
        "11",
    ]
    env = {**__import__("os").environ, "PYTHONPATH": str(REPO_ROOT / "src")}
    cp = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
