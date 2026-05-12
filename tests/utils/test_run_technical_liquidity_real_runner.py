from __future__ import annotations

import json
import os
import subprocess
import sys

import pandas as pd

from qsys.utils.run_technical_liquidity_real_runner import run_technical_liquidity_real_runner


def _write_feature_store(root, include_labels: bool = True) -> None:
    dates = pd.bdate_range("2025-01-02", periods=70)
    assets = ["000001.SZ", "000002.SZ", "000004.SZ"]
    for d_idx, d in enumerate(dates):
        rows = []
        for a_idx, asset in enumerate(assets):
            base = 10.0 + a_idx * 2 + d_idx * 0.1
            close = base
            row = {
                "trade_date": d,
                "ts_code": asset,
                "close": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "amount": 1_000_000 + d_idx * 1000 + a_idx * 100,
                "turnover": 0.01 + 0.001 * a_idx,
                "volume": 50000 + d_idx * 10,
            }
            if include_labels:
                row["fwd_ret_5d"] = 0.001 * (a_idx + 1)
                row["fwd_ret_20d"] = 0.002 * (a_idx + 1)
            rows.append(row)

        part_dir = root / f"trade_date={d.strftime('%Y-%m-%d')}"
        part_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(part_dir / "data.parquet", index=False)


def test_real_runner_writes_expected_artifacts(tmp_path) -> None:
    feature_root = tmp_path / "feature_store"
    _write_feature_store(feature_root, include_labels=True)

    out = run_technical_liquidity_real_runner(
        feature_root=feature_root,
        output_dir=tmp_path / "artifacts",
    )

    expected = {
        "factors",
        "summary",
        "coverage",
        "distribution",
        "correlation",
        "high_correlation_pairs",
        "ic_by_date",
        "ic_summary",
        "run_manifest",
        "warnings",
    }
    assert expected.issubset(out.keys())
    for k in expected:
        assert out[k].exists()

    factors = pd.read_csv(out["factors"])
    assert "fwd_ret_5d" not in factors.columns
    assert "fwd_ret_20d" not in factors.columns

    manifest = json.loads(out["run_manifest"].read_text(encoding="utf-8"))
    assert manifest["phase"] == "18A-1"
    assert manifest["factor_family"] == "technical_liquidity"
    assert "not alpha evidence" in manifest["warning"]


def test_real_runner_without_labels_skips_ic_and_warns(tmp_path) -> None:
    feature_root = tmp_path / "feature_store"
    _write_feature_store(feature_root, include_labels=False)

    out = run_technical_liquidity_real_runner(
        feature_root=feature_root,
        output_dir=tmp_path / "artifacts",
        label_cols=["fwd_ret_5d", "fwd_ret_20d"],
    )

    assert out["coverage"].exists()
    assert out["distribution"].exists()
    assert out["correlation"].exists()
    assert "ic_by_date" not in out
    assert "ic_summary" not in out

    warnings_text = out["warnings"].read_text(encoding="utf-8")
    assert "IC diagnostics skipped" in warnings_text


def test_cli_real_runner_runs(tmp_path) -> None:
    feature_root = tmp_path / "feature_store"
    _write_feature_store(feature_root, include_labels=True)

    cmd = [
        sys.executable,
        "-m",
        "qsys.utils.run_technical_liquidity_real_runner",
        "--feature-root",
        str(feature_root),
        "--output-dir",
        str(tmp_path / "out"),
        "--run-name",
        "phase18a1",
    ]
    env = {**os.environ, "PYTHONPATH": "src"}
    cp = subprocess.run(cmd, cwd="/workspace/a-share-quant-data-platform", env=env, capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
