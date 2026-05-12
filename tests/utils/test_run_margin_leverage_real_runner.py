from __future__ import annotations

import json

import pandas as pd

from qsys.utils.run_margin_leverage_real_runner import run_margin_leverage_real_runner


def _write_panel(root) -> None:
    dates = pd.bdate_range("2025-01-02", periods=30)
    assets = ["sz000001", "sh600000"]
    for i, d in enumerate(dates):
        rows = []
        for a in assets:
            rows.append(
                {
                    "trade_date": d,
                    "ts_code": a,
                    "financing_balance": 100 + i,
                    "financing_buy_amount": 10 + i,
                    "margin_total_balance": 120 + i,
                }
            )
        p = root / f"trade_date={d.strftime('%Y-%m-%d')}"
        p.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(p / "data.parquet", index=False)


def test_run_margin_leverage_real_runner_writes_artifacts_and_warnings(tmp_path) -> None:
    panel_root = tmp_path / "panel"
    _write_panel(panel_root)
    out = run_margin_leverage_real_runner(panel_root=panel_root, output_dir=tmp_path / "out")

    for k in ["factors", "summary", "coverage", "distribution", "correlation", "high_correlation_pairs", "run_manifest", "warnings"]:
        assert out[k].exists()

    manifest = json.loads(out["run_manifest"].read_text(encoding="utf-8"))
    assert manifest["phase"] == "18A-2"
    assert manifest["factor_family"] == "margin_leverage"

    warnings_text = out["warnings"].read_text(encoding="utf-8")
    assert "Missing optional input columns:" in warnings_text
