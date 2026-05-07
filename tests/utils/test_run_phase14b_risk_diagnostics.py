from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.utils.run_phase14b_risk_diagnostics import run_phase14b_risk_diagnostics


def _write_partition(root: Path, date: str, rows: list[dict]) -> None:
    part = root / f"trade_date={date}"
    part.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(part / "data.parquet", index=False)


def test_run_phase14b_risk_diagnostics_smoke(tmp_path: Path) -> None:
    feature_root = tmp_path / "feature_store"
    output_dir = tmp_path / "out"

    _write_partition(
        feature_root,
        "2025-01-01",
        [
            {
                "trade_date": "2025-01-01",
                "ts_code": "A",
                "ret_20d": 0.1,
                "vol_20d": 0.2,
                "amount_20d": 100.0,
                "market_cap": 1000.0,
                "fwd_ret_5d": 0.01,
                "fwd_ret_20d": 0.03,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-01",
                "ts_code": "B",
                "ret_20d": 0.2,
                "vol_20d": 0.4,
                "amount_20d": 120.0,
                "market_cap": 1500.0,
                "fwd_ret_5d": 0.02,
                "fwd_ret_20d": 0.04,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-01",
                "ts_code": "C",
                "ret_20d": 0.3,
                "vol_20d": 0.5,
                "amount_20d": 200.0,
                "market_cap": 2000.0,
                "fwd_ret_5d": 0.00,
                "fwd_ret_20d": 0.05,
                "is_tradable": True,
            },
        ],
    )
    _write_partition(
        feature_root,
        "2025-01-02",
        [
            {
                "trade_date": "2025-01-02",
                "ts_code": "A",
                "ret_20d": 0.2,
                "vol_20d": 0.3,
                "amount_20d": 110.0,
                "market_cap": 1050.0,
                "fwd_ret_5d": 0.01,
                "fwd_ret_20d": 0.02,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-02",
                "ts_code": "B",
                "ret_20d": 0.1,
                "vol_20d": 0.2,
                "amount_20d": 130.0,
                "market_cap": 1600.0,
                "fwd_ret_5d": -0.01,
                "fwd_ret_20d": 0.01,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-02",
                "ts_code": "C",
                "ret_20d": 0.4,
                "vol_20d": 0.6,
                "amount_20d": 210.0,
                "market_cap": 2100.0,
                "fwd_ret_5d": 0.03,
                "fwd_ret_20d": 0.06,
                "is_tradable": True,
            },
        ],
    )

    saved = run_phase14b_risk_diagnostics(
        feature_root=str(feature_root),
        output_dir=str(output_dir),
        n_buckets=3,
    )

    expected = {
        "conditioned_ic_vol_20d_z_fwd_ret_5d",
        "conditioned_ic_vol_20d_z_fwd_ret_20d",
        "conditioned_ic_liquidity_z_fwd_ret_5d",
        "conditioned_ic_liquidity_z_fwd_ret_20d",
        "conditioned_ic_size_z_fwd_ret_5d",
        "conditioned_ic_size_z_fwd_ret_20d",
        "phase14b_coverage_summary",
    }
    assert expected.issubset(set(saved.keys()))
    for k in expected:
        assert saved[k].exists()

    cov = pd.read_csv(saved["phase14b_coverage_summary"])
    assert len(cov) == 1
    assert int(cov.loc[0, "n_rows_loaded"]) == 6
