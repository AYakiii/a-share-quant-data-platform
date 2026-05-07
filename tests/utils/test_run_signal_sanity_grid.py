from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.utils.run_signal_sanity_grid import HORIZONS, SIGNAL_DEFS, run_signal_sanity_grid


def _write_partition(root: Path, date: str, rows: list[dict]) -> None:
    part = root / f"trade_date={date}"
    part.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(part / "data.parquet", index=False)


def test_run_signal_sanity_grid_smoke_with_partial_nans(tmp_path: Path) -> None:
    feature_root = tmp_path / "feature_store"
    out_dir = tmp_path / "out"

    _write_partition(
        feature_root,
        "2025-01-01",
        [
            {
                "trade_date": "2025-01-01",
                "ts_code": "A",
                "ret_1d": 0.01,
                "ret_5d": 0.02,
                "ret_20d": 0.03,
                "vol_20d": 0.20,
                "amount_20d": 100.0,
                "market_cap": 1000.0,
                "fwd_ret_5d": 0.01,
                "fwd_ret_20d": 0.05,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-01",
                "ts_code": "B",
                "ret_1d": -0.01,
                "ret_5d": 0.01,
                "ret_20d": 0.02,
                "vol_20d": 0.30,
                "amount_20d": 120.0,
                "market_cap": 1500.0,
                "fwd_ret_5d": -0.01,
                "fwd_ret_20d": 0.02,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-01",
                "ts_code": "C",
                "ret_1d": 0.03,
                "ret_5d": 0.04,
                "ret_20d": 0.01,
                "vol_20d": 0.25,
                "amount_20d": 130.0,
                "market_cap": 1800.0,
                "fwd_ret_5d": 0.00,
                "fwd_ret_20d": 0.01,
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
                "ret_1d": 0.02,
                "ret_5d": 0.03,
                "ret_20d": None,
                "vol_20d": 0.22,
                "amount_20d": 105.0,
                "market_cap": 1010.0,
                "fwd_ret_5d": 0.02,
                "fwd_ret_20d": 0.04,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-02",
                "ts_code": "B",
                "ret_1d": -0.02,
                "ret_5d": -0.01,
                "ret_20d": 0.01,
                "vol_20d": 0.28,
                "amount_20d": 125.0,
                "market_cap": 1520.0,
                "fwd_ret_5d": -0.02,
                "fwd_ret_20d": None,
                "is_tradable": True,
            },
            {
                "trade_date": "2025-01-02",
                "ts_code": "C",
                "ret_1d": 0.01,
                "ret_5d": 0.00,
                "ret_20d": 0.05,
                "vol_20d": 0.27,
                "amount_20d": 135.0,
                "market_cap": 1810.0,
                "fwd_ret_5d": 0.01,
                "fwd_ret_20d": 0.03,
                "is_tradable": True,
            },
        ],
    )

    fp = run_signal_sanity_grid(
        feature_root=str(feature_root),
        output_dir=str(out_dir),
    )

    assert fp.exists()
    out = pd.read_csv(fp)

    expected_cols = {
        "signal",
        "horizon",
        "mean_rank_ic",
        "median_rank_ic",
        "std_rank_ic",
        "icir",
        "positive_rate",
        "n_dates",
        "avg_n_assets",
    }
    assert expected_cols.issubset(set(out.columns))

    assert set(out["signal"].unique()) == set(SIGNAL_DEFS.keys())
    assert set(out["horizon"].unique()) == set(HORIZONS)
    assert len(out) == len(SIGNAL_DEFS) * len(HORIZONS)
