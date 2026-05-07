from __future__ import annotations

import pandas as pd
import pytest

from qsys.research.signal_quality.conditioned_ic import (
    assign_condition_buckets,
    compute_conditioned_rank_ic,
)


def _exposures_frame() -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples(
        [
            ("2025-01-01", "A"),
            ("2025-01-01", "B"),
            ("2025-01-01", "C"),
            ("2025-01-01", "D"),
            ("2025-01-02", "A"),
            ("2025-01-02", "B"),
            ("2025-01-02", "C"),
            ("2025-01-02", "D"),
        ],
        names=["date", "asset"],
    )
    return pd.DataFrame(
        {
            "vol_20d_z": [-1.0, -0.2, 0.3, 1.2, -1.3, -0.1, 0.2, 1.5],
            "liquidity_z": [-1.1, -0.1, 0.2, 1.0, -1.4, -0.3, 0.3, 1.4],
            "size_z": [-0.9, -0.3, 0.4, 1.1, -1.0, -0.2, 0.1, 1.3],
        },
        index=idx,
    )


def _signal_frame_with_bucket() -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples(
        [
            ("2025-01-01", "A"),
            ("2025-01-01", "B"),
            ("2025-01-01", "C"),
            ("2025-01-01", "D"),
            ("2025-01-02", "A"),
            ("2025-01-02", "B"),
            ("2025-01-02", "C"),
            ("2025-01-02", "D"),
            ("2025-01-03", "A"),
            ("2025-01-03", "B"),
            ("2025-01-03", "C"),
            ("2025-01-03", "D"),
        ],
        names=["date", "asset"],
    )
    return pd.DataFrame(
        {
            "signal": [1, 2, 3, 4, 1, 2, 3, 4, 4, 3, 2, 1],
            "fwd_ret_5d": [1, 2, 4, 3, 1, 2, 4, 3, 1, 2, 3, 4],
            "vol_20d_z_bucket": [1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2],
        },
        index=idx,
    )


def test_assign_condition_buckets_requires_multiindex() -> None:
    df = pd.DataFrame({"vol_20d_z": [0.1], "liquidity_z": [0.2], "size_z": [0.3]})
    with pytest.raises(ValueError, match="MultiIndex"):
        assign_condition_buckets(df)


def test_assign_condition_buckets_missing_column_error() -> None:
    df = _exposures_frame().drop(columns=["size_z"])
    with pytest.raises(ValueError, match="missing required columns"):
        assign_condition_buckets(df)


def test_assign_condition_buckets_output_columns_same_index_and_bucket_range() -> None:
    ex = _exposures_frame()
    out = assign_condition_buckets(ex, n_buckets=3)

    assert out.index.equals(ex.index)
    assert list(out.columns) == ["vol_20d_z_bucket", "liquidity_z_bucket", "size_z_bucket"]

    for c in out.columns:
        non_na = out[c].dropna().astype(int)
        assert ((non_na >= 1) & (non_na <= 3)).all()


def test_assign_condition_buckets_too_few_assets_becomes_nan() -> None:
    idx = pd.MultiIndex.from_tuples(
        [("2025-01-01", "A"), ("2025-01-01", "B")],
        names=["date", "asset"],
    )
    ex = pd.DataFrame(
        {"vol_20d_z": [0.1, 0.2], "liquidity_z": [0.1, 0.2], "size_z": [0.1, 0.2]},
        index=idx,
    )

    out = assign_condition_buckets(ex, n_buckets=3)
    assert out.isna().all().all()


def test_compute_conditioned_rank_ic_requires_multiindex() -> None:
    df = pd.DataFrame({"signal": [1.0], "fwd_ret_5d": [0.1], "b": [1]})
    with pytest.raises(ValueError, match="MultiIndex"):
        compute_conditioned_rank_ic(df, bucket_col="b")


def test_compute_conditioned_rank_ic_missing_column_error() -> None:
    frame = _signal_frame_with_bucket().drop(columns=["signal"])
    with pytest.raises(ValueError, match="missing required columns"):
        compute_conditioned_rank_ic(frame, bucket_col="vol_20d_z_bucket")


def test_compute_conditioned_rank_ic_output_schema_and_bucket_separation() -> None:
    frame = _signal_frame_with_bucket()
    out = compute_conditioned_rank_ic(
        frame,
        bucket_col="vol_20d_z_bucket",
        condition_name="volatility",
    )

    assert list(out.columns) == [
        "condition_name",
        "bucket",
        "horizon",
        "mean_rank_ic",
        "median_rank_ic",
        "std_rank_ic",
        "icir",
        "positive_rate",
        "n_dates",
        "avg_n_assets",
    ]
    assert set(out["bucket"].tolist()) == {1, 2}
    assert (out["condition_name"] == "volatility").all()
    assert (out["horizon"] == "fwd_ret_5d").all()

    b1 = float(out.loc[out["bucket"] == 1, "mean_rank_ic"].iloc[0])
    b2 = float(out.loc[out["bucket"] == 2, "mean_rank_ic"].iloc[0])
    assert b1 != b2


def test_compute_conditioned_rank_ic_nan_handling_no_crash() -> None:
    frame = _signal_frame_with_bucket().copy()
    frame.loc[("2025-01-02", "A"), "signal"] = pd.NA
    frame.loc[("2025-01-03", "D"), "fwd_ret_5d"] = pd.NA
    frame.loc[("2025-01-01", "A"), "vol_20d_z_bucket"] = pd.NA

    out = compute_conditioned_rank_ic(frame, bucket_col="vol_20d_z_bucket")
    assert isinstance(out, pd.DataFrame)
    assert len(out) >= 1
