from __future__ import annotations

import json

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils.generate_synthetic_feature_store import (
    generate_synthetic_feature_frame,
    write_feature_store_partitions,
)


def test_synthetic_feature_frame_includes_baseline_suite_columns() -> None:
    df = generate_synthetic_feature_frame(periods=30, n_assets=30, seed=1)
    required = {"ret_1d", "ret_5d", "ret_20d", "fwd_ret_5d", "fwd_ret_20d"}
    assert required.issubset(df.columns)


def test_synthetic_feature_frame_default_has_nontrivial_coverage() -> None:
    df = generate_synthetic_feature_frame(seed=1)
    assert df["asset"].nunique() >= 30
    assert df["date"].nunique() >= 60


def test_write_feature_store_partitions_writes_provenance(tmp_path) -> None:
    df = generate_synthetic_feature_frame(periods=5, n_assets=3, seed=1)
    root = write_feature_store_partitions(df, feature_root=tmp_path / "sample_store")
    prov = json.loads((root / "_feature_store_provenance.json").read_text(encoding="utf-8"))
    assert prov["data_source_type"] == "synthetic"
    assert prov["is_synthetic"] is True
    assert prov["research_evidence"] is False
