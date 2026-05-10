from __future__ import annotations

import json
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils import run_baseline_candidate_suite as suite


def _sample_features() -> pd.DataFrame:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "A"),
            (pd.Timestamp("2024-01-02"), "B"),
            (pd.Timestamp("2024-01-02"), "C"),
            (pd.Timestamp("2024-01-03"), "A"),
            (pd.Timestamp("2024-01-03"), "B"),
            (pd.Timestamp("2024-01-03"), "C"),
        ],
        names=["date", "asset"],
    )
    return pd.DataFrame(
        {
            "ret_1d": [0.01, -0.01, 0.02, -0.02, 0.01, 0.00],
            "ret_5d": [0.03, 0.01, -0.02, 0.02, -0.01, 0.00],
            "ret_20d": [0.10, 0.20, 0.05, -0.01, 0.02, 0.03],
            "fwd_ret_5d": [0.02, -0.01, 0.03, -0.01, 0.01, 0.00],
            "fwd_ret_20d": [0.04, -0.02, 0.01, 0.01, -0.01, 0.02],
        },
        index=idx,
    )


def test_runner_writes_outputs_and_includes_candidates(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(suite, "load_feature_store_frame", lambda **_: _sample_features())

    fp = suite.run_baseline_candidate_suite(
        feature_root="ignored",
        output_dir=str(tmp_path),
        quantiles=3,
        data_source_type="synthetic",
    )
    assert fp.exists()
    df = pd.read_csv(fp)

    expected = {
        "ret_1d_momentum",
        "ret_1d_reversal",
        "ret_5d_momentum",
        "ret_5d_reversal",
        "ret_20d_momentum",
        "ret_20d_reversal",
    }
    assert expected.issubset(set(df["signal_name"].unique()))
    assert set(df["horizon"].unique()).issuperset({"fwd_ret_5d", "fwd_ret_20d"})

    manifest_fp = tmp_path / "run_manifest.json"
    warnings_fp = tmp_path / "warnings.md"
    assert manifest_fp.exists()
    assert warnings_fp.exists()

    manifest = json.loads(manifest_fp.read_text(encoding="utf-8"))
    assert manifest["signal_recipe"] == "rank_based_baseline_candidates_only"
    assert manifest["data_source_type"] == "synthetic"
    assert manifest["is_synthetic"] is True
    assert manifest["research_evidence"] is False
    warning_text = warnings_fp.read_text(encoding="utf-8")
    assert "pipeline validation only" in warning_text
    assert "No warnings recorded." not in warning_text


def test_runner_records_missing_label_warning(tmp_path, monkeypatch) -> None:
    def _frame_missing_label(**_: object) -> pd.DataFrame:
        return _sample_features().drop(columns=["fwd_ret_20d"])

    monkeypatch.setattr(suite, "load_feature_store_frame", _frame_missing_label)
    suite.run_baseline_candidate_suite(feature_root="ignored", output_dir=str(tmp_path), quantiles=3)

    warnings_text = (tmp_path / "warnings.md").read_text(encoding="utf-8")
    assert "missing label column: fwd_ret_20d" in warnings_text


def test_runner_unknown_data_source_is_explicit_but_not_synthetic_warning(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(suite, "load_feature_store_frame", lambda **_: _sample_features())
    suite.run_baseline_candidate_suite(
        feature_root="ignored",
        output_dir=str(tmp_path),
        quantiles=3,
        data_source_type="unknown",
    )
    manifest = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["data_source_type"] == "unknown"
    assert manifest["is_synthetic"] is False
    assert manifest["research_evidence"] is False


def test_runner_infers_source_type_from_provenance_metadata(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(suite, "load_feature_store_frame", lambda **_: _sample_features())
    Path(tmp_path / "_feature_store_provenance.json").write_text(
        json.dumps({"data_source_type": "synthetic"}),
        encoding="utf-8",
    )
    suite.run_baseline_candidate_suite(feature_root=str(tmp_path), output_dir=str(tmp_path / "out"), quantiles=3)
    manifest = json.loads((tmp_path / "out" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["data_source_type"] == "synthetic"
    assert manifest["is_synthetic"] is True
    assert manifest["research_evidence"] is False


def test_runner_cli_real_conflict_with_synthetic_metadata_is_safe(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(suite, "load_feature_store_frame", lambda **_: _sample_features())
    Path(tmp_path / "_feature_store_provenance.json").write_text(
        json.dumps({"data_source_type": "synthetic"}),
        encoding="utf-8",
    )
    suite.run_baseline_candidate_suite(
        feature_root=str(tmp_path),
        output_dir=str(tmp_path / "out"),
        quantiles=3,
        data_source_type="real",
    )
    manifest = json.loads((tmp_path / "out" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["research_evidence"] is False
    text = (tmp_path / "out" / "warnings.md").read_text(encoding="utf-8")
    assert "Conflict detected" in text
