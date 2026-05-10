from __future__ import annotations

import json

import pytest

pytest.importorskip("pandas")

from qsys.reporting.artifacts import write_run_manifest, write_warnings


def test_write_run_manifest_creates_expected_json_fields(tmp_path) -> None:
    manifest = {
        "run_id": "run_x",
        "created_at": "2026-05-08T00:00:00Z",
        "signal_recipe": "rank(ret_20d)",
        "portfolio_rule": "top_n",
        "rebalance_rule": "weekly_period_end_available_date",
        "execution_assumption": "next_close",
        "cost_model": "turnover_times_bps",
        "benchmark": ["equal_weight"],
        "diagnostics_requested": ["rank_ic"],
        "known_limitations": ["research_level"],
        "warnings": [],
    }
    fp = write_run_manifest(tmp_path, manifest)
    assert fp.exists()

    data = json.loads(fp.read_text(encoding="utf-8"))
    expected = {
        "run_id", "created_at", "code_commit", "feature_root", "data_range", "universe",
        "signal_recipe", "portfolio_rule", "rebalance_rule", "execution_assumption", "cost_model",
        "benchmark", "diagnostics_requested", "known_limitations", "warnings",
    }
    assert expected.issubset(data.keys())
    assert data["run_id"] == "run_x"
    assert data["code_commit"] is None


def test_write_warnings_handles_empty_and_non_empty(tmp_path) -> None:
    empty_fp = write_warnings(tmp_path / "a", [])
    assert "No warnings recorded." in empty_fp.read_text(encoding="utf-8")

    non_empty_fp = write_warnings(tmp_path / "b", ["small sample", "high turnover"])
    text = non_empty_fp.read_text(encoding="utf-8")
    assert "- small sample" in text
    assert "- high turnover" in text
