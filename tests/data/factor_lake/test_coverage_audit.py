from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.coverage_audit import build_backfill_wave_plan, build_health_matrix, run_coverage_audit


def _mk(status: str, rows: int, err: str = "", api: str = "api") -> dict:
    return {"source_family": "f", "api_name": api, "dataset_name": "d", "status": status, "rows": rows, "error_type": "", "error_message": err, "output_path": "", "metadata_path": ""}


def test_health_classification_rules():
    df = pd.DataFrame([
        _mk("success", 10, api="a1"),
        _mk("empty", 0, api="a2"),
        _mk("failed", 0, "Response ended prematurely", api="a3"),
        _mk("failed", 0, "'AI_PC'", api="a4"),
        _mk("failed", 0, "'NoneType' object is not subscriptable", api="a5"),
        _mk("missing", 0, "missing adapter", api="a6"),
        _mk("failed", 0, "other unknown", api="a7"),
    ])
    h = build_health_matrix(df)
    m = {r.api_name: (r.health_class, r.backfill_wave) for r in h.itertuples()}
    assert m["a1"] == ("ready", "Wave 1")
    assert m["a2"] == ("empty_check_later", "Wave 2")
    assert m["a3"] == ("unstable_retry_needed", "Wave 2")
    assert m["a4"] == ("parameter_value_review", "Wave 3")
    assert m["a5"] == ("adapter_defensive_fix_needed", "Wave 3")
    assert m["a6"] == ("pending_adapter", "Wave 3")
    assert m["a7"] == ("manual_review_needed", "Wave 3")


def test_cli_outputs_files(tmp_path):
    catalog = pd.DataFrame([_mk("success", 5, api="x1"), _mk("failed", 0, "missing function", api="x2")])
    catalog.to_csv(tmp_path / "raw_ingest_catalog.csv", index=False)
    pd.DataFrame({"source_family": ["f"], "status": ["success"], "size": [1]}).to_csv(tmp_path / "raw_ingest_summary.csv", index=False)

    out = run_coverage_audit(tmp_path, tmp_path)
    assert (tmp_path / "raw_source_health_matrix.csv").exists()
    assert (tmp_path / "raw_backfill_wave_plan.csv").exists()
    assert out["health_matrix_path"].endswith("raw_source_health_matrix.csv")

    health = pd.read_csv(tmp_path / "raw_source_health_matrix.csv")
    wave = build_backfill_wave_plan(health)
    assert "priority_rank" in wave.columns
