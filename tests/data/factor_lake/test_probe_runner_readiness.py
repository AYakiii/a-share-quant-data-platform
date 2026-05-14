from __future__ import annotations

import json

import pandas as pd

from qsys.data.factor_lake.runner import run_probe


class FakeAk:
    pass


def test_run_probe_no_matching_cases_does_not_crash(tmp_path):
    manifest = run_probe(FakeAk(), output_root=tmp_path, api_name="__nonexistent__")
    assert manifest["selected_cases"] == 0
    assert manifest["no_matching_cases"] is True

    cat = pd.read_csv(tmp_path / "catalogs" / "api_call_catalog.csv")
    assert "status" in cat.columns
    assert len(cat) == 0

    for name in ["failed_cases.csv", "empty_cases.csv", "missing_cases.csv", "timeout_cases.csv"]:
        df = pd.read_csv(tmp_path / "catalogs" / name)
        assert "status" in df.columns

    m = json.loads((tmp_path / "manifests" / "run_manifest.json").read_text(encoding="utf-8"))
    assert m["no_matching_cases"] is True
