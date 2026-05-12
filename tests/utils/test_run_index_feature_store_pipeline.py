from __future__ import annotations

import json
from pathlib import Path

from qsys.utils import run_index_feature_store_pipeline as mod


def test_pipeline_writes_symbols_manifest_warnings_without_factor_run(tmp_path, monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_sample(index_list, n, seed):
        calls["sample"] = {"index_list": index_list, "n": n, "seed": seed}
        return ["sz000001", "sh600000"], None

    def fake_build_feature_store(**kwargs):
        calls["build_fs"] = kwargs
        root = Path(kwargs["feature_root"])
        root.mkdir(parents=True, exist_ok=True)
        return root

    def fail_fetch_index_components(_index_code):
        raise AssertionError("all-assets path should not be called in this test")

    def fail_factor_runner(**_kwargs):
        raise AssertionError("factor runner should not be called unless enabled")

    monkeypatch.setattr(mod, "build_universe_sample", fake_sample)
    monkeypatch.setattr(mod, "build_real_feature_store", fake_build_feature_store)
    monkeypatch.setattr(mod, "fetch_index_components", fail_fetch_index_components)
    monkeypatch.setattr(mod, "run_technical_liquidity_real_runner", fail_factor_runner)

    out = mod.run_index_feature_store_pipeline(
        index_code="000905",
        n_assets=2,
        start_date="2025-01-01",
        end_date="2025-03-31",
        feature_root=tmp_path / "feature_store",
        output_dir=tmp_path / "pipeline_out",
        factor_output_dir=tmp_path / "factor_out",
        run_name="pipe1",
    )

    assert out["symbols"].exists()
    assert out["pipeline_manifest"].exists()
    assert out["warnings"].exists()

    symbols = out["symbols"].read_text(encoding="utf-8").strip().splitlines()
    assert symbols == ["sz000001", "sh600000"]

    manifest = json.loads(out["pipeline_manifest"].read_text(encoding="utf-8"))
    assert manifest["index_code"] == "000905"
    assert manifest["n_selected_symbols"] == 2
    assert manifest["run_technical_liquidity"] is False
    assert "not alpha evidence" in manifest["note"]

    assert calls["build_fs"]["symbols"] == ["sz000001", "sh600000"]
    assert calls["build_fs"]["start_date"] == "2025-01-01"


def test_pipeline_runs_factor_runner_only_when_enabled(tmp_path, monkeypatch) -> None:
    called = {"factor": 0, "sample": 0}

    def fake_sample(index_list, n, seed):
        called["sample"] += 1
        return ["sz000001"], None

    def fake_build_feature_store(**kwargs):
        root = Path(kwargs["feature_root"])
        root.mkdir(parents=True, exist_ok=True)
        return root

    def fake_factor_runner(**kwargs):
        called["factor"] += 1
        out = Path(kwargs["output_dir"]) / kwargs["run_name"]
        out.mkdir(parents=True, exist_ok=True)
        f = out / "factors.csv"
        f.write_text("", encoding="utf-8")
        return {"factors": f}

    monkeypatch.setattr(mod, "build_universe_sample", fake_sample)
    monkeypatch.setattr(mod, "build_real_feature_store", fake_build_feature_store)
    monkeypatch.setattr(mod, "run_technical_liquidity_real_runner", fake_factor_runner)

    mod.run_index_feature_store_pipeline(
        index_code="000905",
        n_assets=1,
        start_date="2025-01-01",
        end_date="2025-01-31",
        run_technical_liquidity=True,
        feature_root=tmp_path / "feature_store",
        output_dir=tmp_path / "pipeline_out",
        factor_output_dir=tmp_path / "factor_out",
        run_name="pipe2",
    )

    assert called["sample"] == 1
    assert called["factor"] == 1
