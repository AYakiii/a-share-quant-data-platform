from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake.acquisition_profiles import get_acquisition_profile
from qsys.utils import run_raw_acquisition_pipeline as mod


def _make_run(root: Path, name: str, rows: list[dict], acceptance: dict | None = None) -> Path:
    run = root / name
    run.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(run / "p0_wave_catalog.csv", index=False)
    (run / "p0_wave_manifest.json").write_text("{}", encoding="utf-8")
    (run / "p0_wave_summary.json").write_text("{}", encoding="utf-8")
    if acceptance is not None:
        (run / "p0_final_acceptance_report.json").write_text(json.dumps(acceptance), encoding="utf-8")
    return run


def _data_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(path)


def _meta_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")


def test_profile_loader_returns_p0():
    assert get_acquisition_profile("p0").profile_name == "p0"


def test_pull_dispatches_to_p0_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    seen = {}

    def _fake(ns):
        seen["out"] = ns
        return {"run_dir": tmp_path}

    monkeypatch.setattr(mod, "run_p0_wave", _fake)
    mod.main(["pull", "--profile", "p0", "--start-date", "20200101", "--end-date", "20200131", "--local-root", str(tmp_path)])
    assert seen["out"].start_date == "20200101"


def test_validate_rejects_failed_timeout_without_acceptance_report(tmp_path: Path):
    data = tmp_path / "x.parquet"
    meta = tmp_path / "x.json"
    _data_file(data)
    _meta_file(meta)
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z", [{"source_family": "index_market", "api_name": "stock_zh_index_hist_csindex", "status": "failed", "rows": 3, "output_path": str(data), "metadata_path": str(meta)}], acceptance=None)
    with pytest.raises(ValueError):
        mod.main(["validate", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path)])


def test_compact_preserves_partition_paths_no_overwrite(tmp_path: Path):
    run_root = tmp_path / "local"
    d1 = run_root / "p0_wave_20260101T000000Z" / "data/raw/akshare/index_market/a1/symbol=000001/data.parquet"
    d2 = run_root / "p0_wave_20260101T000000Z" / "data/raw/akshare/index_market/a1/symbol=000002/data.parquet"
    m1 = d1.with_name("metadata.json")
    m2 = d2.with_name("metadata.json")
    _data_file(d1); _data_file(d2); _meta_file(m1); _meta_file(m2)
    run = _make_run(run_root, "p0_wave_20260101T000000Z", [
        {"source_family": "index_market", "api_name": "a1", "status": "success", "rows": 3, "output_path": str(d1), "metadata_path": str(m1)},
        {"source_family": "index_market", "api_name": "a1", "status": "success", "rows": 3, "output_path": str(d2), "metadata_path": str(m2)},
    ], acceptance={"final_status": "accepted", "unresolved_failed_count": 0})

    compact = tmp_path / "compact"
    mod.main(["compact", "--profile", "p0", "--run-dir", str(run), "--local-root", str(run_root), "--compact-root", str(compact)])
    c = pd.read_csv(compact / "compact_catalog.csv")
    assert len(set(c["relative_output_path"])) == 2
    assert not c["relative_output_path"].astype(str).str.contains("/nan/", case=False).any()


def test_compact_missing_output_path_with_rows_fails(tmp_path: Path):
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z", [{"source_family": "index_market", "api_name": "a1", "status": "success", "rows": 3, "output_path": "", "metadata_path": ""}], acceptance={"final_status": "accepted", "unresolved_failed_count": 0})
    with pytest.raises(ValueError):
        mod.main(["compact", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path), "--compact-root", str(tmp_path / "c")])


def test_drive_qa_works_after_compact_removed(tmp_path: Path):
    run_root = tmp_path / "local"
    d = run_root / "p0_wave_20260101T000000Z" / "data/raw/akshare/index_market/a1/data.parquet"
    m = d.with_name("metadata.json")
    _data_file(d); _meta_file(m)
    run = _make_run(run_root, "p0_wave_20260101T000000Z", [{"source_family": "index_market", "api_name": "a1", "status": "success", "rows": 3, "output_path": str(d), "metadata_path": str(m)}], acceptance={"final_status": "accepted", "unresolved_failed_count": 0})

    compact = tmp_path / "compact"
    mod.main(["compact", "--profile", "p0", "--run-dir", str(run), "--local-root", str(run_root), "--compact-root", str(compact)])
    drive = tmp_path / "drive"
    mod.main(["promote", "--profile", "p0", "--compact-root", str(compact), "--drive-root", str(drive), "--asset-name", "a1", "--promote-to-drive"])
    assert (drive / "a1" / "promotion_report.json").exists()
    shutil.rmtree(compact)
    mod.main(["qa", "--profile", "p0", "--drive-root", str(drive), "--asset-name", "a1"])


def test_qa_detects_missing_metadata(tmp_path: Path):
    asset = tmp_path / "drive" / "a1"
    asset.mkdir(parents=True)
    data_rel = "data/raw/akshare/index_market/a1/data.parquet"
    _data_file(asset / data_rel)
    pd.DataFrame([{"api_name": "x", "rows": 1, "relative_output_path": data_rel, "relative_metadata_path": "meta/missing.json"}]).to_csv(asset / "compact_catalog.csv", index=False)
    (asset / "compact_manifest.json").write_text("{}", encoding="utf-8")
    (asset / "promotion_report.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError):
        mod.main(["qa", "--profile", "p0", "--drive-root", str(tmp_path / "drive"), "--asset-name", "a1"])
