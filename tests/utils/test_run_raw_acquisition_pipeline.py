from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake.acquisition_profiles import get_acquisition_profile
from qsys.utils import run_raw_acquisition_pipeline as mod


def _make_run(root: Path, name: str, accepted: bool = True, forbidden_api: str = "", nan_path: bool = False, unresolved: int = 0) -> Path:
    run = root / name
    run.mkdir(parents=True)
    data = run / "data.parquet"
    meta = run / "metadata.json"
    data.write_text("x", encoding="utf-8")
    meta.write_text("{}", encoding="utf-8")
    out_path = str(data)
    if nan_path:
        out_path = out_path.replace("data.parquet", "nan/data.parquet")
    pd.DataFrame([{"source_family": "index_market", "api_name": forbidden_api or "stock_zh_index_hist_csindex", "status": "success", "rows": 3, "output_path": out_path, "metadata_path": str(meta)}]).to_csv(run / "p0_wave_catalog.csv", index=False)
    (run / "p0_wave_manifest.json").write_text("{}", encoding="utf-8")
    (run / "p0_wave_summary.json").write_text("{}", encoding="utf-8")
    (run / "p0_final_acceptance_report.json").write_text(json.dumps({"final_status": "accepted" if accepted else "failed", "unresolved_failed_count": unresolved}), encoding="utf-8")
    return run


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


def test_validate_passes_accepted_run(tmp_path: Path):
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z")
    mod.main(["validate", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path)])
    assert (run / "validation_report.json").exists()


def test_validate_fails_unresolved_final_acceptance(tmp_path: Path):
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z", accepted=False, unresolved=1)
    with pytest.raises(ValueError):
        mod.main(["validate", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path)])


def test_validate_rejects_forbidden_api(tmp_path: Path):
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z", forbidden_api="tradability_mask_v0")
    with pytest.raises(ValueError):
        mod.main(["validate", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path)])


def test_validate_detects_nan_path(tmp_path: Path):
    run = _make_run(tmp_path, "p0_wave_20260101T000000Z", nan_path=True)
    with pytest.raises(ValueError):
        mod.main(["validate", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path)])


def test_latest_selection_prefers_accepted(tmp_path: Path):
    _make_run(tmp_path, "p0_wave_20260101T000000Z", accepted=False, unresolved=1)
    run2 = _make_run(tmp_path, "p0_wave_20260102T000000Z", accepted=True)
    mod.main(["validate", "--profile", "p0", "--run-dir", "latest", "--local-root", str(tmp_path)])
    assert (run2 / "validation_report.json").exists()


def test_compact_and_promote_and_qa(tmp_path: Path):
    run = _make_run(tmp_path / "local", "p0_wave_20260101T000000Z")
    compact_root = tmp_path / "compact"
    mod.main(["compact", "--profile", "p0", "--run-dir", str(run), "--local-root", str(tmp_path / "local"), "--compact-root", str(compact_root)])
    c = pd.read_csv(compact_root / "compact_catalog.csv")
    assert int(c["rows"].sum()) == 3
    assert not c["output_path"].astype(str).str.contains("/nan/", case=False).any()

    drive = tmp_path / "drive"
    mod.main(["promote", "--profile", "p0", "--compact-root", str(compact_root), "--drive-root", str(drive), "--asset-name", "a1"])
    assert not (drive / "a1").exists()
    mod.main(["promote", "--profile", "p0", "--compact-root", str(compact_root), "--drive-root", str(drive), "--asset-name", "a1", "--promote-to-drive"])
    with pytest.raises(FileExistsError):
        mod.main(["promote", "--profile", "p0", "--compact-root", str(compact_root), "--drive-root", str(drive), "--asset-name", "a1", "--promote-to-drive"])
    mod.main(["qa", "--profile", "p0", "--drive-root", str(drive), "--asset-name", "a1", "--compact-root", str(compact_root)])


def test_qa_detects_missing_parquet(tmp_path: Path):
    asset = tmp_path / "drive" / "a1"
    asset.mkdir(parents=True)
    pd.DataFrame([{"api_name": "x", "rows": 1, "output_path": str(asset / "missing.parquet"), "metadata_path": str(asset / "m.json")}]).to_csv(asset / "compact_catalog.csv", index=False)
    with pytest.raises(ValueError):
        mod.main(["qa", "--profile", "p0", "--drive-root", str(tmp_path / "drive"), "--asset-name", "a1"])
