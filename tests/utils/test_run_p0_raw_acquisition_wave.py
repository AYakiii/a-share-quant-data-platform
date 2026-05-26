from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from qsys.utils import run_p0_raw_acquisition_wave as mod


def _strict_fake_ingest(
    *,
    output_root,
    families,
    start_date,
    end_date,
    max_workers,
    continue_on_error,
    selected_api_names,
    symbols,
    index_symbols,
    trade_dates,
    report_dates,
    industry_names,
    concept_names,
    universe_root,
    include_disabled,
    resume,
    ak_module,
    request_sleep=0.0,
    task_timeout_sec=None,
    task_retry_attempts=0,
    task_retry_sleep_sec=0.0,
    task_retry_backoff=1.0,
    task_retry_jitter_sec=0.0,
):
    out_root = Path(output_root)
    api_names = selected_api_names
    rows = []
    for api in api_names:
        status = "failed" if api == "index_hist_sw" else "success"
        rows.append({
            "source_family": families[0],
            "api_name": api,
            "status": status,
            "rows": 0 if status == "failed" else 2,
            "output_path": f"{out_root}/data/{api}.parquet",
            "metadata_path": f"{out_root}/data/{api}.json",
            "error_type": "RuntimeError" if status == "failed" else "",
            "error_message": "boom" if status == "failed" else "",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:01+00:00",
            "elapsed_sec": 1.0,
        })
    fp = out_root / "fake_catalog.csv"
    pd.DataFrame(rows).to_csv(fp, index=False)
    return {"catalog_csv": fp}

def _with_defaults(**overrides):
    base = dict(
        start_date="20260105",
        end_date="20260109",
        max_workers=2,
        continue_on_error=True,
        show_progress=False,
        request_sleep=0.0,
        task_timeout_sec=None,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
        symbols="",
        symbols_file="",
        index_symbols="",
        trade_dates="",
        report_dates="",
        industry_names="",
        concept_names="",
        universe_root="config/factor_sources/acquisition_universe",
        include_disabled=False,
        resume=False,
        auto_recover_failed=False,
        recovery_max_workers=1,
        recovery_request_sleep=0.5,
        recovery_task_timeout_sec=120.0,
        recovery_task_retry_attempts=2,
        recovery_task_retry_sleep_sec=1.0,
        recovery_task_retry_backoff=1.5,
        recovery_task_retry_jitter_sec=0.2,
    )
    base.update(overrides)
    return Namespace(**base)


def _fake_rescue(*_args, **_kwargs):
    return [{
        "source_group": "rescue_sources",
        "source_family": "industry",
        "source_spec": "sw_industry_membership_rescue",
        "api_name": "sw_industry_membership_rescue",
        "status": "success",
        "rows": 3,
        "output_path": "x.parquet",
        "metadata_path": "",
        "error_type": "",
        "error_message": "",
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:01+00:00",
        "elapsed_sec": 1.0,
    }]


def test_parse_args_defaults_workers():
    args = mod.parse_args(["--start-date", "20260105", "--end-date", "20260109", "--output-root", "/tmp/p0"])
    assert args.max_workers == 2


def test_default_rescue_sources_excludes_tradability():
    source_specs = mod.P0_GROUPS["rescue_sources"]["source_specs"]
    assert "sw_industry_membership_rescue" in source_specs
    assert "tradability_mask_v0" not in source_specs


@pytest.mark.parametrize("bad", ["/content/drive", "/content/gdrive", "/tmp/MyDrive/x", "/content/drive/MyDrive/a_share_quant_cache"])
def test_reject_drive_paths(bad: str):
    with pytest.raises(ValueError):
        mod._validate_local_output_root(bad)


def test_accept_local_path():
    mod._validate_local_output_root("/tmp/local_only")


def test_run_writes_manifest_catalog_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    args = _with_defaults(output_root=str(tmp_path))
    out = mod.run_p0_wave(args, ingest_fn=_strict_fake_ingest)

    manifest = json.loads(Path(out["manifest_json"]).read_text(encoding="utf-8"))
    assert manifest["stage"] == "U1-M5 Step 3"
    assert "local staging only" in manifest["note"]
    assert manifest["start_date"] == "20260105"
    assert manifest["max_workers"] == 2
    assert "started_at" in manifest and "finished_at" in manifest and "elapsed_sec" in manifest
    assert isinstance(manifest["status_counts"], dict)

    catalog = pd.read_csv(out["catalog_csv"])
    needed = {"source_group", "source_family", "api_name", "source_spec", "status", "rows", "output_path", "metadata_path", "error_type", "error_message", "started_at", "finished_at", "elapsed_sec"}
    assert needed.issubset(set(catalog.columns))
    assert (catalog["status"] == "failed").any()
    assert not (catalog["source_spec"] == "tradability_mask_v0").any()

    summary = json.loads(Path(out["summary_json"]).read_text(encoding="utf-8"))
    assert summary["total_tasks"] == len(catalog)
    assert summary["failed_count"] >= 1


def test_passes_raw_coverage_retry_params_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    captured: dict[str, object] = {}

    def _capturing_ingest(**kwargs):
        captured.update(kwargs)
        return _strict_fake_ingest(**kwargs)

    args = _with_defaults(
        output_root=str(tmp_path),
        show_progress=True,
        request_sleep=0.3,
        task_timeout_sec=15.0,
        task_retry_attempts=2,
        task_retry_sleep_sec=0.4,
        task_retry_backoff=1.5,
        task_retry_jitter_sec=0.2,
    )
    mod.run_p0_wave(args, ingest_fn=_capturing_ingest)
    assert "show_progress" not in captured
    assert captured["request_sleep"] == 0.3
    assert captured["task_timeout_sec"] == 15.0
    assert captured["task_retry_attempts"] == 2
    assert captured["task_retry_sleep_sec"] == 0.4
    assert captured["task_retry_backoff"] == 1.5
    assert captured["task_retry_jitter_sec"] == 0.2
    assert captured["ak_module"] is not None


def test_show_progress_still_used_for_rescue_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    seen: dict[str, object] = {}

    def _capture_rescue(source_name, raw_root, run_dir, start_date, end_date, max_workers, show_progress):
        seen["show_progress"] = show_progress
        return _fake_rescue()

    monkeypatch.setattr(mod, "_run_rescue_source", _capture_rescue)
    args = _with_defaults(output_root=str(tmp_path), show_progress=True)
    mod.run_p0_wave(args, ingest_fn=_strict_fake_ingest)
    assert seen["show_progress"] is True


def test_symbols_file_and_index_symbols_kept_separate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    symbols_file = tmp_path / "stock_universe_v1_symbols.txt"
    symbols_file.write_text("000001\n600000\n000001\n# comment\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def _capturing_ingest(**kwargs):
        captured.update(kwargs)
        return _strict_fake_ingest(**kwargs)

    args = _with_defaults(
        output_root=str(tmp_path / "out"),
        symbols="000002",
        symbols_file=str(symbols_file),
        index_symbols="000300,000905,000300",
        trade_dates="20260105,20260106",
        report_dates="20251231",
        industry_names="银行",
        concept_names="芯片",
        universe_root="config/factor_sources/acquisition_universe",
        include_disabled=True,
        resume=True,
    )
    mod.run_p0_wave(args, ingest_fn=_capturing_ingest)
    assert captured["symbols"] == ["000002", "000001", "600000"]
    assert captured["index_symbols"] == ["000300", "000905"]
    assert captured["trade_dates"] == ["20260105", "20260106"]
    assert captured["report_dates"] == ["20251231"]
    assert captured["industry_names"] == ["银行"]
    assert captured["concept_names"] == ["芯片"]
    assert captured["include_disabled"] is True
    assert captured["resume"] is True


def test_accepts_catalog_path_result_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)

    def _catalog_path_ingest(**kwargs):
        out_root = Path(kwargs["output_root"])
        fp = out_root / "raw_ingest_catalog.csv"
        pd.DataFrame([{
            "source_family": kwargs["families"][0],
            "api_name": kwargs["selected_api_names"][0],
            "status": "success",
            "rows": 1,
            "output_path": "x",
            "metadata_path": "y",
        }]).to_csv(fp, index=False)
        return {"catalog_path": str(fp)}

    args = _with_defaults(output_root=str(tmp_path))
    out = mod.run_p0_wave(args, ingest_fn=_catalog_path_ingest)
    assert Path(out["catalog_csv"]).exists()
    assert Path(out["summary_json"]).exists()
    assert Path(out["manifest_json"]).exists()


def test_run_rescue_source_uses_run_dir_cache_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    rescue_dir = tmp_path / "rescue_output"
    rescue_dir.mkdir(parents=True, exist_ok=True)
    inv_fp = rescue_dir / "cache_inventory.csv"
    pd.DataFrame([{
        "actual_api_name": "sw_industry_membership_rescue",
        "status": "success",
        "rows": 5,
        "path": str(rescue_dir / "x.parquet"),
        "elapsed_seconds": 1.2,
    }]).to_csv(inv_fp, index=False)

    class _FakeRunner:
        def __init__(self, **_kwargs):
            pass

        def run(self, **_kwargs):
            return {"run_dir": str(rescue_dir)}

    monkeypatch.setattr(mod, "RawWarehouseRunner", _FakeRunner)
    monkeypatch.setattr(mod, "get_source_spec", lambda _x: object())
    rows = mod._run_rescue_source(
        "sw_industry_membership_rescue",
        raw_root=tmp_path / "raw",
        run_dir=tmp_path,
        start_date="20260105",
        end_date="20260109",
        max_workers=2,
        show_progress=False,
    )
    assert len(rows) == 1
    assert rows[0]["status"] == "success"
    assert rows[0]["rows"] == 5


def test_run_rescue_source_missing_cache_inventory_is_visible_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    rescue_dir = tmp_path / "rescue_missing"
    rescue_dir.mkdir(parents=True, exist_ok=True)

    class _FakeRunner:
        def __init__(self, **_kwargs):
            pass

        def run(self, **_kwargs):
            return {"run_dir": str(rescue_dir)}

    monkeypatch.setattr(mod, "RawWarehouseRunner", _FakeRunner)
    monkeypatch.setattr(mod, "get_source_spec", lambda _x: object())
    rows = mod._run_rescue_source(
        "tradability_mask_v0",
        raw_root=tmp_path / "raw",
        run_dir=tmp_path,
        start_date="20260105",
        end_date="20260109",
        max_workers=2,
        show_progress=True,
    )
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["error_type"] == "MissingArtifactError"
    assert "cache_inventory.csv" in rows[0]["error_message"]


def test_run_rescue_source_nan_rows_and_elapsed_are_normalized(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    rescue_dir = tmp_path / "rescue_nan"
    rescue_dir.mkdir(parents=True, exist_ok=True)
    inv_fp = rescue_dir / "cache_inventory.csv"
    pd.DataFrame([
        {
            "actual_api_name": "sw_industry_membership_rescue",
            "status": "failed",
            "rows": float("nan"),
            "path": "",
            "error_message": "upstream failure",
            "elapsed_seconds": float("nan"),
        },
        {
            "actual_api_name": "sw_industry_membership_rescue",
            "status": "skipped",
            "rows": "",
            "path": "",
            "error_message": "already exists",
            "elapsed_seconds": "",
        },
    ]).to_csv(inv_fp, index=False)

    class _FakeRunner:
        def __init__(self, **_kwargs):
            pass

        def run(self, **_kwargs):
            return {"run_dir": str(rescue_dir)}

    monkeypatch.setattr(mod, "RawWarehouseRunner", _FakeRunner)
    monkeypatch.setattr(mod, "get_source_spec", lambda _x: object())
    rows = mod._run_rescue_source(
        "sw_industry_membership_rescue",
        raw_root=tmp_path / "raw",
        run_dir=tmp_path,
        start_date="20260105",
        end_date="20260109",
        max_workers=2,
        show_progress=False,
    )
    assert len(rows) == 2
    assert rows[0]["status"] == "failed"
    assert rows[0]["rows"] == 0
    assert rows[0]["elapsed_sec"] == 0.0
    assert rows[0]["error_message"] == "upstream failure"
    assert rows[1]["status"] == "skipped"
    assert rows[1]["rows"] == 0
    assert rows[1]["elapsed_sec"] == 0.0
    assert rows[1]["error_message"] == "already exists"


def test_failed_sources_ignores_nan_api_name_and_uses_source_spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def _failed_rescue(*_args, **_kwargs):
        return [{
            "source_group": "rescue_sources",
            "source_family": "trading_event",
            "source_spec": "tradability_mask_v0",
            "api_name": float("nan"),
            "status": "failed",
            "rows": 0,
            "output_path": "",
            "metadata_path": "",
            "error_type": "RuntimeError",
            "error_message": "x",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:01+00:00",
            "elapsed_sec": 1.0,
        }]

    monkeypatch.setattr(mod, "_run_rescue_source", _failed_rescue)

    def _ingest_with_nan_failed_api(**kwargs):
        out_root = Path(kwargs["output_root"])
        fp = out_root / "fake_catalog.csv"
        pd.DataFrame([{
            "source_family": kwargs["families"][0],
            "api_name": "ok_api",
            "status": "success",
            "rows": 1,
            "output_path": "",
            "metadata_path": "",
            "error_type": "",
            "error_message": "",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:01+00:00",
            "elapsed_sec": 1.0,
        }]).to_csv(fp, index=False)
        return {"catalog_csv": str(fp)}

    args = _with_defaults(output_root=str(tmp_path))
    out = mod.run_p0_wave(args, ingest_fn=_ingest_with_nan_failed_api)
    summary = json.loads(Path(out["summary_json"]).read_text(encoding="utf-8"))
    assert "nantradability_mask_v0" not in summary["failed_sources"]
    assert "tradability_mask_v0" in summary["failed_sources"]


def test_auto_recovery_success_generates_acceptance_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    calls = {"n": 0}

    def _ingest(**kwargs):
        calls["n"] += 1
        out_root = Path(kwargs["output_root"])
        fp = out_root / f"catalog_{calls['n']}.csv"
        if calls["n"] == 1:
            pd.DataFrame([{
                "source_family": kwargs["families"][0],
                "api_name": "sw_index_second_info",
                "status": "failed",
                "rows": 0,
            }]).to_csv(fp, index=False)
        else:
            pd.DataFrame([{
                "source_family": kwargs["families"][0],
                "api_name": "sw_index_second_info",
                "status": "success",
                "rows": 131,
            }]).to_csv(fp, index=False)
        return {"catalog_csv": str(fp)}

    args = _with_defaults(output_root=str(tmp_path), auto_recover_failed=True)
    out = mod.run_p0_wave(args, ingest_fn=_ingest)
    report = json.loads((Path(out["run_dir"]) / "p0_final_acceptance_report.json").read_text(encoding="utf-8"))
    assert (Path(out["run_dir"]) / "p0_recovery_catalog.csv").exists()
    assert report["main_failed_count"] == 1
    assert report["recovered_count"] == 1
    assert report["unresolved_failed_count"] == 0
    assert report["final_status"] == "accepted"
    assert "local staging only" in report["note"]


def test_auto_recovery_failure_unresolved_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    calls = {"n": 0}

    def _ingest(**kwargs):
        calls["n"] += 1
        out_root = Path(kwargs["output_root"])
        fp = out_root / f"catalog_fail_{calls['n']}.csv"
        if calls["n"] == 1:
            rows = [{
                "source_family": kwargs["families"][0],
                "api_name": "sw_index_second_info",
                "status": "failed",
                "rows": 0,
            }]
        else:
            rows = [{
                "source_family": kwargs["families"][0],
                "api_name": "ok_api",
                "status": "success",
                "rows": 1,
            }]
        pd.DataFrame(rows).to_csv(fp, index=False)
        return {"catalog_csv": str(fp)}

    args = _with_defaults(output_root=str(tmp_path), auto_recover_failed=True)
    out = mod.run_p0_wave(args, ingest_fn=_ingest)
    report = json.loads((Path(out["run_dir"]) / "p0_final_acceptance_report.json").read_text(encoding="utf-8"))
    assert report["final_status"] == "failed"
    assert report["unresolved_failed_count"] == 1


def test_auto_recovery_mixed_status_same_pair_is_unresolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    calls = {"n": 0}

    def _ingest(**kwargs):
        calls["n"] += 1
        out_root = Path(kwargs["output_root"])
        fp = out_root / f"catalog_mixed_{calls['n']}.csv"
        if calls["n"] == 1:
            rows = [{
                "source_family": kwargs["families"][0],
                "api_name": "sw_index_second_info",
                "status": "failed",
                "rows": 0,
            }]
        elif calls["n"] == 2:
            rows = [{
                "source_family": kwargs["families"][0],
                "api_name": "ok_api",
                "status": "success",
                "rows": 1,
            }]
        else:
            rows = [
                {
                    "source_family": kwargs["families"][0],
                    "api_name": "sw_index_second_info",
                    "status": "success",
                    "rows": 131,
                },
                {
                    "source_family": kwargs["families"][0],
                    "api_name": "sw_index_second_info",
                    "status": "failed",
                    "rows": 0,
                },
            ]
        pd.DataFrame(rows).to_csv(fp, index=False)
        return {"catalog_csv": str(fp)}

    args = _with_defaults(output_root=str(tmp_path), auto_recover_failed=True)
    out = mod.run_p0_wave(args, ingest_fn=_ingest)
    report = json.loads((Path(out["run_dir"]) / "p0_final_acceptance_report.json").read_text(encoding="utf-8"))
    assert report["final_status"] == "failed"
    assert report["unresolved_failed_count"] == 1
    assert any(x.get("api_name") == "sw_index_second_info" for x in report["unresolved_failed_sources"])


def test_auto_recovery_non_recoverable_main_failure_stays_unresolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def _failed_rescue(*_args, **_kwargs):
        return [{
            "source_group": "rescue_sources",
            "source_family": "industry",
            "source_spec": "sw_industry_membership_rescue",
            "api_name": "sw_industry_membership_rescue",
            "status": "failed",
            "rows": 0,
            "output_path": "",
            "metadata_path": "",
            "error_type": "RuntimeError",
            "error_message": "rescue failed",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:01+00:00",
            "elapsed_sec": 1.0,
        }]

    monkeypatch.setattr(mod, "_run_rescue_source", _failed_rescue)

    def _ingest_success(**kwargs):
        out_root = Path(kwargs["output_root"])
        fp = out_root / f"catalog_non_recoverable_{kwargs['families'][0]}.csv"
        pd.DataFrame([{
            "source_family": kwargs["families"][0],
            "api_name": kwargs["selected_api_names"][0],
            "status": "success",
            "rows": 1,
        }]).to_csv(fp, index=False)
        return {"catalog_csv": str(fp)}

    args = _with_defaults(output_root=str(tmp_path), auto_recover_failed=True)
    out = mod.run_p0_wave(args, ingest_fn=_ingest_success)
    report = json.loads((Path(out["run_dir"]) / "p0_final_acceptance_report.json").read_text(encoding="utf-8"))
    assert report["final_status"] == "failed"
    assert report["unresolved_failed_count"] == 1
    assert any(x.get("api_name") == "sw_industry_membership_rescue" for x in report["unresolved_failed_sources"])
