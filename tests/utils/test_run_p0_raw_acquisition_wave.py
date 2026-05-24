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


@pytest.mark.parametrize("bad", ["/content/drive", "/content/gdrive", "/tmp/MyDrive/x", "/content/drive/MyDrive/a_share_quant_cache"])
def test_reject_drive_paths(bad: str):
    with pytest.raises(ValueError):
        mod._validate_local_output_root(bad)


def test_accept_local_path():
    mod._validate_local_output_root("/tmp/local_only")


def test_run_writes_manifest_catalog_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    args = Namespace(
        start_date="20260105",
        end_date="20260109",
        output_root=str(tmp_path),
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
    )
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

    summary = json.loads(Path(out["summary_json"]).read_text(encoding="utf-8"))
    assert summary["total_tasks"] == len(catalog)
    assert summary["failed_count"] >= 1


def test_passes_raw_coverage_retry_params_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    captured: dict[str, object] = {}

    def _capturing_ingest(**kwargs):
        captured.update(kwargs)
        return _strict_fake_ingest(**kwargs)

    args = Namespace(
        start_date="20260105",
        end_date="20260109",
        output_root=str(tmp_path),
        max_workers=2,
        continue_on_error=True,
        show_progress=True,
        request_sleep=0.3,
        task_timeout_sec=15.0,
        task_retry_attempts=2,
        task_retry_sleep_sec=0.4,
        task_retry_backoff=1.5,
        task_retry_jitter_sec=0.2,
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
    args = Namespace(
        start_date="20260105",
        end_date="20260109",
        output_root=str(tmp_path),
        max_workers=2,
        continue_on_error=True,
        show_progress=True,
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
    )
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

    args = Namespace(
        start_date="20260105",
        end_date="20260109",
        output_root=str(tmp_path / "out"),
        max_workers=2,
        continue_on_error=True,
        show_progress=False,
        request_sleep=0.0,
        task_timeout_sec=None,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
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

    args = Namespace(
        start_date="20260105",
        end_date="20260109",
        output_root=str(tmp_path),
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
    )
    out = mod.run_p0_wave(args, ingest_fn=_catalog_path_ingest)
    assert Path(out["catalog_csv"]).exists()
    assert Path(out["summary_json"]).exists()
    assert Path(out["manifest_json"]).exists()
