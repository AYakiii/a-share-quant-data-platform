from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from qsys.utils import run_p0_raw_acquisition_wave as mod


def _fake_ingest(**kwargs):
    out_root = Path(kwargs["output_root"])
    api_names = kwargs["selected_api_names"]
    rows = []
    for api in api_names:
        status = "failed" if api == "index_hist_sw" else "success"
        rows.append({
            "source_family": kwargs["families"][0],
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
    args = mod.parse_args(["--start-date", "2026-01-05", "--end-date", "2026-01-09", "--output-root", "/tmp/p0"])
    assert args.max_workers == 2


@pytest.mark.parametrize("bad", ["/content/drive", "/content/gdrive", "/tmp/MyDrive/x", "/content/drive/MyDrive/a_share_quant_cache"])
def test_reject_drive_paths(bad: str):
    with pytest.raises(ValueError):
        mod._validate_local_output_root(bad)


def test_accept_local_path():
    mod._validate_local_output_root("/tmp/local_only")


def test_run_writes_manifest_catalog_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mod, "_run_rescue_source", _fake_rescue)
    args = Namespace(start_date="2026-01-05", end_date="2026-01-09", output_root=str(tmp_path), max_workers=2, continue_on_error=True, show_progress=False)
    out = mod.run_p0_wave(args, ingest_fn=_fake_ingest)

    manifest = json.loads(Path(out["manifest_json"]).read_text(encoding="utf-8"))
    assert manifest["stage"] == "U1-M5 Step 3"
    assert "local staging only" in manifest["note"]
    assert manifest["start_date"] == "2026-01-05"
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
