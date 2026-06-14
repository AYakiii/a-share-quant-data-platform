from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qsys.utils import run_akshare_raw_ingest as cli


def test_parse_symbols_dedup_preserve_order():
    symbols = cli._parse_symbols("000001,000002,000001,000003,000002")
    assert symbols == ["000001", "000002", "000003"]


def test_build_symbol_batches_846_size120():
    symbols = [f"{i:06d}" for i in range(846)]
    batches = cli._build_symbol_batches(symbols, 120)
    assert len(batches) == 8
    assert batches[0][0] == 0 and batches[0][1] == 119
    assert batches[1][0] == 120 and batches[1][1] == 239
    assert batches[-1][0] == 840 and batches[-1][1] == 845


def test_batch_label_generation():
    assert cli._batch_label(0, 119) == "batch0000_0119"
    assert cli._batch_label(120, 239) == "batch0120_0239"


def test_catalog_path_rewrite_and_path_cleanliness(tmp_path):
    output_root = tmp_path / "out"
    batch_root = output_root / "_batches" / "batch0000_0119"
    src = batch_root / "data" / "raw" / "akshare" / "market_price" / "stock_zh_a_hist" / "symbol=000001" / "adjust=qfq" / "year=2022" / "month=01"
    src.mkdir(parents=True)
    (src / "data.parquet").write_text("x", encoding="utf-8")
    (src / "metadata.json").write_text("{}", encoding="utf-8")

    p = cli._rewrite_to_master_path(str(src / "data.parquet"), batch_root, output_root)
    assert "_batches" not in p
    assert "batch0000_0119" not in p
    assert "batch_id=" not in p
    assert "batch_label" not in p
    assert "pool_id=" not in p


def test_merge_conflict_detection_no_overwrite(tmp_path):
    out = tmp_path / "out"
    batch1 = out / "_batches" / "batch0000_0119"
    batch2 = out / "_batches" / "batch0120_0239"
    rel = Path("data/raw/akshare/market_price/stock_zh_a_hist/symbol=000001/adjust=qfq/year=2022/month=01/data.parquet")

    p1 = batch1 / rel
    p2 = batch2 / rel
    p1.parent.mkdir(parents=True, exist_ok=True)
    p2.parent.mkdir(parents=True, exist_ok=True)
    p1.write_text("batch1", encoding="utf-8")
    p2.write_text("batch2", encoding="utf-8")

    conflicts: list[dict[str, str]] = []
    merged1, c1 = cli._merge_batch_raw(batch1, out, "batch0000_0119", conflicts)
    merged2, c2 = cli._merge_batch_raw(batch2, out, "batch0120_0239", conflicts)

    assert merged1 == 1 and c1 == 0
    assert merged2 == 0 and c2 == 1
    dst = out / rel
    assert dst.read_text(encoding="utf-8") == "batch1"
    assert len(conflicts) == 1


def test_timeout_reporting_and_stop_on_timeout(tmp_path, monkeypatch):
    class _Proc:
        def __init__(self, returncode: int):
            self.returncode = returncode

    call_count = {"n": 0}

    def fake_run(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise cli.subprocess.TimeoutExpired(cmd=kwargs.get("args", "cmd"), timeout=0.01)
        return _Proc(0)

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    args = argparse.Namespace(
        output_root=str(tmp_path / "out"),
        families="market_price",
        start_date="20220101",
        end_date="20220131",
        max_workers=2,
        request_sleep=0.0,
        continue_on_error=False,
        include_disabled=False,
        resume=False,
        symbols=",".join([f"{i:06d}" for i in range(10)]),
        index_symbols="",
        trade_dates="",
        report_dates="",
        industry_names="",
        concept_names="",
        api_names="stock_zh_a_hist",
        universe_root="config/factor_sources/acquisition_universe",
        symbol_batch_size=5,
        batch_timeout_sec=0.01,
        stop_on_batch_timeout=True,
        keep_batch_outputs=True,
        disable_symbol_batching=False,
        task_timeout_sec=None,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
    )

    out = cli._run_with_symbol_batching(args)
    report = pd.read_csv(out["batch_report_path"])
    assert len(report) == 1
    assert report.iloc[0]["batch_status"] == "timeout"
    assert int(report.iloc[0]["return_code"]) == -999
    assert bool(report.iloc[0]["merged_to_master"]) is False
    assert call_count["n"] == 1


def test_batch_timeout_reports_child_task_events_and_no_empty_master_catalog(tmp_path, monkeypatch):
    class _Proc:
        def __init__(self, returncode: int):
            self.returncode = returncode

    def fake_run(*args, **kwargs):
        raise cli.subprocess.TimeoutExpired(cmd="cmd", timeout=0.01)

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    args = argparse.Namespace(
        output_root=str(tmp_path / "out"),
        families="market_price",
        start_date="20220101",
        end_date="20220131",
        max_workers=2,
        request_sleep=0.0,
        continue_on_error=False,
        include_disabled=False,
        resume=False,
        symbols=",".join([f"{i:06d}" for i in range(10)]),
        index_symbols="",
        trade_dates="",
        report_dates="",
        industry_names="",
        concept_names="",
        api_names="stock_zh_a_hist",
        universe_root="config/factor_sources/acquisition_universe",
        symbol_batch_size=5,
        batch_timeout_sec=0.01,
        stop_on_batch_timeout=True,
        keep_batch_outputs=True,
        disable_symbol_batching=False,
        task_timeout_sec=1.0,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
    )

    out = cli._run_with_symbol_batching(args)
    report = pd.read_csv(out["batch_report_path"])
    assert "child_task_events_path" in report.columns
    assert (tmp_path / "out" / "raw_ingest_catalog.csv").exists() is False


def test_heartbeat_sec_non_batched_passes_through(monkeypatch, tmp_path):
    captured = {}

    def fake_run_raw_ingest_official(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(cli, "run_raw_ingest_official", fake_run_raw_ingest_official)
    args = argparse.Namespace(
        output_root=str(tmp_path / "out"),
        families="market_price",
        symbols="000001",
        index_symbols="",
        trade_dates="",
        report_dates="",
        industry_names="",
        concept_names="",
        api_names="stock_zh_a_hist",
        universe_root="config/factor_sources/acquisition_universe",
        start_date="20220101",
        end_date="20220131",
        max_workers=1,
        request_sleep=0.0,
        continue_on_error=True,
        include_disabled=False,
        resume=False,
        task_timeout_sec=None,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
        heartbeat_sec=7.5,
    )

    cli._run_without_batching(args)
    assert captured["heartbeat_sec"] == 7.5


def test_heartbeat_sec_batched_child_command_passes_through(tmp_path):
    args = argparse.Namespace(
        families="market_price",
        start_date="20220101",
        end_date="20220131",
        max_workers=1,
        request_sleep=0.0,
        symbols="000001,000002",
        index_symbols="",
        trade_dates="",
        report_dates="",
        industry_names="",
        concept_names="",
        api_names="stock_zh_a_hist",
        universe_root="config/factor_sources/acquisition_universe",
        task_timeout_sec=None,
        task_retry_attempts=0,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
        heartbeat_sec=9.0,
        continue_on_error=False,
        include_disabled=False,
        resume=False,
    )

    cmd = cli._build_child_cmd(args, tmp_path / "batch", ["000001"])
    assert "--heartbeat-sec" in cmd
    assert cmd[cmd.index("--heartbeat-sec") + 1] == "9.0"
