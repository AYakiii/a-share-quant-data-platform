from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from qsys.data.warehouse import raw_warehouse as rw
from qsys.data.warehouse.raw_warehouse import (
    RawWarehouseRunner,
    run_fetch_write_with_hard_timeout,
)
from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec
from qsys.utils.build_raw_warehouse import _merge_symbols


def _make_spec(fetch_fn, plan=None, *, source_name: str = "demo", partition_keys=("exchange", "trade_date"), acquisition_status: str = "enabled"):
    def _plan(**kwargs):
        return [FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"})]

    def _path(raw_root, p):
        return raw_root / source_name / f"exchange={p.values['exchange']}" / f"trade_date={p.values['trade_date']}" / "data.parquet"

    return SourceSpec(source_name, "v1", partition_keys, "exchange_date", plan or _plan, fetch_fn, _path, {}, acquisition_status=acquisition_status)


def _fetch_ok(_):
    return pd.DataFrame({"a": [1, 2]})


def test_worker_writes_parquet_and_returns_metadata_only(tmp_path):
    p = FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"})
    out_fp = tmp_path / "x.parquet"

    meta = run_fetch_write_with_hard_timeout(_fetch_ok, p, out_fp, 5)

    assert meta["status"] == "fetched"
    assert meta["rows"] == 2
    assert meta["n_columns"] == 1
    assert out_fp.exists()


def test_max_workers_1_sequential_behavior(tmp_path, monkeypatch):
    order: list[str] = []

    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
    ]

    def _fake(_fetch_fn, partition, _raw_fp, _timeout):
        order.append(partition.values["trade_date"])
        return {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01}

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _fake)
    RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "seq", max_workers=1).run(start_date="2025-01-02", end_date="2025-01-03", include_calendar_days=False, exchanges="sse")
    assert order == ["2025-01-02", "2025-01-03"]


def test_max_workers_2_parallel_and_artifacts(tmp_path, monkeypatch):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-06"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-07"}),
    ]
    seq = iter([
        {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01},
        {"status": "empty", "rows": 0, "n_columns": 0, "elapsed_seconds": 0.01},
        "__timeout__",
        {"status": "failed", "error_type": "RuntimeError", "error_message": "x", "traceback_tail": "tb", "elapsed_seconds": 0.01},
    ])

    def _fake(*args, **kwargs):
        out = next(seq)
        if out == "__timeout__":
            raise rw.FetchTimeoutError("t")
        return out

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _fake)
    out = RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "par", retries=0, max_workers=2).run(
        start_date="2025-01-02", end_date="2025-01-07", include_calendar_days=False, exchanges="sse"
    )
    run_dir = out["run_dir"]
    inv = pd.read_csv(run_dir / "cache_inventory.csv")
    assert len(inv) == 4
    assert list(inv["trade_date"]) == ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]
    manifest = json.loads((run_dir / "warehouse_manifest.json").read_text(encoding="utf-8"))
    assert manifest["n_fetched"] == 1
    assert manifest["n_empty"] == 1
    assert manifest["n_timed_out"] == 1
    assert manifest["n_failed"] == 1
    assert (run_dir / "operation_events.jsonl").exists()


def test_cache_hit_under_parallel_mode(tmp_path, monkeypatch):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
    ]

    def _path(raw_root, p):
        return raw_root / "demo" / f"exchange={p.values['exchange']}" / f"trade_date={p.values['trade_date']}" / "data.parquet"

    raw_hit = _path(tmp_path / "raw", FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}))
    raw_hit.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(raw_hit, index=False)

    calls = {"n": 0}

    def _fake(*args, **kwargs):
        calls["n"] += 1
        return {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01}

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _fake)
    spec = SourceSpec("demo", "v1", ("exchange", "trade_date"), "exchange_date", plan, _fetch_ok, _path, {})
    out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "cache_parallel", max_workers=2).run(
        start_date="2025-01-02", end_date="2025-01-03", include_calendar_days=False, exchanges="sse"
    )
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert set(inv["status"]) == {"cache_hit", "fetched"}
    assert calls["n"] == 1


def test_skipped_and_include_disabled_behavior(tmp_path, monkeypatch):
    spec = _make_spec(_fetch_ok, acquisition_status="disabled")
    out_skip = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "skip", include_disabled=False).run(
        start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse"
    )
    inv_skip = pd.read_csv(out_skip["run_dir"] / "cache_inventory.csv")
    assert inv_skip.iloc[0]["status"] == "skipped"

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01})
    out_force = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "force", include_disabled=True).run(
        start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse"
    )
    inv_force = pd.read_csv(out_force["run_dir"] / "cache_inventory.csv")
    assert inv_force.iloc[0]["status"] == "fetched"


def test_stock_inventory_symbol_preserves_leading_zero_after_read_csv(tmp_path, monkeypatch):
    def _plan(**kwargs):
        return [FetchPartition(values={"symbol": "000001", "start_date": "2026-01-01", "end_date": "2026-01-10"})]

    def _path(raw_root, p):
        return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={p.values['symbol']}" / f"start_date={p.values['start_date']}_end_date={p.values['end_date']}" / "data.parquet"

    spec = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", _plan, _fetch_ok, _path, {})
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 2, "n_columns": 1, "elapsed_seconds": 0.01})
    out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "stock_zero", retries=0).run(start_date="2026-01-01", end_date="2026-01-10", symbols="000001")
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv", dtype={"symbol": str})
    assert inv.loc[0, "symbol"] == "000001"


def test_merge_symbols_file_and_cli_deduplicate_and_keep_order(tmp_path):
    fp = tmp_path / "symbols.txt"
    fp.write_text("# universe\n000001\n\n000002\n000001\n", encoding="utf-8")
    out = _merge_symbols("000333,000002", str(fp))
    assert out == ["000333", "000002", "000001"]


def test_heartbeat_not_printed_when_show_progress_false(tmp_path, monkeypatch, capsys):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-06"}),
    ]
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01})
    RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "hb_off", max_workers=2, heartbeat_sec=0.05, show_progress=False).run(
        start_date="2025-01-02", end_date="2025-01-03", include_calendar_days=False, exchanges="sse"
    )
    out = capsys.readouterr().out
    assert "[heartbeat]" not in out


def test_heartbeat_prints_in_parallel_mode(tmp_path, monkeypatch, capsys):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-06"}),
    ]

    def _slow(*args, **kwargs):
        import time

        time.sleep(0.15)
        return {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.15}

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _slow)
    RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "hb_on", max_workers=2, heartbeat_sec=0.05, show_progress=True).run(
        start_date="2025-01-02", end_date="2025-01-06", include_calendar_days=False, exchanges="sse"
    )
    out = capsys.readouterr().out
    assert "[heartbeat]" in out



def test_partition_batching_processes_all_partitions(tmp_path, monkeypatch):
    plan = lambda **kwargs: [FetchPartition(values={"exchange":"SSE","trade_date":f"2025-01-0{i}"}) for i in range(2,8)]
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status":"fetched","rows":1,"n_columns":1,"elapsed_seconds":0.01})
    out = RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path/"raw", tmp_path/"out", "batch", max_workers=2, partition_batch_size=2).run(start_date="2025-01-02", end_date="2025-01-07", include_calendar_days=False, exchanges="sse")
    inv = pd.read_csv(out["run_dir"]/"cache_inventory.csv")
    assert len(inv)==6

def test_batch_timeout_records_timeout(tmp_path, monkeypatch):
    plan = lambda **kwargs: [FetchPartition(values={"exchange":"SSE","trade_date":"2025-01-02"}), FetchPartition(values={"exchange":"SSE","trade_date":"2025-01-03"})]
    def _slow(*args, **kwargs):
        import time; time.sleep(0.2); return {"status":"fetched","rows":1,"n_columns":1,"elapsed_seconds":0.2}
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _slow)
    out = RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path/"raw", tmp_path/"out", "bt", max_workers=2, partition_batch_size=2, batch_timeout_sec=0.05).run(start_date="2025-01-02", end_date="2025-01-03", include_calendar_days=False, exchanges="sse")
    inv = pd.read_csv(out["run_dir"]/"cache_inventory.csv")
    assert "timed_out" in set(inv["status"])

def test_dual_api_metadata_and_date_filtering(tmp_path, monkeypatch):
    import qsys.data.warehouse.source_specs as ss
    def _hist(**kwargs):
        return type("R",(),{"raw":pd.DataFrame({"date":["2025-12-31","2026-01-02","2027-01-01"],"x":[1,2,3]})})
    monkeypatch.setattr(ss, "fetch_stock_zh_a_hist", _hist)
    part = FetchPartition(values={"symbol":"000001","start_date":"2026-01-01","end_date":"2026-01-10"})
    out = ss._fetch_stock_zh_a_daily_partition(part)
    assert out["actual_api_name"]=="stock_zh_a_hist"
    assert out["rows_before_filter"]==3 and out["rows_after_filter"]==1


def test_build_raw_warehouse_help_import_smoke():
    proc = subprocess.run([sys.executable, "-m", "qsys.utils.build_raw_warehouse", "--help"], capture_output=True, text=True, env={**__import__("os").environ, "PYTHONPATH": "src"})
    assert proc.returncode == 0
    assert "--source" in proc.stdout



def test_inventory_contains_dual_api_and_batch_fields(tmp_path, monkeypatch):
    plan = lambda **kwargs: [FetchPartition(values={"symbol": "000001", "start_date": "2026-01-01", "end_date": "2026-01-10"})]
    def _path(raw_root, p):
        return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={p.values['symbol']}" / f"start_date={p.values['start_date']}_end_date={p.values['end_date']}" / "data.parquet"
    spec = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", plan, _fetch_ok, _path, {})
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status":"fetched","rows":1,"n_columns":1,"elapsed_seconds":0.01, "actual_api_name":"stock_zh_a_hist", "rows_after_filter":1})
    out = RawWarehouseRunner(spec, tmp_path/"raw", tmp_path/"out", "meta", partition_batch_size=1).run(start_date="2026-01-01", end_date="2026-01-10", symbols="000001")
    inv = pd.read_csv(out["run_dir"]/"cache_inventory.csv")
    for col in ["actual_api_name", "rows_after_filter", "batch_id"]:
        assert col in inv.columns

def test_batch_timeout_rows_preserve_partition_keys(tmp_path, monkeypatch):
    plan = lambda **kwargs: [FetchPartition(values={"symbol":"000001","start_date":"2026-01-01","end_date":"2026-01-10"})]
    def _path(raw_root, p):
        return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={p.values['symbol']}" / f"start_date={p.values['start_date']}_end_date={p.values['end_date']}" / "data.parquet"
    spec = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", plan, _fetch_ok, _path, {})
    def _slow(*args, **kwargs):
        import time; time.sleep(0.2); return {"status":"fetched","rows":1,"n_columns":1,"elapsed_seconds":0.2}
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _slow)
    out = RawWarehouseRunner(spec, tmp_path/"raw", tmp_path/"out", "bt_keys", max_workers=2, partition_batch_size=1, batch_timeout_sec=0.05).run(start_date="2026-01-01", end_date="2026-01-10", symbols="000001")
    inv = pd.read_csv(out["run_dir"]/"cache_inventory.csv", dtype={"symbol": str})
    row = inv.iloc[0]
    assert row["symbol"] == "000001"
    assert row["start_date"] == "2026-01-01" and row["end_date"] == "2026-01-10"


def test_merge_symbols_normalize_apostrophe(tmp_path):
    fp = tmp_path / "symbols.txt"
    fp.write_text("'000001\n600519\n", encoding="utf-8")
    out = _merge_symbols("'000001, 600519", str(fp))
    assert out == ["000001", "600519"]
