from __future__ import annotations

import json
import pandas as pd
import pytest

from qsys.data.warehouse import raw_warehouse as rw
from qsys.data.warehouse.raw_warehouse import RawWarehouseRunner, run_fetch_write_with_hard_timeout
from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec


def _make_spec(fetch_fn, plan=None):
    def _plan(**kwargs):
        return [FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"})]

    def _path(raw_root, p):
        return raw_root / "demo" / f"exchange={p.values['exchange']}" / f"trade_date={p.values['trade_date']}" / "data.parquet"

    return SourceSpec("demo", "v1", ("exchange", "trade_date"), "exchange_date", plan or _plan, fetch_fn, _path, {})


def _fetch_ok(_):
    return pd.DataFrame({"a": [1, 2]})

def test_worker_writes_parquet_and_returns_metadata_only(tmp_path):
    p = FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"})
    out_fp = tmp_path / "x.parquet"
    meta = run_fetch_write_with_hard_timeout(_fetch_ok, p, out_fp, 5)
    assert meta["status"] == "fetched"
    assert meta["rows"] == 2 and meta["n_columns"] == 1
    assert out_fp.exists()


def test_cache_hit_skips_fetch(tmp_path):
    called = {"n": 0}

    def _fetch(_):
        called["n"] += 1
        return pd.DataFrame({"x": [2]})

    raw_fp = tmp_path / "raw" / "demo" / "exchange=SSE" / "trade_date=2025-01-02" / "data.parquet"
    raw_fp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(raw_fp, index=False)
    RawWarehouseRunner(_make_spec(_fetch), tmp_path / "raw", tmp_path / "out", "r1").run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    assert called["n"] == 0


def test_retries_semantics(tmp_path, monkeypatch):
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "failed", "error_type": "X", "error_message": "m", "traceback_tail": "t", "elapsed_seconds": 0.01})
    out0 = RawWarehouseRunner(_make_spec(_fetch_ok), tmp_path / "raw", tmp_path / "out", "r0", retries=0, request_sleep=0).run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    inv0 = pd.read_csv(out0["run_dir"] / "cache_inventory.csv")
    assert int(inv0.iloc[0]["attempts"]) == 1

    calls = {"n": 0}
    def _fake(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01}
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _fake)
    out1 = RawWarehouseRunner(_make_spec(_fetch_ok), tmp_path / "raw2", tmp_path / "out", "r1", retries=1, request_sleep=0).run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    inv1 = pd.read_csv(out1["run_dir"] / "cache_inventory.csv")
    assert int(inv1.iloc[0]["attempts"]) == 2


def test_artifact_headers_and_statuses(tmp_path, monkeypatch):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
    ]
    seq = iter([
        {"status": "failed", "error_type": "RuntimeError", "error_message": "x", "traceback_tail": "tb", "elapsed_seconds": 0.01},
        {"status": "empty", "rows": 0, "n_columns": 0, "elapsed_seconds": 0.01},
    ])
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: next(seq))
    out = RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "r2", retries=0).run(
        start_date="2025-01-02", end_date="2025-01-03", include_calendar_days=False, exchanges="sse"
    )
    run_dir = out["run_dir"]
    inv = pd.read_csv(run_dir / "cache_inventory.csv")
    assert {"status", "error_type", "error_message", "traceback_tail", "rows", "n_columns", "attempts"}.issubset(inv.columns)
    assert set(inv["status"]) == {"failed", "empty"}

    failed = pd.read_csv(run_dir / "failed_partitions.csv")
    assert {"error_type", "error_message", "traceback_tail"}.issubset(failed.columns)
    assert "failed" in set(failed["status"])

    empty = pd.read_csv(run_dir / "empty_partitions.csv")
    assert {"rows", "n_columns"}.issubset(empty.columns)


def test_timeout_artifact_and_manifest_and_warnings(tmp_path, monkeypatch):
    old = rw.run_fetch_write_with_hard_timeout
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: (_ for _ in ()).throw(rw.FetchTimeoutError("t")))
    try:
        out = RawWarehouseRunner(_make_spec(lambda _: pd.DataFrame({"a": [1]})), tmp_path / "raw", tmp_path / "out", "r3", retries=0).run(
            start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse"
        )
    finally:
        monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", old)
    run_dir = out["run_dir"]
    tdf = pd.read_csv(run_dir / "timeout_partitions.csv")
    assert {"timeout_seconds", "error_type", "error_message"}.issubset(tdf.columns)
    man = json.loads((run_dir / "warehouse_manifest.json").read_text(encoding="utf-8"))
    assert man["n_timed_out"] == 1 and man["n_fetched"] == 0 and man["max_attempts"] == 1
    warnings = (run_dir / "warnings.md").read_text(encoding="utf-8")
    assert "Timed-out partitions" in warnings and "Zero fetched partitions" in warnings
