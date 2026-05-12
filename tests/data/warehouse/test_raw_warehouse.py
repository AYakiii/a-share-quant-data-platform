from __future__ import annotations

import time

import pandas as pd

from qsys.data.warehouse import raw_warehouse as rw
from qsys.data.warehouse.raw_warehouse import RawWarehouseRunner
from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec, get_source_spec


def _spec(fetch_fn):
    def _plan(**kwargs):
        return [FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"})]

    def _path(raw_root, p):
        return raw_root / "demo" / f"exchange={p.values['exchange']}" / f"trade_date={p.values['trade_date']}" / "data.parquet"

    return SourceSpec("demo", "v1", ("exchange", "trade_date"), "exchange_date", _plan, fetch_fn, _path, {})


def test_cache_hit_skips_fetch(tmp_path):
    called = {"n": 0}
    raw_fp = tmp_path / "raw" / "demo" / "exchange=SSE" / "trade_date=2025-01-02" / "data.parquet"
    raw_fp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(raw_fp, index=False)

    def _fetch(_):
        called["n"] += 1
        return pd.DataFrame({"x": [2]})

    old = rw.run_with_hard_timeout
    rw.run_with_hard_timeout = lambda fn, timeout: fn()
    try:
        runner = RawWarehouseRunner(_spec(_fetch), tmp_path / "raw", tmp_path / "out", "r1")
        runner.run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    finally:
        rw.run_with_hard_timeout = old
    assert called["n"] == 0


def test_overwrite_forces_refetch(tmp_path):
    called = {"n": 0}

    def _fetch(_):
        called["n"] += 1
        return pd.DataFrame({"x": [2]})

    raw_fp = tmp_path / "raw" / "demo" / "exchange=SSE" / "trade_date=2025-01-02" / "data.parquet"
    raw_fp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(raw_fp, index=False)
    old = rw.run_with_hard_timeout
    rw.run_with_hard_timeout = lambda fn, timeout: fn()
    try:
        runner = RawWarehouseRunner(_spec(_fetch), tmp_path / "raw", tmp_path / "out", "r1", overwrite_cache=True)
        runner.run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    finally:
        rw.run_with_hard_timeout = old
    assert called["n"] == 1


def test_timeout_records_and_continue(tmp_path):
    def _fetch(_):
        time.sleep(0.2)
        return pd.DataFrame({"x": [1]})

    old = rw.run_with_hard_timeout
    rw.run_with_hard_timeout = lambda fn, timeout: (_ for _ in ()).throw(rw.FetchTimeoutError("x"))
    try:
        runner = RawWarehouseRunner(_spec(_fetch), tmp_path / "raw", tmp_path / "out", "r1", request_timeout=0.05)
        out = runner.run(start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse")
    finally:
        rw.run_with_hard_timeout = old
    tdf = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert "timed_out" in set(tdf["status"])


def test_inventory_statuses(tmp_path):
    seq = iter([pd.DataFrame({"x": [1]}), pd.DataFrame(), RuntimeError("x")])

    def _plan(**kwargs):
        return [
            FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
            FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
            FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-06"}),
        ]

    def _fetch(_):
        x = next(seq)
        if isinstance(x, Exception):
            raise x
        return x

    spec = _spec(_fetch)
    spec = SourceSpec(**{**spec.__dict__, "build_fetch_plan": _plan})
    old = rw.run_with_hard_timeout
    rw.run_with_hard_timeout = lambda fn, timeout: fn()
    try:
        out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "r1", retries=1).run(
            start_date="2025-01-02", end_date="2025-01-06", include_calendar_days=False, exchanges="sse"
        )
    finally:
        rw.run_with_hard_timeout = old
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert set(inv["status"]) == {"fetched", "empty", "failed"}


def test_margin_detail_partition_paths_and_no_symbols_required(tmp_path, monkeypatch):
    spec = get_source_spec("margin_detail")
    monkeypatch.setattr("qsys.data.warehouse.source_specs.fetch_stock_margin_detail_sse", lambda d: type("R", (), {"raw": pd.DataFrame({"a": [1]})})())
    monkeypatch.setattr("qsys.data.warehouse.source_specs.fetch_stock_margin_detail_szse", lambda d: type("R", (), {"raw": pd.DataFrame({"a": [1]})})())
    old = rw.run_with_hard_timeout
    rw.run_with_hard_timeout = lambda fn, timeout: fn()
    try:
        out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "r1", request_sleep=0).run(
            start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="both"
        )
    finally:
        rw.run_with_hard_timeout = old
    assert (tmp_path / "raw" / "margin_detail" / "v1" / "exchange=SSE" / "trade_date=2025-01-02" / "data.parquet").exists()
    assert (tmp_path / "raw" / "margin_detail" / "v1" / "exchange=SZSE" / "trade_date=2025-01-02" / "data.parquet").exists()
    assert (out["run_dir"] / "warehouse_manifest.json").exists()
