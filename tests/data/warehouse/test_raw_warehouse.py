from __future__ import annotations

import json

import pandas as pd

from qsys.data.warehouse import raw_warehouse as rw
from qsys.data.warehouse.raw_warehouse import RawWarehouseRunner, run_fetch_write_with_hard_timeout
from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec


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


def test_timeout_empty_failed_and_manifest_counts(tmp_path, monkeypatch):
    plan = lambda **kwargs: [
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-02"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-03"}),
        FetchPartition(values={"exchange": "SSE", "trade_date": "2025-01-06"}),
    ]
    seq = iter([
        {"status": "failed", "error_type": "RuntimeError", "error_message": "x", "traceback_tail": "tb", "elapsed_seconds": 0.01},
        {"status": "empty", "rows": 0, "n_columns": 0, "elapsed_seconds": 0.01},
        "__timeout__",
    ])

    def _fake(*args, **kwargs):
        out = next(seq)
        if out == "__timeout__":
            raise rw.FetchTimeoutError("t")
        return out

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", _fake)
    out = RawWarehouseRunner(_make_spec(_fetch_ok, plan=plan), tmp_path / "raw", tmp_path / "out", "r2", retries=0).run(
        start_date="2025-01-02", end_date="2025-01-06", include_calendar_days=False, exchanges="sse"
    )
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert {"started_at", "finished_at", "elapsed_seconds", "status", "error_type"}.issubset(inv.columns)
    manifest = json.loads((out["run_dir"] / "warehouse_manifest.json").read_text(encoding="utf-8"))
    assert manifest["n_failed"] == 1
    assert manifest["n_empty"] == 1
    assert manifest["n_timed_out"] == 1


def test_skipped_and_include_disabled_behavior(tmp_path, monkeypatch):
    calls = {"n": 0}

    def _fetch(_):
        calls["n"] += 1
        return pd.DataFrame({"x": [1]})

    spec = _make_spec(_fetch, acquisition_status="disabled")
    out_skip = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "skip", include_disabled=False).run(
        start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse"
    )
    inv_skip = pd.read_csv(out_skip["run_dir"] / "cache_inventory.csv")
    assert inv_skip.iloc[0]["status"] == "skipped"
    assert calls["n"] == 0

    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 1, "n_columns": 1, "elapsed_seconds": 0.01})
    out_force = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "force", include_disabled=True).run(
        start_date="2025-01-02", end_date="2025-01-02", include_calendar_days=False, exchanges="sse"
    )
    inv_force = pd.read_csv(out_force["run_dir"] / "cache_inventory.csv")
    assert inv_force.iloc[0]["status"] == "fetched"


def test_stock_like_spec_partition_keys_supported(tmp_path, monkeypatch):
    def _plan(**kwargs):
        return [FetchPartition(values={"symbol": "000001", "start_date": "2026-01-01", "end_date": "2026-01-10"})]

    def _path(raw_root, p):
        return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={p.values['symbol']}" / f"start_date={p.values['start_date']}_end_date={p.values['end_date']}" / "data.parquet"

    spec = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", _plan, _fetch_ok, _path, {})
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 2, "n_columns": 1, "elapsed_seconds": 0.01})
    out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "stock", retries=0).run(start_date="2026-01-01", end_date="2026-01-10", symbols="000001")
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert set(["symbol", "start_date", "end_date"]).issubset(inv.columns)


def test_stock_inventory_symbol_preserves_leading_zero_after_read_csv(tmp_path, monkeypatch):
    def _plan(**kwargs):
        return [FetchPartition(values={"symbol": "000001", "start_date": "2026-01-01", "end_date": "2026-01-10"})]

    def _path(raw_root, p):
        return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={p.values['symbol']}" / f"start_date={p.values['start_date']}_end_date={p.values['end_date']}" / "data.parquet"

    spec = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", _plan, _fetch_ok, _path, {})
    monkeypatch.setattr(rw, "run_fetch_write_with_hard_timeout", lambda *args, **kwargs: {"status": "fetched", "rows": 2, "n_columns": 1, "elapsed_seconds": 0.01})
    out = RawWarehouseRunner(spec, tmp_path / "raw", tmp_path / "out", "stock_zero", retries=0).run(start_date="2026-01-01", end_date="2026-01-10", symbols="000001")
    inv = pd.read_csv(out["run_dir"] / "cache_inventory.csv")
    assert inv.loc[0, "symbol"] == "'000001"
