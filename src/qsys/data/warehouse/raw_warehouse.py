from __future__ import annotations

import csv
import json
import multiprocessing as mp
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec
from qsys.reporting.artifacts import write_warnings


class FetchTimeoutError(TimeoutError):
    pass


def _tail_traceback(err: BaseException, limit: int = 5) -> str:
    lines = traceback.format_exception(type(err), err, err.__traceback__)
    return "".join(lines[-limit:]).strip()


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _serialize_partition_value(key: str, value: Any) -> Any:
    if isinstance(value, str) and value.isdigit() and key in {"symbol", "code", "ts_code"}:
        return f"'{value}"
    return value


def _serialize_record_partition_keys(record: dict[str, Any], partition_keys: list[str]) -> dict[str, Any]:
    out = dict(record)
    for key in partition_keys:
        if key in out:
            out[key] = _serialize_partition_value(key, out[key])
    return out


def _fetch_write_worker(queue: mp.Queue[Any], fetch_fn: Callable[[FetchPartition], pd.DataFrame], partition: FetchPartition, raw_fp: str) -> None:
    started = time.perf_counter()
    try:
        data = fetch_fn(partition)
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            queue.put({"status": "empty", "rows": 0, "n_columns": len(df.columns), "columns": list(map(str, df.columns)), "path": raw_fp, "elapsed_seconds": time.perf_counter() - started})
            return
        fp = Path(raw_fp)
        fp.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(fp, index=False)
        queue.put({"status": "fetched", "rows": int(len(df)), "n_columns": int(len(df.columns)), "columns": list(map(str, df.columns)), "path": raw_fp, "elapsed_seconds": time.perf_counter() - started})
    except Exception as exc:  # pragma: no cover
        queue.put({"status": "failed", "error_type": type(exc).__name__, "error_message": str(exc), "traceback_tail": _tail_traceback(exc), "path": raw_fp, "elapsed_seconds": time.perf_counter() - started})


def run_fetch_write_with_hard_timeout(fetch_fn: Callable[[FetchPartition], pd.DataFrame], partition: FetchPartition, raw_fp: Path, timeout_seconds: float) -> dict[str, Any]:
    q: mp.Queue[Any] = mp.Queue()
    proc = mp.Process(target=_fetch_write_worker, args=(q, fetch_fn, partition, str(raw_fp)))
    proc.start()
    proc.join(timeout_seconds)
    if proc.is_alive():
        proc.terminate()
        proc.join(1)
        raise FetchTimeoutError(f"fetch exceeded timeout={timeout_seconds}s")
    if q.empty():
        raise RuntimeError("fetch worker exited without result")
    return q.get()


@dataclass
class RawWarehouseRunner:
    source_spec: SourceSpec
    raw_root: Path
    output_dir: Path
    run_name: str
    overwrite_cache: bool = False
    request_timeout: float = 30.0
    retries: int = 1
    retry_wait: float = 0.0
    request_sleep: float = 0.1
    request_jitter: float = 0.0
    max_workers: int = 2
    show_progress: bool = False
    progress_every: int = 20
    include_disabled: bool = False

    def run(self, **fetch_plan_kwargs: Any) -> dict[str, Path]:
        started = time.perf_counter()
        partitions = list(self.source_spec.build_fetch_plan(**fetch_plan_kwargs))
        run_dir = self.output_dir / self.run_name
        run_dir.mkdir(parents=True, exist_ok=True)

        inventory: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        step = max(1, self.progress_every)

        workers = max(1, self.max_workers)
        if workers == 1:
            for i, p in enumerate(partitions, 1):
                rec, part_events = self._process_partition(p)
                inventory.append(rec)
                events.extend(part_events)
                if self.show_progress and (i == 1 or i == len(partitions) or i % step == 0):
                    print(f"[{i}/{len(partitions)}] { {k: p.values[k] for k in self.source_spec.partition_keys} } -> {rec['status']}", flush=True)
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [ex.submit(self._process_partition, p) for p in partitions]
                for i, fut in enumerate(as_completed(futs), 1):
                    rec, part_events = fut.result()
                    inventory.append(rec)
                    events.extend(part_events)
                    if self.show_progress and (i == 1 or i == len(partitions) or i % step == 0):
                        base = {k: rec[k] for k in self.source_spec.partition_keys}
                        print(f"[{i}/{len(partitions)}] {base} -> {rec['status']}", flush=True)

        pk = list(self.source_spec.partition_keys)
        inventory_sorted = sorted(inventory, key=lambda r: tuple(str(r.get(k, "")) for k in pk))
        failed = [r for r in inventory_sorted if r["status"] == "failed"]
        timed_out = [r for r in inventory_sorted if r["status"] == "timed_out"]
        empty = [r for r in inventory_sorted if r["status"] == "empty"]
        skipped = [r for r in inventory_sorted if r["status"] == "skipped"]

        self._write_artifacts(run_dir, inventory_sorted, failed, timed_out, empty, skipped)
        (run_dir / "operation_events.jsonl").write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events), encoding="utf-8")

        status_counts = pd.DataFrame(inventory_sorted)["status"].value_counts().to_dict() if inventory_sorted else {}
        cache_hits = int(status_counts.get("cache_hit", 0))
        cache_misses = len(inventory_sorted) - cache_hits - int(status_counts.get("skipped", 0))
        network_attempts = int(sum(int(r.get("attempts", 0)) for r in inventory_sorted))
        network_failed = int(status_counts.get("failed", 0) + status_counts.get("timed_out", 0))

        warnings: list[str] = []
        if int(status_counts.get("failed", 0)):
            warnings.append(f"Failed partitions: {int(status_counts.get('failed', 0))}")
        if int(status_counts.get("timed_out", 0)):
            warnings.append(f"Timed-out partitions: {int(status_counts.get('timed_out', 0))}")
        if int(status_counts.get("empty", 0)):
            warnings.append(f"Empty partitions: {int(status_counts.get('empty', 0))}")
        if int(status_counts.get("skipped", 0)):
            warnings.append(f"Skipped partitions: {int(status_counts.get('skipped', 0))}")

        manifest = {
            "source": self.source_spec.source_name,
            "source_version": self.source_spec.source_version,
            "fetch_mode": self.source_spec.fetch_mode,
            "run_name": self.run_name,
            "raw_root": str(self.raw_root),
            "output_dir": str(run_dir),
            "partition_keys": pk,
            "n_partitions": len(partitions),
            "planned_partitions": len(partitions),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "status_counts": status_counts,
            "n_fetched": int(status_counts.get("fetched", 0)),
            "n_failed": int(status_counts.get("failed", 0)),
            "n_timed_out": int(status_counts.get("timed_out", 0)),
            "n_empty": int(status_counts.get("empty", 0)),
            "n_skipped": int(status_counts.get("skipped", 0)),
            "network_requests_attempted": network_attempts,
            "network_requests_failed": network_failed,
            "request_timeout": self.request_timeout,
            "retries": self.retries,
            "max_attempts": self.retries + 1,
            "retry_wait": self.retry_wait,
            "request_sleep": self.request_sleep,
            "request_jitter": self.request_jitter,
            "max_workers": workers,
            "overwrite_cache": self.overwrite_cache,
            "include_disabled": self.include_disabled,
            "acquisition_status": self.source_spec.acquisition_status,
            "manual_review_required": self.source_spec.manual_review_required,
            "disabled_reason": self.source_spec.disabled_reason,
            "elapsed_seconds": time.perf_counter() - started,
        }
        (run_dir / "warehouse_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        write_warnings(run_dir, warnings)
        return {"run_dir": run_dir}

    def _process_partition(self, p: FetchPartition) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        raw_fp = self.source_spec.build_raw_partition_path(Path(self.raw_root), p)
        base = {k: p.values[k] for k in self.source_spec.partition_keys}
        part_events: list[dict[str, Any]] = [{"event": "partition_started", "timestamp": _utc_now_iso(), "partition": base}]

        if self._should_skip_for_acquisition_policy():
            rec = self._skipped_record(base, raw_fp)
            part_events.append({"event": "partition_skipped", "timestamp": _utc_now_iso(), "partition": base, "reason": rec["disabled_reason"] or "acquisition policy"})
            return rec, part_events

        cache_exists_before = raw_fp.exists()
        if cache_exists_before and not self.overwrite_cache:
            rec = {
                **base,
                "status": "cache_hit",
                "path": str(raw_fp),
                "cache_exists_before": True,
                "attempts": 0,
                "started_at": _utc_now_iso(),
                "finished_at": _utc_now_iso(),
                "elapsed_seconds": 0.0,
                "rows": None,
                "n_columns": None,
                "error_type": "",
                "error_message": "",
                "traceback_tail": "",
                "timeout_seconds": None,
                "acquisition_status": self.source_spec.acquisition_status,
                "manual_review_required": self.source_spec.manual_review_required,
                "disabled_reason": self.source_spec.disabled_reason or "",
            }
        else:
            rec = self._fetch_with_retries(p, raw_fp, cache_exists_before)
        part_events.append({"event": "partition_finished", "timestamp": _utc_now_iso(), "partition": base, "status": rec["status"]})
        return rec, part_events

    def _should_skip_for_acquisition_policy(self) -> bool:
        return (self.source_spec.acquisition_status in {"disabled", "manual_review", "excluded"}) and not self.include_disabled

    def _skipped_record(self, base: dict[str, Any], raw_fp: Path) -> dict[str, Any]:
        now = _utc_now_iso()
        return {
            **base,
            "status": "skipped",
            "path": str(raw_fp),
            "cache_exists_before": raw_fp.exists(),
            "attempts": 0,
            "started_at": now,
            "finished_at": now,
            "elapsed_seconds": 0.0,
            "rows": None,
            "n_columns": None,
            "error_type": "",
            "error_message": "",
            "traceback_tail": "",
            "timeout_seconds": None,
            "acquisition_status": self.source_spec.acquisition_status,
            "manual_review_required": self.source_spec.manual_review_required,
            "disabled_reason": self.source_spec.disabled_reason or "",
        }

    def _fetch_with_retries(self, p: FetchPartition, raw_fp: Path, cache_exists_before: bool) -> dict[str, Any]:
        max_attempts = self.retries + 1
        for attempt in range(1, max_attempts + 1):
            t0 = time.perf_counter()
            started_at = _utc_now_iso()
            try:
                if self.request_sleep > 0 or self.request_jitter > 0:
                    time.sleep(self.request_sleep + (random.uniform(0.0, self.request_jitter) if self.request_jitter > 0 else 0.0))
                meta = run_fetch_write_with_hard_timeout(self.source_spec.fetch_partition, p, raw_fp, self.request_timeout)
                rec = {**p.values, "status": meta.get("status", "failed"), "path": str(raw_fp), "cache_exists_before": cache_exists_before, "attempts": attempt,
                       "started_at": started_at, "finished_at": _utc_now_iso(), "elapsed_seconds": meta.get("elapsed_seconds", time.perf_counter() - t0), "rows": meta.get("rows"), "n_columns": meta.get("n_columns"),
                       "error_type": meta.get("error_type", ""), "error_message": meta.get("error_message", ""), "traceback_tail": meta.get("traceback_tail", ""), "timeout_seconds": None,
                       "acquisition_status": self.source_spec.acquisition_status, "manual_review_required": self.source_spec.manual_review_required, "disabled_reason": self.source_spec.disabled_reason or ""}
                if rec["status"] in {"fetched", "empty"}:
                    return rec
                if attempt >= max_attempts:
                    rec["status"] = "failed" if rec["status"] != "timed_out" else "timed_out"
                    return rec
            except FetchTimeoutError as exc:
                if attempt >= max_attempts:
                    return {**p.values, "status": "timed_out", "path": str(raw_fp), "cache_exists_before": cache_exists_before, "attempts": attempt, "started_at": started_at, "finished_at": _utc_now_iso(), "elapsed_seconds": time.perf_counter() - t0,
                            "rows": None, "n_columns": None, "error_type": type(exc).__name__, "error_message": str(exc), "traceback_tail": "", "timeout_seconds": self.request_timeout,
                            "acquisition_status": self.source_spec.acquisition_status, "manual_review_required": self.source_spec.manual_review_required, "disabled_reason": self.source_spec.disabled_reason or ""}
            except Exception as exc:
                if attempt >= max_attempts:
                    return {**p.values, "status": "failed", "path": str(raw_fp), "cache_exists_before": cache_exists_before, "attempts": attempt, "started_at": started_at, "finished_at": _utc_now_iso(), "elapsed_seconds": time.perf_counter() - t0,
                            "rows": None, "n_columns": None, "error_type": type(exc).__name__, "error_message": str(exc), "traceback_tail": _tail_traceback(exc), "timeout_seconds": None,
                            "acquisition_status": self.source_spec.acquisition_status, "manual_review_required": self.source_spec.manual_review_required, "disabled_reason": self.source_spec.disabled_reason or ""}
            if self.retry_wait > 0:
                time.sleep(self.retry_wait)
        raise RuntimeError("unreachable")

    def _write_artifacts(self, run_dir: Path, inventory: list[dict[str, Any]], failed: list[dict[str, Any]], timed_out: list[dict[str, Any]], empty: list[dict[str, Any]], skipped: list[dict[str, Any]]) -> None:
        pk = list(self.source_spec.partition_keys)
        common = ["status", "path", "cache_exists_before", "attempts", "started_at", "finished_at", "elapsed_seconds", "rows", "n_columns", "error_type", "error_message", "traceback_tail", "timeout_seconds", "acquisition_status", "manual_review_required", "disabled_reason"]
        inventory_rows = [_serialize_record_partition_keys(r, pk) for r in inventory]
        failed_rows = [_serialize_record_partition_keys(r, pk) for r in failed]
        timeout_rows = [_serialize_record_partition_keys(r, pk) for r in timed_out]
        empty_rows = [_serialize_record_partition_keys(r, pk) for r in empty]
        skipped_rows = [_serialize_record_partition_keys(r, pk) for r in skipped]
        pd.DataFrame(inventory_rows, columns=pk + common).to_csv(run_dir / "cache_inventory.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        pd.DataFrame(failed_rows, columns=pk + common).to_csv(run_dir / "failed_partitions.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        pd.DataFrame(timeout_rows, columns=pk + common).to_csv(run_dir / "timeout_partitions.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        pd.DataFrame(empty_rows, columns=pk + common).to_csv(run_dir / "empty_partitions.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        pd.DataFrame(skipped_rows, columns=pk + common).to_csv(run_dir / "skipped_partitions.csv", index=False, quoting=csv.QUOTE_MINIMAL)
