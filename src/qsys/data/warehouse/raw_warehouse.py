from __future__ import annotations

import json
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass
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


def _fetch_write_worker(queue: mp.Queue[Any], fetch_fn: Callable[[FetchPartition], pd.DataFrame], partition: FetchPartition, raw_fp: str) -> None:
    started = time.perf_counter()
    try:
        data = fetch_fn(partition)
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            queue.put(
                {
                    "status": "empty",
                    "rows": 0,
                    "n_columns": len(df.columns),
                    "columns": list(map(str, df.columns)),
                    "path": raw_fp,
                    "elapsed_seconds": time.perf_counter() - started,
                }
            )
            return
        fp = Path(raw_fp)
        fp.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(fp, index=False)
        queue.put(
            {
                "status": "fetched",
                "rows": int(len(df)),
                "n_columns": int(len(df.columns)),
                "columns": list(map(str, df.columns)),
                "path": raw_fp,
                "elapsed_seconds": time.perf_counter() - started,
            }
        )
    except Exception as exc:  # pragma: no cover
        queue.put(
            {
                "status": "failed",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback_tail": _tail_traceback(exc),
                "path": raw_fp,
                "elapsed_seconds": time.perf_counter() - started,
            }
        )


def run_fetch_write_with_hard_timeout(
    fetch_fn: Callable[[FetchPartition], pd.DataFrame], partition: FetchPartition, raw_fp: Path, timeout_seconds: float
) -> dict[str, Any]:
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
    show_progress: bool = False
    progress_every: int = 20

    def run(self, **fetch_plan_kwargs: Any) -> dict[str, Path]:
        started = time.perf_counter()
        partitions = list(self.source_spec.build_fetch_plan(**fetch_plan_kwargs))
        run_dir = self.output_dir / self.run_name
        run_dir.mkdir(parents=True, exist_ok=True)

        inventory: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        timed_out: list[dict[str, Any]] = []
        empty: list[dict[str, Any]] = []
        warnings: list[str] = []
        cache_hits = cache_misses = 0
        network_attempts = network_failed = 0

        step = max(1, self.progress_every)
        for i, p in enumerate(partitions, 1):
            raw_fp = self.source_spec.build_raw_partition_path(Path(self.raw_root), p)
            base = {k: p.values[k] for k in self.source_spec.partition_keys}
            cache_exists_before = raw_fp.exists()
            if cache_exists_before and not self.overwrite_cache:
                rec = {
                    **base,
                    "status": "cache_hit",
                    "path": str(raw_fp),
                    "cache_exists_before": True,
                    "attempts": 0,
                    "elapsed_seconds": 0.0,
                    "rows": None,
                    "n_columns": None,
                    "error_type": "",
                    "error_message": "",
                    "traceback_tail": "",
                }
                cache_hits += 1
            else:
                cache_misses += 1
                rec = self._fetch_with_retries(p, raw_fp, cache_exists_before)
                network_attempts += int(rec["attempts"])
                if rec["status"] in {"failed", "timed_out"}:
                    network_failed += 1
                    failed.append(rec) if rec["status"] == "failed" else timed_out.append(rec)
                if rec["status"] == "empty":
                    empty.append(rec)
            inventory.append(rec)
            if self.show_progress and (i == 1 or i == len(partitions) or i % step == 0):
                print(f"[{i}/{len(partitions)}] {base} -> {rec['status']}", flush=True)

        self._write_artifacts(run_dir, inventory, failed, timed_out, empty)
        status_counts = pd.DataFrame(inventory)["status"].value_counts().to_dict() if inventory else {}
        n_fetched = int(status_counts.get("fetched", 0))
        n_failed = int(status_counts.get("failed", 0))
        n_timed_out = int(status_counts.get("timed_out", 0))
        n_empty = int(status_counts.get("empty", 0))

        if n_failed:
            warnings.append(f"Failed partitions: {n_failed}")
            warnings.extend([f"- {r['path']} [{r['error_type']}] {r['error_message']}" for r in failed[:5]])
        if n_timed_out:
            warnings.append(f"Timed-out partitions: {n_timed_out}")
            warnings.append("Consider increasing --request-timeout if many partitions timed out.")
        if n_empty:
            warnings.append(f"Empty partitions: {n_empty}")
        if n_fetched == 0:
            warnings.append("Zero fetched partitions in this run.")

        manifest = {
            "source": self.source_spec.source_name,
            "source_version": self.source_spec.source_version,
            "fetch_mode": self.source_spec.fetch_mode,
            "run_name": self.run_name,
            "start_date": fetch_plan_kwargs.get("start_date"),
            "end_date": fetch_plan_kwargs.get("end_date"),
            "raw_root": str(self.raw_root),
            "output_dir": str(run_dir),
            "partition_keys": list(self.source_spec.partition_keys),
            "n_partitions": len(partitions),
            "planned_partitions": len(partitions),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "status_counts": status_counts,
            "n_fetched": n_fetched,
            "n_failed": n_failed,
            "n_timed_out": n_timed_out,
            "n_empty": n_empty,
            "network_requests_attempted": network_attempts,
            "network_requests_failed": network_failed,
            "request_timeout": self.request_timeout,
            "retries": self.retries,
            "max_attempts": self.retries + 1,
            "retry_wait": self.retry_wait,
            "request_sleep": self.request_sleep,
            "overwrite_cache": self.overwrite_cache,
            "include_calendar_days": bool(fetch_plan_kwargs.get("include_calendar_days", False)),
            "elapsed_seconds": time.perf_counter() - started,
            "assumptions": ["child process performs fetch+write; parent only receives metadata"],
        }
        (run_dir / "warehouse_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        write_warnings(run_dir, warnings)
        return {"run_dir": run_dir}

    def _fetch_with_retries(self, p: FetchPartition, raw_fp: Path, cache_exists_before: bool) -> dict[str, Any]:
        max_attempts = self.retries + 1
        for attempt in range(1, max_attempts + 1):
            t0 = time.perf_counter()
            try:
                meta = run_fetch_write_with_hard_timeout(self.source_spec.fetch_partition, p, raw_fp, self.request_timeout)
                rec = {
                    **p.values,
                    "status": meta.get("status", "failed"),
                    "path": str(raw_fp),
                    "cache_exists_before": cache_exists_before,
                    "attempts": attempt,
                    "elapsed_seconds": meta.get("elapsed_seconds", time.perf_counter() - t0),
                    "rows": meta.get("rows"),
                    "n_columns": meta.get("n_columns"),
                    "error_type": meta.get("error_type", ""),
                    "error_message": meta.get("error_message", ""),
                    "traceback_tail": meta.get("traceback_tail", ""),
                    "timeout_seconds": None,
                }
                if rec["status"] in {"fetched", "empty"}:
                    if rec["status"] == "fetched" and self.request_sleep > 0:
                        time.sleep(self.request_sleep)
                    return rec
                if attempt >= max_attempts:
                    rec["status"] = "failed" if rec["status"] != "timed_out" else "timed_out"
                    return rec
            except FetchTimeoutError as exc:
                if attempt >= max_attempts:
                    return {
                        **p.values,
                        "status": "timed_out",
                        "path": str(raw_fp),
                        "cache_exists_before": cache_exists_before,
                        "attempts": attempt,
                        "elapsed_seconds": time.perf_counter() - t0,
                        "rows": None,
                        "n_columns": None,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "traceback_tail": "",
                        "timeout_seconds": self.request_timeout,
                    }
            except Exception as exc:
                if attempt >= max_attempts:
                    return {
                        **p.values,
                        "status": "failed",
                        "path": str(raw_fp),
                        "cache_exists_before": cache_exists_before,
                        "attempts": attempt,
                        "elapsed_seconds": time.perf_counter() - t0,
                        "rows": None,
                        "n_columns": None,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "traceback_tail": _tail_traceback(exc),
                        "timeout_seconds": None,
                    }
            if self.retry_wait > 0:
                time.sleep(self.retry_wait)
        raise RuntimeError("unreachable")

    def _write_artifacts(self, run_dir: Path, inventory: list[dict[str, Any]], failed: list[dict[str, Any]], timed_out: list[dict[str, Any]], empty: list[dict[str, Any]]) -> None:
        pk = list(self.source_spec.partition_keys)
        inv_cols = pk + ["status", "path", "cache_exists_before", "attempts", "elapsed_seconds", "rows", "n_columns", "error_type", "error_message", "traceback_tail"]
        failed_cols = pk + ["status", "path", "cache_exists_before", "attempts", "elapsed_seconds", "error_type", "error_message", "traceback_tail"]
        timeout_cols = pk + ["status", "path", "cache_exists_before", "attempts", "timeout_seconds", "elapsed_seconds", "error_type", "error_message", "traceback_tail"]
        empty_cols = pk + ["status", "path", "cache_exists_before", "attempts", "elapsed_seconds", "rows", "n_columns"]
        pd.DataFrame(inventory, columns=inv_cols).to_csv(run_dir / "cache_inventory.csv", index=False)
        pd.DataFrame(failed, columns=failed_cols).to_csv(run_dir / "failed_partitions.csv", index=False)
        pd.DataFrame(timed_out, columns=timeout_cols).to_csv(run_dir / "timeout_partitions.csv", index=False)
        pd.DataFrame(empty, columns=empty_cols).to_csv(run_dir / "empty_partitions.csv", index=False)
