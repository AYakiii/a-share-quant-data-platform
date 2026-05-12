from __future__ import annotations

import json
import multiprocessing as mp
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from qsys.reporting.artifacts import write_warnings
from qsys.data.warehouse.source_specs import FetchPartition, SourceSpec


class FetchTimeoutError(TimeoutError):
    pass


def run_with_hard_timeout(fn: Callable[[], Any], timeout_seconds: float) -> Any:
    """Run callable in subprocess and force-stop on timeout."""

    q: mp.Queue[Any] = mp.Queue()

    def _target(queue: mp.Queue[Any]) -> None:
        try:
            queue.put(("ok", fn()))
        except Exception as exc:  # pragma: no cover
            queue.put(("err", repr(exc)))

    proc = mp.Process(target=_target, args=(q,))
    proc.start()
    proc.join(timeout_seconds)
    if proc.is_alive():
        proc.terminate()
        proc.join(1)
        raise FetchTimeoutError(f"fetch exceeded timeout={timeout_seconds}s")
    if q.empty():
        raise RuntimeError("fetch worker exited without result")
    status, payload = q.get()
    if status == "err":
        raise RuntimeError(payload)
    return payload


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
        partitions = list(self.source_spec.build_fetch_plan(**fetch_plan_kwargs))
        run_dir = self.output_dir / self.run_name
        run_dir.mkdir(parents=True, exist_ok=True)

        inventory: list[dict[str, Any]] = []
        failed: list[dict[str, str]] = []
        timed_out: list[dict[str, str]] = []
        empty: list[dict[str, str]] = []
        warnings: list[str] = []
        cache_hits = cache_misses = 0

        step = max(1, self.progress_every)
        for i, p in enumerate(partitions, 1):
            raw_fp = self.source_spec.build_raw_partition_path(Path(self.raw_root), p)
            part_info = {k: p.values[k] for k in self.source_spec.partition_keys}
            if raw_fp.exists() and not self.overwrite_cache:
                status = "cache_hit"
                cache_hits += 1
            else:
                cache_misses += 1
                status = self._fetch_with_retries(p, raw_fp, failed, timed_out, empty, warnings)
            inventory.append({**part_info, "status": status, "path": str(raw_fp)})
            if self.show_progress and (i == 1 or i == len(partitions) or i % step == 0):
                print(f"[{i}/{len(partitions)}] {part_info} -> {status}", flush=True)

        inv_df = pd.DataFrame(inventory)
        inv_df.to_csv(run_dir / "cache_inventory.csv", index=False)
        pd.DataFrame(failed).to_csv(run_dir / "failed_partitions.csv", index=False)
        pd.DataFrame(timed_out).to_csv(run_dir / "timeout_partitions.csv", index=False)
        pd.DataFrame(empty).to_csv(run_dir / "empty_partitions.csv", index=False)

        manifest = {
            "source": self.source_spec.source_name,
            "version": self.source_spec.source_version,
            "fetch_mode": self.source_spec.fetch_mode,
            "run_name": self.run_name,
            "n_partitions": len(partitions),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "status_counts": inv_df["status"].value_counts().to_dict() if not inv_df.empty else {},
        }
        (run_dir / "warehouse_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        write_warnings(run_dir, warnings)
        return {"run_dir": run_dir}

    def _fetch_with_retries(self, p: FetchPartition, raw_fp: Path, failed: list[dict[str, str]], timed_out: list[dict[str, str]], empty: list[dict[str, str]], warnings: list[str]) -> str:
        for attempt in range(1, self.retries + 1):
            try:
                data = run_with_hard_timeout(lambda: self.source_spec.fetch_partition(p), self.request_timeout)
                df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
                if df.empty:
                    empty.append(p.values)
                    return "empty"
                raw_fp.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(raw_fp, index=False)
                if self.request_sleep > 0:
                    time.sleep(self.request_sleep)
                return "fetched"
            except FetchTimeoutError:
                if attempt >= self.retries:
                    timed_out.append(p.values)
                    warnings.append(f"timeout partition={p.values}")
                    return "timed_out"
            except Exception:
                if attempt >= self.retries:
                    failed.append(p.values)
                    return "failed"
            if self.retry_wait > 0:
                time.sleep(self.retry_wait)
        return "failed"
