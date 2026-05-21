from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.warehouse import RawWarehouseRunner, get_source_spec


def _normalize_symbol(sym: str) -> str:
    cleaned = sym.strip().lstrip("'")
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    return digits.zfill(6) if digits else cleaned


def _load_symbols_file(path: str) -> list[str]:
    rows = Path(path).read_text(encoding="utf-8").splitlines()
    return [_normalize_symbol(line) for line in rows if line.strip() and not line.strip().startswith("#")]


def _merge_symbols(symbols_csv: str, symbols_file: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if symbols_csv:
        for sym in symbols_csv.split(","):
            item = _normalize_symbol(sym)
            if item and item not in seen:
                seen.add(item)
                out.append(item)
    if symbols_file:
        for item in _load_symbols_file(symbols_file):
            if item and item not in seen:
                seen.add(item)
                out.append(item)
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--symbols", default="")
    p.add_argument("--symbols-file", default="")
    p.add_argument("--raw-root", default="data/raw")
    p.add_argument("--output-dir", default="outputs/raw_warehouse")
    p.add_argument("--run-name", default=None)
    p.add_argument("--overwrite-cache", action="store_true")
    p.add_argument("--request-timeout", type=float, default=30)
    p.add_argument("--retries", type=int, default=1)
    p.add_argument("--retry-wait", type=float, default=0)
    p.add_argument("--request-sleep", type=float, default=0.1)
    p.add_argument("--request-jitter", type=float, default=0.0)
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--heartbeat-sec", type=float, default=30)
    p.add_argument("--partition-batch-size", type=int, default=0)
    p.add_argument("--batch-timeout-sec", type=float, default=0)
    p.add_argument("--include-disabled", action="store_true")
    p.add_argument("--include-calendar-days", action="store_true")
    p.add_argument("--show-progress", action="store_true")
    p.add_argument("--progress-every", type=int, default=20)
    p.add_argument("--exchanges", default="both")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    spec = get_source_spec(a.source)
    run_name = a.run_name or f"{a.source}_{a.start_date}_{a.end_date}"
    runner = RawWarehouseRunner(
        source_spec=spec,
        raw_root=Path(a.raw_root),
        output_dir=Path(a.output_dir),
        run_name=run_name,
        overwrite_cache=a.overwrite_cache,
        request_timeout=a.request_timeout,
        retries=a.retries,
        retry_wait=a.retry_wait,
        request_sleep=a.request_sleep,
        request_jitter=a.request_jitter,
        max_workers=max(1, a.max_workers),
        heartbeat_sec=max(0.1, a.heartbeat_sec),
        partition_batch_size=max(0, a.partition_batch_size),
        batch_timeout_sec=max(0.0, a.batch_timeout_sec),
        show_progress=a.show_progress,
        progress_every=a.progress_every,
        include_disabled=a.include_disabled,
    )
    kwargs = {
        "start_date": a.start_date,
        "end_date": a.end_date,
        "include_calendar_days": a.include_calendar_days,
        "exchanges": a.exchanges,
    }
    if a.source == "stock_zh_a_daily":
        merged = _merge_symbols(a.symbols, a.symbols_file)
        kwargs["symbols"] = merged
    if a.source == "tradability_mask_v0":
        kwargs["raw_root"] = str(Path(a.raw_root))
    out = runner.run(**kwargs)
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()
