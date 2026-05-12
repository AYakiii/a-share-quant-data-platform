from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.warehouse import RawWarehouseRunner, get_source_spec


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--raw-root", default="data/raw")
    p.add_argument("--output-dir", default="outputs/raw_warehouse")
    p.add_argument("--run-name", default=None)
    p.add_argument("--overwrite-cache", action="store_true")
    p.add_argument("--request-timeout", type=float, default=30)
    p.add_argument("--retries", type=int, default=1)
    p.add_argument("--retry-wait", type=float, default=0)
    p.add_argument("--request-sleep", type=float, default=0.1)
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
        show_progress=a.show_progress,
        progress_every=a.progress_every,
    )
    out = runner.run(
        start_date=a.start_date,
        end_date=a.end_date,
        include_calendar_days=a.include_calendar_days,
        exchanges=a.exchanges,
    )
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()
