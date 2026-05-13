# Phase 18A-5: P0 Raw Execution Smoke and Adapter Readiness Validation

## Purpose
This phase validates the **raw execution stack only** on a tiny P0/P1 set.

It does **not** build normalized panels, factors, signals, diagnostics, or backtests.

## Tiny smoke task set
The smoke utility builds a fixed conservative task set:
- `daily_bar_raw` for symbol `000001` (tiny date range)
- `index_bar_raw` for index symbol `000300` (tiny date range)
- `margin_detail_raw` SSE one date
- `margin_detail_raw` SZSE one date

## Safety defaults
- default is dry-run
- real execution requires `--execute` and `--max-tasks`
- failures are recorded in task results instead of crashing the whole run

## Commands
Dry-run (default-safe):

```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_p0_smoke \
  --output-root outputs/factor_lake_backfill \
  --metastore-path outputs/factor_lake_backfill/metastore.sqlite \
  --max-tasks 4 \
  --dry-run
```

Tiny real execution:

```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_p0_smoke \
  --output-root outputs/factor_lake_backfill \
  --metastore-path outputs/factor_lake_backfill/metastore.sqlite \
  --execute \
  --max-tasks 2 \
  --request-sleep 1.0
```

## Expected outputs
- raw partition files under `data/raw/akshare/...`
- per-partition `metadata.json`
- SQLite metastore with task results
- `outputs/factor_lake_smoke/p0_smoke_summary.json`
- `outputs/factor_lake_smoke/p0_smoke_summary.csv`

## Local readback validation
After successful write, the smoke utility attempts `local_api.read_raw_partition(...)` on at least one successful partition and reports `readback_ok`.

## Interpreting failed/empty results
Some AkShare endpoints can be unstable or empty for specific dates. In this phase:
- `failed` means adapter call/IO failure was captured
- `empty` means endpoint responded with no rows
- both are useful readiness signals for future staged backfill

## Scope reminder
This phase is strictly raw execution validation infrastructure for future 2010–2026 Raw Factor Lake construction.
