# Phase 18A-4: Raw Backfill Execution Layer

## Position in Phase 18A stack
Execution layer builds on:
1. Source Capability Registry (18A-1)
2. Raw Backfill Plan (18A-2)
3. Raw Backfill Tasks (18A-3)

This phase turns planned tasks into controlled, resumable raw ingest execution results.

## Why dry-run is default
Dry-run first prevents accidental large-scale 2010–2026 execution. It validates task volume, task distribution, and planned partitions before hitting remote APIs.

## Execution model
- `execute_backfill_task(...)`
- `execute_backfill_tasks(...)`

Behavior:
- default `dry_run=True`
- real execution only with explicit execute mode
- supports `max_tasks`, `continue_on_error`, `request_sleep`
- records task results in SQLite table `backfill_task_result`
- marks already-successful tasks as `skipped_completed`

## Safe usage examples
Dry-run:

```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_backfill_tasks \
  --output-root outputs/factor_lake_backfill \
  --priority P0 \
  --max-tasks 20 \
  --dry-run
```

Small real execution:

```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_backfill_tasks \
  --output-root outputs/factor_lake_backfill \
  --priority P0 \
  --dataset-name daily_bar_raw \
  --max-tasks 5 \
  --execute \
  --request-sleep 1.0
```

Safety guard: `--execute` requires `--max-tasks`.

## What this phase still does NOT do
- no normalized panel construction
- no feature/factor store
- no signal/model/diagnostics/backtest logic

## Future path
After repeated dry-runs and small real batches, this layer can support staged expansion toward full 2010–2026 Raw Factor Lake backfill with tighter scheduling, retries, and checkpoint controls.
