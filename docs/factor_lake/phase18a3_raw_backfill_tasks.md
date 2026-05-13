# Phase 18A-3: Raw Backfill Task Planner and Dry-run Executor

## Why task planning follows backfill planning
Phase 18A-2 defines *what* should be backfilled. Phase 18A-3 defines *how* those plan rows become explicit executable ingest tasks.

This separates strategic planning from operational scheduling and keeps future execution reproducible.

## Plan item -> task conversion
Each `RawBackfillPlanItem` is converted into one conservative `RawBackfillTask` in dry-run mode.

Task fields include:
- dataset/source/api identity
- priority
- partition and fetch params
- output partition
- planned date window
- status and notes

## Why dry-run first
This phase intentionally avoids live full ingest. Dry-run outputs are used to verify:
- task structure
- filter behavior
- per-family/per-dataset/per-priority task counts
- export artifact format

## Future real execution path
In future phases, task execution can iterate over these task CSVs and call `run_raw_ingest` with controlled batching/retries/checkpointing.

## Non-goals (still unchanged)
- no normalized panel construction
- no feature/factor store construction
- no signals/backtests
- no full real AkShare historical ingest

## Export command
```bash
PYTHONPATH=src python -m qsys.utils.export_factor_lake_backfill_tasks --output-root outputs/factor_lake_registry
```

Optional filters:
- `--priority P0`
- `--source-family market_price`
- `--dataset-name daily_bar_raw`
- `--max-tasks 100`

## Outputs
- `outputs/factor_lake_registry/raw_backfill_tasks.csv`
- `outputs/factor_lake_registry/raw_backfill_task_summary.csv`
