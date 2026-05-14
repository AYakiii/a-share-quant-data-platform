# Phase 18A-2: Raw Lake Backfill Planning Layer

## Why planning comes before full ingest
Before launching long-horizon ingest (2010–2026), we need a consistent planning table for source priority, partitioning strategy, PIT requirements, and look-ahead risk.

This avoids ad-hoc backfill execution and keeps Raw Factor Lake construction auditable.

## Priority groups
Recommended operational grouping:

- **P0**
  - trading calendar (future registry extension)
  - stock basic info (future registry extension)
  - daily stock bars
  - index bars
- **P1**
  - margin leverage
  - industry/concept data
- **P2**
  - financial fundamentals
  - ownership/governance
  - corporate actions
  - disclosure/IR
  - trading attention/events

## Data-shape categories in the plan
The plan rows map datasets into expected shapes:
- daily panel sources
- event tables
- report-period snapshot sources
- ownership/governance sources

## What this phase produces
- `qsys.data.factor_lake.backfill_plan.RawBackfillPlanItem`
- `generate_default_backfill_plan()`
- `backfill_plan_to_frame()`
- `export_backfill_plan_csv()`
- CLI export command:

```bash
PYTHONPATH=src python -m qsys.utils.export_factor_lake_backfill_plan --output-root outputs/factor_lake_registry
```

Output:
- `outputs/factor_lake_registry/raw_backfill_plan.csv`

## How this supports 2010–2026 Raw Factor Lake construction
The backfill plan standardizes:
- date window defaults (`2010-01-01` to `2026-12-31`)
- fetch granularity and partition strategy
- PIT requirement flags
- look-ahead risk labels
- source-family execution order

## What this phase intentionally does NOT do
- no live AkShare ingestion
- no normalized panel construction
- no factor store / feature store v2
- no signal/model/backtest/diagnostics logic
