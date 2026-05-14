# Phase 18A-10: Raw Coverage Audit & Backfill Wave Plan

## Purpose
After broad raw coverage ingest, we need deterministic audit outputs to decide staged backfill execution order.
This phase reads raw coverage catalog outputs and classifies each API into health classes and backfill waves.

## Inputs
- `raw_ingest_catalog.csv`
- `raw_ingest_summary.csv` (optional for cross-check)

## Outputs
- `raw_source_health_matrix.csv`
- `raw_backfill_wave_plan.csv`

## Health classification
- `ready` -> Wave 1 (`success` with rows > 0)
- `empty_check_later` -> Wave 2 (`empty` or `success` with 0 rows)
- `unstable_retry_needed` -> Wave 2 (network/timeout instability)
- `parameter_value_review` -> Wave 3 (invalid/manual parameter-value issues)
- `adapter_defensive_fix_needed` -> Wave 3 (NoneType/KeyError/decode-like issues)
- `pending_adapter` -> Wave 3 (missing/not-implemented adapter)
- `manual_review_needed` -> Wave 3 (fallback class)

## CLI
```bash
PYTHONPATH=src python -m qsys.utils.audit_factor_lake_raw_coverage \
  --input-root outputs/factor_lake_raw_coverage_after_fix_1m \
  --output-root outputs/factor_lake_raw_coverage_after_fix_1m
```

## Policy alignment
Coverage-first remains unchanged:
- Wave 1 proceeds even if some APIs fail
- failures and empty results are recorded and audited, not hidden

## Out of scope
- normalized panels
- feature/factor store
- factor construction
- signal diagnostics/backtests
