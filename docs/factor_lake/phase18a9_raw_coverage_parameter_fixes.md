# Phase 18A-9: Raw Coverage Parameter Fix Batch

## Context
Broad coverage runs are working overall and now produce auditable mixed outcomes (`success/failed/empty/pending_adapter`).
This phase fixes obvious parameter-mapping and defensive-handling issues discovered in the coverage health matrix.

## What was fixed
1. Parameter mapping robustness for APIs that reject generic kwargs.
   - Added signature-based parameter filtering in coverage execution.
   - Prevents passing unsupported fields such as `date` or `start_date/end_date` to no-arg APIs.
2. Defensive handling for malformed/None results.
   - If API call returns `None`, coverage run records failure with clear error instead of crashing.
3. Retry-friendly failure visibility.
   - Unstable APIs (e.g. premature response ending) remain clearly recorded in catalog for later retry waves.
4. Concept name manual review remains explicit.
   - `stock_board_concept_index_ths` still depends on valid board naming; keep audited in coverage catalog.

## Why this phase matters
The project priority is broad Raw Factor Lake coverage, not one-by-one manual fixes before moving forward.
Failures should be recorded and audited systematically, then retried in later waves.

## Still out of scope
- normalized panels
- feature/factor store
- factor construction
- signal diagnostics/backtests

## Outcome
Coverage ingest remains resilient:
- `continue_on_error=True` by default
- one API failure does not stop the full family run
- successful APIs continue writing raw files + metadata
- failed/empty/pending entries are captured for operational follow-up
