# Phase 15F-3: Benchmark Comparison for Portfolio-Level Baselines

## Why this phase is required

Benchmark comparison is required before interpreting alpha at the portfolio level.
Raw strategy return alone is insufficient because it can reflect broad universe exposure rather than signal value.

Current baseline portfolio results should **not** be promoted without benchmark-relative evidence.

## What was implemented

- Added a minimal equal-weight benchmark comparison flow in the existing portfolio baseline runner.
- Benchmark uses the same feature-store universe and date range as each strategy run.
- Comparison is reported per `cost_bps` to match strategy execution assumptions.
- Outputs now include:
  - `benchmark_comparison.csv`
  - `benchmark_daily_returns.csv`
  - updated `run_manifest.json`
  - `warnings.md`

## Benchmark scope and limitations

- Equal-weight is a **simple research benchmark**, not a production benchmark.
- No production tradability claim is made.
- CSI500 index benchmark integration can be added later if needed; this phase keeps scope minimal and focuses on immediate benchmark-relative validation.
