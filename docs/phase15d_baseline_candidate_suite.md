# Phase 15D-1: Baseline Candidate Suite

Date: 2026-05-08

## What was implemented

- Added `src/qsys/utils/run_baseline_candidate_suite.py`.
- The runner evaluates simple return-only baseline candidates (if source columns are available):
  - `ret_1d_momentum = rank(ret_1d)`
  - `ret_1d_reversal = -rank(ret_1d)`
  - `ret_5d_momentum = rank(ret_5d)`
  - `ret_5d_reversal = -rank(ret_5d)`
  - `ret_20d_momentum = rank(ret_20d)`
  - `ret_20d_reversal = -rank(ret_20d)`
- Each candidate is evaluated against available label horizons:
  - `fwd_ret_5d`
  - `fwd_ret_20d`
- Output artifacts:
  - `signal_quality_report.csv`
  - `run_manifest.json` (via `qsys.reporting.write_run_manifest`)
  - `warnings.md` (via `qsys.reporting.write_warnings`)

## Diagnostics included

- mean Rank IC
- median Rank IC
- IC std
- ICIR
- t-stat
- positive_rate
- n_dates
- quantile spread / top-minus-bottom (when available)

## Warning policy included

- missing required feature columns
- missing label columns
- small sample size
- insufficient cross-sectional assets
- unavailable diagnostics

## Clarifications

- `ret_20d` is treated as one baseline candidate source, not as a required baseline center.
- No volatility-penalty variants were tested in this phase.
- Volatility/risk-exposure track is intentionally deferred to separate design work.
- This report compares simple candidates for consistency; it does **not** prove alpha profitability or production tradability.
