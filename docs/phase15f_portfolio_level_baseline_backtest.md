# Phase 15F-1: Portfolio-Level Baseline Backtest

Date: 2026-05-10

## Scope

Added a minimal portfolio-level validation runner for baseline signals using existing backtest modules.
No new alpha candidates, no volatility penalty variants, no ML, no risk optimizer.

## Implemented

- New runner: `src/qsys/utils/run_baseline_portfolio_backtest.py`
- Signals:
  - `ret_20d_reversal = -rank(ret_20d)`
  - `ret_5d_reversal = -rank(ret_5d)`
  - optional: `ret_20d_momentum = rank(ret_20d)`
- Portfolio assumptions (defaults):
  - `long_only = true`
  - `top_n = 50`
  - `rebalance = weekly`
- Cost sensitivity defaults:
  - `5 bps`
  - `10 bps`
- Artifacts:
  - `portfolio_summary.csv`
  - `daily_returns.csv`
  - `turnover.csv`
  - `run_manifest.json`
  - `warnings.md`

## Interpretation guardrails

- This is portfolio-level validation, not production trading.
- No baseline is promoted.
- Cost model is simplified `turnover × bps`.
- 5 bps and 10 bps are sensitivity settings, not exact execution costs.
- Results depend on current data sample and universe construction.
- Point-in-time universe integration into this workflow is a future improvement.
- Volatility/risk-control remains deferred.
