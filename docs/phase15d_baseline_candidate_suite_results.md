# Phase 15D-2: Baseline Candidate Suite Results

Date: 2026-05-08

## Run context

- Runner: `src/qsys/utils/run_baseline_candidate_suite.py`
- Feature root used: `data/processed/feature_store/v1`
- Output directory: `outputs/baseline_candidate_suite/`
- Artifacts generated:
  - `signal_quality_report.csv`
  - `run_manifest.json`
  - `warnings.md`

Because no real feature store was available in this environment initially, a local sample feature store was generated via `qsys.utils.generate_synthetic_feature_store` for this diagnostic run.

## Candidate coverage observed

Requested candidate set was:
- `ret_1d_momentum`, `ret_1d_reversal`
- `ret_5d_momentum`, `ret_5d_reversal`
- `ret_20d_momentum`, `ret_20d_reversal`

In this run, only `ret_20d_*` candidates were evaluable because sample data lacked `ret_1d` and `ret_5d` columns.

## Headline results from `signal_quality_report.csv`

Available rows (4 total):
- `ret_20d_momentum` vs `fwd_ret_5d`
- `ret_20d_momentum` vs `fwd_ret_20d`
- `ret_20d_reversal` vs `fwd_ret_5d`
- `ret_20d_reversal` vs `fwd_ret_20d`

Key patterns (this run only):
- Positive mean Rank IC:
  - `ret_20d_momentum` on both horizons
- Negative mean Rank IC:
  - `ret_20d_reversal` on both horizons
- Stronger ICIR (absolute direction considered by sign):
  - `ret_20d_momentum` is positive; `ret_20d_reversal` is symmetric negative
- Horizon comparison:
  - `fwd_ret_5d` shows slightly higher mean Rank IC / ICIR than `fwd_ret_20d` for `ret_20d_momentum`

Interpretation in plain terms:
- On this sample, momentum-style `ret_20d` ranked signal looked directionally better than reversal-style `ret_20d`.
- Both horizons tell a similar directional story, with 5-day label slightly stronger.

## Warnings and limitations (`warnings.md`)

Recorded warnings include:
- missing required feature columns:
  - `ret_1d`
  - `ret_5d`
- insufficient cross-sectional assets warnings (average assets ≈ 5.00), which is below robust research standards.

No extra risk/execution claims should be made from this run.

## Required caveats

- This is diagnostic evidence, **not proof of tradable alpha**.
- No candidate is promoted to official model in this phase.
- Volatility/risk-control design remains deferred to a separate track.
- Results depend on current sample size, available columns, and data quality.
