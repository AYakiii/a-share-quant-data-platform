# Phase 15D-3: Baseline Suite Data Coverage Fix

Date: 2026-05-08

## What changed

To improve diagnostic coverage in a controlled sample environment, synthetic feature-store generation was updated so baseline-suite required columns are present and sample size is less trivial.

### Generator updates
- Updated `src/qsys/utils/generate_synthetic_feature_store.py` to include:
  - `ret_1d`
  - `ret_5d`
  - `ret_20d`
  - `fwd_ret_5d`
  - `fwd_ret_20d`
- Increased default synthetic coverage to:
  - `n_assets=30`
  - `periods=80`

### Baseline suite rerun
- Regenerated sample feature store under `data/processed/feature_store/v1`.
- Re-ran baseline candidate suite to `outputs/baseline_candidate_suite/`.
- Confirmed all six required candidates now appear in `signal_quality_report.csv`.
- Confirmed horizons include both `fwd_ret_5d` and `fwd_ret_20d`.
- Confirmed `warnings.md` no longer reports missing `ret_1d` / `ret_5d`.

## Test updates

- Added `tests/utils/test_generate_synthetic_feature_store.py` to verify:
  - required baseline-suite columns are generated,
  - default synthetic coverage is nontrivial (assets/dates threshold checks).

## Scope and caveats

- This phase improves **diagnostic coverage**, not alpha validity.
- Synthetic/sample results are **not evidence of tradable alpha**.
- No baseline is promoted to official model.
- Volatility/risk-control remains deferred to a separate design track.
