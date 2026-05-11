# Phase 17I-17K: Synthetic End-to-End Factor Research Smoke Pipeline

## Purpose

This phase adds a deterministic synthetic smoke pipeline to exercise the factor research infrastructure end to end:
synthetic panel generation -> factor building -> output validation -> diagnostics -> artifact writing.

## Synthetic-only boundary

The pipeline uses synthetic data only.
It does not call AkShare, does not call the internet, and does not use live/real market data.

## Not alpha evidence

Pipeline outputs are for integration/smoke validation only.
They are not alpha evidence and not a tradable strategy claim.

## Scope boundary

- no signal generation
- no portfolio backtest
- no benchmark comparison

## Infrastructure exercised

- Phase 17F: `build_technical_liquidity_factors`
- Phase 17G: `validate_factor_output`, `write_factor_output`
- Phase 17H: `run_basic_factor_diagnostics`

## Expected artifacts

- factor artifacts: `factors.csv`, `summary.csv`, `metadata.json`
- diagnostics artifacts:
  - `coverage.csv`
  - `distribution.csv`
  - `correlation.csv`
  - `high_correlation_pairs.csv`
  - `ic_by_date.csv`
  - `ic_summary.csv`
- run manifest: `run_manifest.json`

## Example command

```bash
python -m qsys.utils.run_factor_research_smoke \
  --output-dir ./artifacts/smoke_run \
  --n-assets 20 \
  --n-dates 90 \
  --seed 42
```
