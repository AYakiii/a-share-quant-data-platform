# Phase 15D-4: Data Provenance Guard for Baseline Suite Outputs

Date: 2026-05-08

## What was implemented

A minimal provenance guard was added to baseline-suite outputs so synthetic/sample runs cannot be mistaken for research evidence.

### Runner updates
- Updated `src/qsys/utils/run_baseline_candidate_suite.py` to accept:
  - `--data-source-type synthetic|real|sample|unknown`
  - default: `unknown`
- Added provenance fields to `run_manifest.json`:
  - `data_source_type`
  - `is_synthetic`
  - `research_evidence`

### Provenance semantics
- For `data_source_type in {synthetic, sample}`:
  - `is_synthetic = true`
  - `research_evidence = false`
  - `warnings.md` includes explicit pipeline-validation warning.
- For `data_source_type = unknown`:
  - explicit unknown provenance is recorded,
  - no synthetic claim is made,
  - `research_evidence` remains false.

## Refreshed sample outputs

- Re-ran baseline suite with:
  - `--data-source-type synthetic`
- Refreshed `outputs/baseline_candidate_suite/run_manifest.json` and `warnings.md` accordingly.

## Test updates

- Updated `tests/utils/test_run_baseline_candidate_suite.py` to verify:
  - synthetic runs include provenance fields,
  - synthetic runs include non-empty provenance warning (not only "No warnings recorded."),
  - unknown-source runs remain explicit and conservative.

## Required caveats

- Synthetic results are pipeline validation artifacts.
- Real-data runs are required before any research conclusion.
- No candidate is promoted.
- Volatility/risk-control remains deferred.
