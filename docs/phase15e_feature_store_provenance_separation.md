# Phase 15E-2: Feature Store Path & Provenance Separation

Date: 2026-05-08

## What changed

To reduce accidental mislabeling between synthetic/sample and real feature stores:

1. Synthetic generator default output path was changed from:
   - `data/processed/feature_store/v1`
   to:
   - `data/sample/feature_store/v1`

2. Synthetic generator now writes provenance metadata:
   - `_feature_store_provenance.json`

3. Baseline candidate suite now checks provenance metadata when present.

## Provenance metadata written for synthetic store

`_feature_store_provenance.json` includes:
- `data_source_type: synthetic`
- `is_synthetic: true`
- `research_evidence: false`
- `generated_by: generate_synthetic_feature_store.py`

## Baseline suite behavior updates

- If `--data-source-type` is omitted, runner infers source type from `_feature_store_provenance.json` when available.
- If CLI says `real` but metadata says synthetic/sample, runner emits conflict warning and treats run as non-research evidence.
- Runner does not silently treat synthetic/sample data as real evidence.

## Documentation updates

- README now distinguishes real/processed path vs synthetic/sample path.
- Phase 15E-1 note updated with path-separation follow-up.

## Scope reminders

- No new alpha candidates.
- No volatility-penalty variants.
- No ML or risk-control additions.
- No baseline promotion.
