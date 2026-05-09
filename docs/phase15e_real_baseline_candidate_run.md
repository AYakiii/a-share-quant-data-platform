# Phase 15E-1: Real Feature Store Baseline Run Status

Date: 2026-05-08

## Objective check

Goal was to run baseline candidate suite on **real or real-derived** feature-store data (not synthetic/sample) and summarize cautiously.

## What was inspected

1. `data/processed/feature_store/v1`
2. README real-data instructions
3. `src/qsys/utils/build_real_feature_store.py`
4. Current partition schema in existing local feature-store files

## Findings

- A feature-store directory exists at `data/processed/feature_store/v1` with 80 partitions.
- Current files in that path contain columns like:
  - `date`, `asset`, `ret_1d`, `ret_5d`, `ret_20d`, `vol_20d`, `fwd_ret_5d`, `fwd_ret_20d`, `market_cap`, `amount_20d`
- This schema matches the project synthetic/sample generator flow, not the documented real builder output shape.
- The real-data builder (`build_real_feature_store.py`) writes a richer schema including:
  - `trade_date`, `ts_code`, `open/high/low/close`, `volume`, `amount`, `turnover`, `outstanding_share`, `is_tradable`, and derived return columns.

### Conclusion on real-data availability

A verified real feature store is **not currently available** in this environment snapshot.
The existing `data/processed/feature_store/v1` appears synthetic/sample-derived.

## Action taken

Per instruction, did **not** run a `--data-source-type real` baseline suite on synthetic/sample data.
No silent fallback was used.

## Exact command needed to generate real feature store

From README + utility:

```bash
PYTHONPATH=src python -m qsys.utils.build_real_feature_store \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2020-01-01 \
  --limit 300
```

Optional tuning flags from utility:
- `--end-date YYYY-MM-DD`
- `--retries`
- `--retry-wait`
- `--request-sleep`
- `--symbols` (explicit symbol list)

## What remains for Phase 15E once real data is built

1. Run baseline suite with explicit real provenance:

```bash
PYTHONPATH=src python -m qsys.utils.run_baseline_candidate_suite \
  --feature-root data/processed/feature_store/v1 \
  --output-dir outputs/baseline_candidate_suite_real \
  --data-source-type real
```

2. Validate artifacts:
- `signal_quality_report.csv`
- `run_manifest.json`
- `warnings.md`

3. Summarize as first-pass diagnostics only:
- no baseline promotion,
- volatility/risk-control still deferred,
- production tradability not evaluated.

## Path-separation follow-up

- Real/processed feature-store path should remain:
  - `data/processed/feature_store/v1`
- Synthetic/sample feature-store path should be separated as:
  - `data/sample/feature_store/v1`

## Required caveats

- This phase produced a readiness/status assessment, not real-run performance evidence.
- Any real-data conclusions will depend on successful AkShare ingestion and actual coverage quality.
