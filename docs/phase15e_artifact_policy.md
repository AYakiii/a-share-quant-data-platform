# Phase 15E-3: Data and Output Artifact Policy

Date: 2026-05-08

## What was done

1. Updated `.gitignore` to reduce accidental commits of generated data/output artifacts:
   - `data/raw/`
   - `data/interim/`
   - `data/processed/feature_store/`
   - `data/sample/feature_store/`
   - `outputs/`
   - `*.parquet`
   - local caches (`.pytest_cache/`, `__pycache__/`, `.ipynb_checkpoints/`)

2. Added `docs/artifact_policy.md` to define what should and should not be versioned.

3. Kept existing committed outputs in place (no deletion in this phase), and documented them as sample/pipeline-validation artifacts rather than research evidence.

## Why this matters

- Prevents accidental repository growth from large generated artifacts.
- Reduces risk of synthetic/sample outputs being mistaken for production-grade research evidence.
- Preserves clear boundaries: code/docs/tests are versioned; generated data/outputs are generally not.

## Scope reminders

- No new alpha candidates.
- No volatility-penalty variants.
- No ML/risk-control additions.
- No baseline promotion.
