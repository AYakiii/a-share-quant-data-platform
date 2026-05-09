# Artifact Policy (Data & Outputs)

## Versioned by default

The following should be versioned in Git:
- source code (`src/`, `scripts/`)
- tests (`tests/`)
- documentation (`docs/`)
- schemas/contracts/phase notes

## Not versioned by default

The following should generally **not** be committed:
- real feature stores under `data/processed/feature_store/`
- raw/interim datasets under `data/raw/` and `data/interim/`
- generated sample/synthetic feature stores under `data/sample/feature_store/`
- generated experiment outputs under `outputs/`
- large binary artifacts such as parquet files

## Exception policy for fixtures

Small deterministic fixtures may be committed only if intentionally placed under `tests/fixtures/` (or equivalent clearly named test-fixture location) and documented in tests.

## Provenance and evidence interpretation

- Sample/synthetic outputs are pipeline-validation artifacts, not research-evidence artifacts.
- Real-data research conclusions should rely on reproducible real-data runs with explicit provenance metadata and run manifests.

## Existing committed outputs

Some sample outputs already tracked in this repository (for example under `outputs/baseline_candidate_suite/`) are treated as historical pipeline-validation artifacts.
They should not be interpreted as tradable-alpha evidence.
