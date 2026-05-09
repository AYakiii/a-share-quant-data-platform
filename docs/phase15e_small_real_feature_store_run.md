# Phase 15E-4: Small Real Feature Store Run (Smoke Attempt)

Date: 2026-05-08

## Objective

Attempt a small real feature-store build and, if successful, run baseline candidate suite with `--data-source-type real`.

## Command attempted

```bash
PYTHONPATH=src python -m qsys.utils.build_real_feature_store \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2025-01-01 \
  --limit 30
```

## Result

The real-data build **failed** before feature-store creation due to network/proxy access failure while calling AkShare upstream endpoints.

Observed error (key points):
- `requests.exceptions.ProxyError`
- underlying tunnel/proxy error: `Tunnel connection failed: 403 Forbidden`
- failure occurred during symbol-universe fetch (`ak.stock_zh_a_spot_em()`), so no real dataset was built.

## Consequence

Per instruction, baseline suite was **not** run as `--data-source-type real` after this failure.
No silent fallback to synthetic/sample data was used.

## Dependency / access issue identified

- AkShare dependency is installed, but outbound data-source access is blocked by proxy/network policy in this environment.
- This is an environment connectivity issue, not a fake-success condition.

## What remains for completion

Once network/proxy access is available:

1. Build real store:
```bash
PYTHONPATH=src python -m qsys.utils.build_real_feature_store \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2025-01-01 \
  --limit 30
```

2. Run baseline suite on real store:
```bash
PYTHONPATH=src python -m qsys.utils.run_baseline_candidate_suite \
  --feature-root data/processed/feature_store/v1 \
  --output-dir outputs/baseline_candidate_suite_real \
  --data-source-type real
```

3. Validate artifacts:
- `signal_quality_report.csv`
- `run_manifest.json`
- `warnings.md`

## Required caveats

- This was a small real-data smoke attempt, but build did not complete.
- No baseline is promoted.
- Any eventual results are diagnostic only, not tradable-alpha proof.
- Volatility/risk-control remains deferred.
- Production tradability is not evaluated.
