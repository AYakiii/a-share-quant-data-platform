# Phase U1: RawWarehouse Core Upgrade

## Why upgrade RawWarehouse Core
RawWarehouse is upgraded from a margin-only prototype into a multi-source raw ingestion core so future Raw DWH sources can share one operational runner (retry/timeout/cache/artifacts/manifest) without source-specific engine rewrites.

## Registry direction
`margin_detail` remains fully supported, but is now one registered `SourceSpec` in a `SOURCE_SPECS` registry. `stock_zh_a_daily` is added as the second registered source to validate multi-source ingestion behavior.

## Why preserve the existing Colab workbench fallback
The current production Colab workflow around `run_factor_lake_raw_ingest.py` and `factor_lake/raw_ingest.py` is intentionally untouched in this phase to avoid operational risk to already validated Drive datasets.

## Borrowed ideas from factor_lake/raw_ingest
Borrowed operational concepts:
- per-partition status records
- started/finished timing and elapsed fields
- attempts and error metadata fields
- failed/empty/timed_out/skipped separation
- warnings + manifest artifacts
- acquisition policy metadata and `include_disabled`
- `operation_events.jsonl`

## Intentionally not migrated
- no coupling to factor_lake modules
- no migration of raw_ingest catalog schema into warehouse artifacts
- no full API registry migration from factor_lake
- no Drive publish/commit behavior

## Example CLI
Margin detail:
```bash
PYTHONPATH=src python -m qsys.utils.build_raw_warehouse \
  --source margin_detail \
  --start-date 2024-01-01 \
  --end-date 2024-01-10 \
  --raw-root data/raw \
  --output-dir outputs/raw_warehouse \
  --exchanges both
```

Stock daily smoke:
```bash
PYTHONPATH=src python -m qsys.utils.build_raw_warehouse \
  --source stock_zh_a_daily \
  --symbols 000001,000002 \
  --start-date 2026-01-01 \
  --end-date 2026-01-10 \
  --raw-root /content/raw_warehouse_stage/raw \
  --output-dir /content/raw_warehouse_stage/outputs \
  --run-name stock_zh_a_daily_smoke \
  --request-timeout 30 \
  --retries 1 \
  --request-sleep 0.8 \
  --show-progress
```

## Drive safety rule
No production Drive write is performed by default in this phase.
