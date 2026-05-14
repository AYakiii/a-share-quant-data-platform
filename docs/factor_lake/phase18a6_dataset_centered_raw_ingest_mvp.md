# Phase 18A-6: Dataset-centered Raw Ingest MVP

## Why this phase exists
Probe cases verify endpoint availability, but they are not a long-term data lake abstraction.
Phase 18A-6 moves to **dataset-centered raw ingest** for first P0 datasets:
- `daily_bar_raw`
- `index_bar_raw`
- `margin_detail_raw`

## Raw-only scope
This phase is still raw ingestion infrastructure only:
- preserve API-returned raw columns
- write partitioned raw files + metadata + ingest catalog/log
- no normalized panels
- no feature/factor/signal/backtest logic

## CLI usage (tiny safe run)
```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_ingest_mvp \
  --output-root outputs/factor_lake_raw_ingest_mvp \
  --datasets daily_bar_raw,index_bar_raw,margin_detail_raw \
  --symbols 000001,600000,000858 \
  --index-symbols 000300,000905,000852 \
  --trade-dates 20240327,20240328,20240329 \
  --start-date 20240101 \
  --end-date 20240331 \
  --request-sleep 1
```

## Parameter-driven datasets
- `daily_bar_raw`: `symbols`, `start_date`, `end_date`, daily API preference (`stock_zh_a_daily`/`stock_zh_a_hist`) with fallback.
- `index_bar_raw`: `index_symbols`, `start_date`, `end_date`.
- `margin_detail_raw`: `exchanges`, `trade_dates`.

## Outputs
- raw partitions under `data/raw/akshare/...`
- per-partition metadata JSON
- metastore entries (`sync_meta`, `raw_dataset_inventory`, `ingest_run_log`)
- `outputs/factor_lake_raw_ingest_mvp/raw_ingest_catalog.csv`
- `outputs/factor_lake_raw_ingest_mvp/raw_ingest_summary.csv`

## Long-term direction
This MVP is the first dataset-centered ingest layer toward complete raw coverage of future factor/data sources.
Later phases add more source families into the same raw lake pattern before moving downstream to normalization and research layers.
