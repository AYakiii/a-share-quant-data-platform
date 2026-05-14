# Phase 18A-7: Broad Raw Source Coverage Expansion

## Why this phase
The project has moved beyond initial P0 endpoint checks.
Priority now is broad raw source coverage across families so future factor/data sources are connected into the Raw Factor Lake first.

## Strategy
Coverage-first execution means:
- run tiny representative requests across many source families
- record `success`, `failed`, `empty`, `pending_adapter`
- do not block whole run on one API failure
- audit results in catalog/summary for follow-up waves

## In-scope families
- market_price
- index_market
- margin_leverage
- financial_fundamental
- industry_concept
- event_ownership
- corporate_action
- disclosure_ir
- trading_attention

## CLI
```bash
PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_coverage_ingest \
  --output-root outputs/factor_lake_raw_coverage \
  --families market_price,index_market,margin_leverage,financial_fundamental,industry_concept,event_ownership,corporate_action,disclosure_ir,trading_attention \
  --symbols 000001,600000 \
  --index-symbols 000300,000905 \
  --report-dates 20240331 \
  --trade-dates 20240328,20240329 \
  --industry-names 半导体 \
  --concept-names AI\ PC \
  --start-date 20240101 \
  --end-date 20240331 \
  --request-sleep 1 \
  --continue-on-error
```

## Outputs
- raw partitions under `output_root/data/raw/akshare/...`
- per-partition `metadata.json`
- `output_root/raw_ingest_catalog.csv`
- `output_root/raw_ingest_summary.csv`

## Out of scope
- normalized panels
- feature/factor store
- factor construction
- signal diagnostics/backtests

## Next steps
After broad coverage is mapped and audited, ingest can progress into staged historical backfill waves (not full 2010–2026 by default in one run).
