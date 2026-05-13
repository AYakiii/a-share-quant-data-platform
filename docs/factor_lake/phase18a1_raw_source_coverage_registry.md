# Phase 18A-1: Raw Source Coverage Registry Expansion

## Why Raw Factor Lake first
The project is intentionally prioritizing **raw-source coverage** before feature research because historical completeness and point-in-time data availability are foundational for any 2010–2026 backfill.

In short:
1. map what can be fetched,
2. define how it is partitioned/stored,
3. track look-ahead risks,
4. only then build normalized layers and factor logic.

## Why this phase does not build normalized panels
This phase expands the source capability map only. It does **not** perform cross-source field alignment, schema harmonization, or business cleaning. Raw ingest should preserve original API-returned columns.

## Why this phase does not build factors/signals/backtests
Factors, signals, and backtests depend on stable and complete upstream raw coverage. Phase 18A focuses on the data foundation, not research strategy construction.

## Source family coverage
The expanded registry now maps raw capabilities across:
- market_price
- index_market
- margin_leverage
- financial_fundamental
- industry_concept
- event_ownership
- disclosure_ir
- corporate_action
- trading_attention

Each dataset-centered row records:
- `dataset_name`, `source`, `source_family`, `api_name`, `adapter_function`
- `frequency`, `fetch_granularity`, `partition_keys`
- `date_field`, `symbol_field`, `report_period_field`, `announcement_date_field`
- `normalized_target`, `factor_family_target`, `lookahead_risk_fields`
- `priority`, `notes`

## Data shape guidance
The registry includes explicit notes for common shapes:
- daily panel sources (equity/index OHLCV)
- event tables (block trade, LHB, margin detail)
- report-period snapshot sources (dividend/unlock snapshots)
- ownership/governance sources (holder structure, pledge detail)

## Exporting source capability table
Command:

```bash
PYTHONPATH=src python -m qsys.utils.export_factor_lake_registry --output-root .
```

Expected output:
- `outputs/factor_lake_registry/source_capability_registry.csv`

## How this supports future 2010–2026 backfill
The registry provides an execution-ready planning map for:
- adapter coverage checks,
- per-source partition strategy,
- PIT risk annotations,
- staged backfill ordering by `priority`.

It is the reference inventory before scaling up long-horizon ingest.
