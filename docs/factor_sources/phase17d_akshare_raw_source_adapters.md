# Phase 17D Pilot: AkShare Raw Source Adapter Layer

## Purpose

Phase 17D introduces a thin **raw source adapter layer** for manually verified AkShare endpoints.
The goal is deterministic raw-data ingestion contracts, not factor modeling.

## Scope boundary

This layer is **not**:
- a factor builder,
- a signal engine,
- a backtest module,
- a benchmark/report/manifest behavior change.

It only fetches raw tables, records minimal metadata, and persists artifacts deterministically.

## Relationship to Phase 17C

Phase 17C delivered the static source inventory (`akshare_free_factor_source_inventory_v0`).
Phase 17D consumes that planning direction and implements pilot adapters for a subset of verified APIs.

## Pilot APIs included

1. `stock_zh_a_hist`
2. `stock_zh_index_hist_csindex`
3. `stock_yysj_em`
4. `stock_margin_detail_sse`
5. `stock_margin_detail_szse`

## PIT / look-ahead discipline

Adapters in this phase do not construct factors and do not compute post-event labels.
They preserve source raw columns and only attach minimal fetch metadata.
PIT and look-ahead controls remain mandatory in downstream research layers.

## SZSE margin detail date behavior

For `stock_margin_detail_szse`, if raw output does not contain a date column,
the adapter injects a normalized `trade_date` column using the input `date` parameter.
This keeps temporal provenance explicit while preserving all original raw columns.

## Expansion policy

Future adapters should be added gradually and only for manually verified sources,
following the same contract:
- preserve raw columns,
- add minimal metadata,
- deterministic persistence,
- no strategy-performance claims.
