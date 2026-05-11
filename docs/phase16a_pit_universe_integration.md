# Phase 16A: Point-in-Time CSI500 Universe Integration

## Objective

Integrate point-in-time (PIT) CSI500 membership filtering so each trade date only uses securities that were members as of that date.

## What was implemented

- Added PIT membership masking support using index constituent snapshots loaded from:
  - `data/raw/index_constituents/baostock/index_name=csi500/year=*/data.parquet`
- PIT as-of logic uses **latest `snapshot_date <= trade_date`** and never uses future membership snapshots.
- Added helper to filter feature-store frames by PIT membership per date.
- Added optional CLI/runtime switches for baseline diagnostics and portfolio backtest runners:
  - `--use-pit-universe`
  - `--universe-root`
  - `--index-name` (default `csi500`)

## Why this matters

This directly addresses universe timing and survivorship-risk concerns in prior non-PIT runs.

## Interpretation guardrails

- PIT integration improves evaluation validity but does **not** prove alpha.
- All prior non-PIT results should be treated as preliminary.
- After PIT integration, baseline candidate suite and portfolio backtest should be rerun to refresh empirical conclusions.
