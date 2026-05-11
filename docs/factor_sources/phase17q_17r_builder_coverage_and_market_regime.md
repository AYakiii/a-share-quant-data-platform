# Phase 17Q / 17R: Builder Coverage Matrix and Market Regime Builder v0

## Why this phase exists

Phase 17Q introduces a lightweight, static factor-builder coverage matrix to summarize what builders exist and their safe-use status.
Phase 17R adds a deterministic market-regime candidate factor builder over normalized index-history inputs.

## Coverage matrix vs FactorRegistry

The builder coverage matrix is documentation and validation metadata for research governance.
It is not a production FactorRegistry and does not orchestrate runtime execution.

## Market regime builder boundary

`build_market_regime_factors` is a candidate factor builder, not a signal engine.
No signal generation, no portfolio construction, no backtest logic, and no benchmark comparison are included.

## Intended usage

Market-regime factors may later be used as conditioning/state variables in downstream research phases.
This phase only defines deterministic factor columns.

## Relationship to prior Phase 17 artifacts

This phase complements:
- source inventory;
- raw adapter coverage;
- factor family taxonomy;
- family builder readiness matrix.

The new matrix provides builder-level implementation coverage while prior artifacts describe source and family readiness surfaces.

## Label handling

`fwd_ret_5d` and `fwd_ret_20d` remain diagnostic labels only and are not feature inputs for the builder.
