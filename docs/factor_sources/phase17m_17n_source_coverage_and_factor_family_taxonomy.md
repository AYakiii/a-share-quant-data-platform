# Phase 17M / 17N: Raw Adapter Coverage and Factor Family Taxonomy v0

## Why this phase exists

Phase 17M/17N adds a static reporting layer so the project can track:
1) what raw adapters currently exist, and
2) how those sources map into future factor families.

## Coverage vs taxonomy

- Raw adapter coverage (`raw_adapter_coverage_v0.csv`) is implementation-facing and function-level.
- Factor family taxonomy (`factor_family_taxonomy_v0.csv`) is research-facing and family-level.

## Current raw data universe coverage

The coverage file enumerates active adapters across:
- market/index/disclosure/margin
- industry/theme
- ownership/governance
- corporate actions/unlock
- block trade/LHB/event attention

All listed adapters are raw-only and `computes_factors=false`.

## Factor family staging

Some families are ready at builder level (e.g. `technical_liquidity`), while most are
currently raw-adapter-only and require PIT/event-time handling before formal builders.

## Label-policy note

`fwd_ret_5d` and `fwd_ret_20d` are diagnostic labels only.
They should not define or constrain the factor-universe construction roadmap.

## Scope boundary

This phase adds static coverage/taxonomy reporting and validators only.
No signal generation, backtesting, or alpha claim is included.
