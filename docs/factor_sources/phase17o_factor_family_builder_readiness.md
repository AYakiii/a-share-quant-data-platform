# Phase 17O: Factor Family Builder Readiness Matrix v0

## Why this phase exists

Phase 17O adds a static readiness matrix so builder implementation can be prioritized with clear constraints,
rather than promoting every raw-source family to a builder immediately.

## Taxonomy vs readiness

- Taxonomy describes *what* families exist and their intended roles.
- Readiness matrix describes *when/how safely* each family can enter builder implementation.

## Why not all families are immediately builder-ready

Many families require explicit PIT handling, event windows, and post-event blacklist controls before
safe feature construction is possible.

## Label-policy note

`fwd_ret_5d` and `fwd_ret_20d` are diagnostic labels only.
They are not intended to define the factor library architecture.

## Current readiness overview

- Ready now: `technical_liquidity`, `market_regime`, `margin_leverage`.
- Blocked until additional contracts: industry membership as-of, event windows, disclosure/PIT alignment.

## Recommended next steps

Use this matrix as the planning gate before any new family builder implementation.
Families marked `needs_pit_alignment` or `needs_event_window_design` should first add timing/window contracts,
then proceed to builder v0.

## Scope boundary

This phase is static planning/validation only.
No signal generation, no backtest, and no alpha claim is included.
