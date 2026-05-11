# Phase 17P: Margin Leverage Factor Builder v0

## Why this phase exists

Phase 17P expands the factor library beyond price-volume by adding a deterministic margin-leverage
candidate factor builder over normalized margin panel data.

## Relationship to Phase 17O readiness

In Phase 17O, `margin_leverage` is marked as builder-ready. This phase implements that first builder v0.

## Scope boundary

Margin leverage outputs are candidate factors, not trading signals.
No portfolio backtest, benchmark comparison, or alpha claim is included.

## Input assumptions

This builder assumes normalized margin panel input with MultiIndex `[date, asset]`.
It does not implement SSE/SZSE raw normalization in this phase.

## Factor formulas

- Level: `financing_balance`, `margin_total_balance`
- Change: `x / x.shift(w) - 1`
- Activity means: rolling mean over 5d/20d
- Shock: `mean_5d / mean_20d - 1` with non-positive denominators mapped to NaN
- Optional extensions include net-buy and short-side factors when raw columns exist.

## PIT and no-lookahead discipline

All computations are asset-wise and use only current/past rows.
No forward labels or post-event outcomes are consumed.

## Future work

Future phases may add normalized raw margin transforms from SSE/SZSE adapters and richer
cross-source leverage-family builders, but that is out of scope here.
