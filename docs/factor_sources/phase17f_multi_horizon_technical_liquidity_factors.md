# Phase 17F: Multi-Horizon Technical & Liquidity Factor Builder v0

## Purpose

Phase 17F adds a deterministic candidate factor builder for technical and liquidity families
from OHLCV-style panel data.

## Scope boundary

This module is a **candidate factor builder**, not a signal engine.
It does not perform ranking, portfolio construction, backtesting, benchmark comparison, or alpha claims.

## Why multi-horizon design

A single 20d horizon can be too narrow for low/mid-frequency research.
This phase uses short/medium/long windows to support later stability and regime diagnostics:
- short: 5d
- medium: 20d
- long: 60d

## Default windows

```python
{
  "return": [5, 20, 60],
  "volatility": [20, 60],
  "liquidity": [5, 20, 60],
  "drawdown": [20, 60],
  "range": [20, 60],
}
```

## Factor families and formulas

- Return/Trend: `ret_Xd`, `momentum_Xd`, `reversal_Xd`
- Risk: realized volatility, downside volatility, max drawdown
- Liquidity: amount/turnover means, 5d-vs-20d shocks, Amihud illiquidity
- Range/Position: high-low range, close-to-high

All calculations are asset-wise, rolling, and use `min_periods=window`.

## PIT and no-lookahead discipline

All factors are computed using current and historical observations only.
No future rows are referenced.
No label/post-event outcomes are generated or consumed.

## Relationship to Phase 17C / 17D / 17E

- 17C: source inventory and planning map
- 17D: raw source adapter contract
- 17E: field-role safety contract
- 17F: candidate factor construction infrastructure only

## Next-step diagnostics (not in this phase)

Future diagnostics should evaluate factor stability, redundancy, IC quality, inter-factor correlation,
and regime behavior. This phase intentionally does not implement diagnostics.
