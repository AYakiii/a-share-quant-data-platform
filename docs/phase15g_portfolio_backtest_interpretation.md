# Phase 15G: Portfolio Backtest Interpretation and Handoff

## Scope and objective

This phase is a handoff note for interpreting completed real-data portfolio-level validation and setting next development direction.

It does **not** introduce new alpha candidates, volatility-penalty variants, ML models, or risk optimization.

## Completed pipeline status

The following pipeline stages are now completed and connected in real-data runs:

1. CSI500 universe workflow (research pipeline)
2. Real feature store build (2023-2025, required return columns present)
3. Baseline candidate signal suite runs
4. Portfolio-level Top50 backtest runner with cost settings
5. Equal-weight benchmark comparison on the same universe/date window

## Key empirical findings

### Signal-level vs portfolio-level

- Signal-level diagnostics previously suggested reversal signals could look stronger than momentum in ranking quality views.
- Portfolio-level results showed that some strategies still produced positive absolute returns.

### Benchmark-relative conclusion

- Benchmark comparison indicates all tested Top50 strategies underperformed equal-weight over the evaluated window.
- Equal-weight benchmark metrics (approximately):
  - `total_return`: 0.3268
  - `annualized_return`: 0.0923
  - `sharpe`: 0.436
  - `max_drawdown`: -0.331
- Strategy excess return vs equal-weight is negative across tested settings:
  - `ret_20d_reversal` (5/10 bps): negative excess return
  - `ret_5d_reversal` (5/10 bps): negative excess return
  - `ret_20d_momentum` (5/10 bps): negative excess return

### Promotion decision

No current baseline should be promoted as an investable portfolio baseline at this stage.

## Interpretation

1. Positive absolute returns likely include broad market/universe beta effects rather than robust active alpha.
2. Top50 active selection did not add value versus simple equal-weight universe exposure.
3. High drawdowns and generally low Sharpe weaken tradability for current simple constructions.
4. Cost sensitivity is material; this is especially visible in `ret_5d_reversal`, where higher turnover materially degrades outcomes.

## Current limitations

1. Universe construction may not yet be fully point-in-time in all practical senses.
2. Equal-weight benchmark is a research benchmark only, not a production benchmark.
3. CSI500 index-level benchmark comparison is not yet integrated in this phase output.
4. Risk/exposure attribution is not yet included in this portfolio interpretation layer.
5. No production-grade execution microstructure model is included.

## Recommended next-phase options

A. **Point-in-time universe integration**
- Tighten constituent membership timing assumptions and audit PIT behavior end-to-end.

B. **Benchmark-relative portfolio construction**
- Shift evaluation focus to active return generation and benchmark-relative robustness instead of absolute return alone.

C. **Risk/exposure diagnostics**
- Add factor/style/industry exposure and risk decomposition diagnostics for strategy-vs-benchmark behavior.

D. **Broader portfolio rule comparison (before new factors)**
- Compare portfolio construction rules and constraints more systematically before introducing new alpha factors.

## Handoff statement

Phase 15F-3 established the benchmark-comparison requirement and showed that current Top50 baselines do not outperform equal-weight on a benchmark-relative basis.

The immediate priority is to improve portfolio construction validity and interpretation quality, not to expand factor complexity.
