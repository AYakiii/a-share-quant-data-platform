# Phase 17 Final Factor Library Handoff (17U)

## 1) What Phase 17 accomplished

Phase 17 established a deterministic, contract-governed factor-research foundation from raw-source ingestion to candidate factor outputs, with strict boundary controls against look-ahead and post-event leakage.

## 2) Implemented layers

- Source inventory layer
- Raw adapter layer
- Schema / field-role contract layer
- Candidate factor builder layer
- Factor output contract layer
- Diagnostics layer (research-only)
- Synthetic smoke pipeline layer
- Coverage / taxonomy / readiness governance layer
- Builder coverage matrix
- Industry as-of and event-window contracts

## 3) Current implemented builders

- technical_liquidity
- margin_leverage
- market_regime

## 4) Raw source families now covered

Market, index, industry/theme, ownership/corporate, trading attention/events, disclosure, margin, and related corporate-action/event datasets included in Phase 17 adapter scope.

## 5) Why `fwd_ret_5d` / `fwd_ret_20d` are diagnostic labels only

These columns are future-outcome labels used only for diagnostics/evaluation alignment, not as feature inputs for builder outputs.

## 6) Families ready for future builders

Families marked ready in the readiness + coverage artifacts can move to controlled real-source panelization in Phase 18, starting with safe builder families and explicit data contracts.

## 7) Families requiring industry-asof or event-window design

Industry-structure and event-driven families require explicit as-of membership logic and event-window PIT design before real-data builder expansion.

## 8) Explicit forbidden-field examples

- fwd_ret_5d
- fwd_ret_20d
- 解禁后20日涨跌幅
- 上榜后1日
- 上榜后2日
- 上榜后5日
- 上榜后10日

## 9) Recommended next phase

- Phase 18 should start in a new window.
- Do not start with portfolio/backtest.
- Suggested Phase 18A: real-data normalized source panels / real-source runner for selected safe builders.

## 10) Scope warnings

- no alpha claim
- no baseline promotion
- no live trading
- no backtest result should be inferred from Phase 17
