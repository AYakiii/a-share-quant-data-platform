# Phase 17G: Factor Output Contract v0

## Why this phase exists

Phase 17G defines a lightweight contract for validating and persisting candidate factor outputs.
This prevents unsafe columns and malformed output frames from silently flowing into later research steps.

## Relationship to Phase 17F

Phase 17F builds candidate technical/liquidity factor columns.
Phase 17G standardizes how those outputs are validated, summarized, and saved.

## Scope boundary

This phase is **not** signal generation.
This phase is **not** factor diagnostics.
This phase is **not** backtesting.

## Factor output safety requirements

- factor output must be a `DataFrame` with `MultiIndex [date, asset]`
- no duplicate index entries
- factor columns must be numeric
- `inf` / `-inf` are disallowed
- all-NaN columns should be flagged unless explicitly allowed

## Forbidden fields and no-lookahead discipline

Known label/post-event fields must not appear in factor output:
- `fwd_ret_5d`
- `fwd_ret_20d`
- `解禁后20日涨跌幅`
- `上榜后1日`, `上榜后2日`, `上榜后5日`, `上榜后10日`

## Why raw input columns should not appear in factor output

Columns such as `open/high/low/close/volume/amount/turnover` belong to raw panel inputs.
Factor outputs should contain derived candidate factors only, to keep downstream contracts clean.

## Forward path

A future Phase 17H can consume this contract layer for diagnostics and richer evaluation workflows,
while keeping this phase focused on output integrity and persistence.
