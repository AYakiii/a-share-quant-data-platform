# Phase 17H: Technical/Liquidity Factor Diagnostics v0

## Purpose

Phase 17H adds lightweight diagnostics for candidate factor outputs.
It evaluates coverage, missingness, distribution, correlation/redundancy, and cross-sectional IC behavior.

## Relationship to Phase 17F and 17G

- Phase 17F builds candidate technical/liquidity factors.
- Phase 17G validates/summarizes/writes factor outputs.
- Phase 17H consumes those outputs for diagnostic statistics only.

## Scope boundary

Diagnostics are **not** signals.
IC is diagnostic evidence, **not** a trading rule.
This phase does not perform portfolio construction, benchmark comparison, or backtesting.
No baseline promotion is made.

## Diagnostic definitions

- Coverage: non-null share and missingness by factor.
- Distribution: mean/std/min/max and quantiles (p01/p05/p25/median/p75/p95/p99).
- Correlation: pairwise factor correlation matrix (`pearson`/`spearman`).
- Redundancy: high-correlation factor pairs from upper triangle.
- IC by date: cross-sectional correlation between factor and label per date.
- IC summary: mean/median/std, ICIR, t-stat, positive rate, sample size.

## No-lookahead and safety discipline

Labels are used only in diagnostic IC computations, never as factor inputs.
This phase does not generate signal ranks or portfolio weights.

## Future extensions

Future phases may add richer diagnostics (regime conditioning, robustness slices, report integration),
but those are intentionally out of scope for Phase 17H v0.
