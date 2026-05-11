# AkShare Free Factor Source Inventory v0

## Why this inventory exists

This file is a **static, pre-engineering source inventory** for AkShare free endpoints that were manually probed in Colab.
It defines a shared reference for what data sources are available and how they should be treated in research planning.

## Scope boundary

This is a **source inventory**, not a factor registry.

- It does not define final factor formulas.
- It does not run downloads.
- It does not alter feature store, signal diagnostics, portfolio backtest, or benchmark comparison pipelines.

## Field-role contract

Each row in the inventory captures:

- API identity (`api_name`) and source grouping (`source_family`)
- expected data nature (`data_type`)
- verification status (`status`)
- key data fields and time/symbol anchors (`key_fields`, `date_field`, `symbol_field`)
- PIT assessment (`pit_quality`)
- look-ahead risk markers (`lookahead_risk_fields`)
- research usefulness and suggested role/phase (`research_value`, `recommended_role`, `recommended_phase`)
- implementation cautions (`notes`)

## PIT and look-ahead controls are mandatory

PIT discipline and look-ahead risk tracking are required before factor construction.

Some columns are post-event outcomes and must be blacklisted from signal inputs, including:

- `fwd_ret_5d`
- `fwd_ret_20d`
- `解禁后20日涨跌幅`
- `上榜后1日`
- `上榜后2日`
- `上榜后5日`
- `上榜后10日`

## Operational note

This inventory is intentionally lightweight and machine-readable (`CSV`) so research planning code can load/validate it without AkShare runtime dependencies.
