# Phase 17E: Raw Source Schema Snapshot & Field Role Contract

## Why this phase exists

Phase 17E defines a lightweight, static field-role contract over raw source outputs.
Its purpose is to help future factor builders distinguish safe candidate inputs from identifiers,
dates, metadata-only fields, labels, and known post-event/look-ahead risk fields.

## Relationship to prior phases

- **Phase 17C** provided source-level inventory and planning context.
- **Phase 17D** provided raw adapter fetch/persistence contracts.
- **Phase 17E** adds field-level semantic constraints on top of those layers.

## Scope boundary

This is **not** FactorRegistry.
This is **not** a factor builder.
This phase does not generate factors, signals, or backtests.

## Field roles

Allowed `field_role` values:
- `identifier`
- `date`
- `raw_value`
- `raw_text`
- `metadata`
- `label`
- `post_event_outcome`
- `forbidden_feature`
- `unknown`

## Interpreting tradable_feature_allowed

`tradable_feature_allowed=true` means a field is not blocked by this contract layer from future
candidate-feature consideration. It does **not** imply the field is alpha-effective or production-ready.

## PIT / look-ahead safety rule

Any `label`, `post_event_outcome`, or known look-ahead risk field must not be used as signal input.
In contract terms, those fields must remain `tradable_feature_allowed=false`.

## Future usage rule

Future factor builders must consult this contract before selecting feature candidates,
and must preserve PIT and anti-look-ahead discipline during feature construction.
