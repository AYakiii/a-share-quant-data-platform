# Phase 17L-1: AkShare Industry / Theme Raw Source Adapter Expansion

## Purpose

Phase 17L-1 expands the raw-source adapter layer to include manually verified industry/theme APIs.
This phase is for raw source integration only.

## Scope boundary

These adapters are raw adapters, not factor builders.
They do not compute factors, do not generate signals, and do not run backtests.

## Relationship to earlier phases

- Phase 17C provided the source inventory map.
- Phase 17D provided the raw adapter contract (`SourceFetchResult`, metadata builder, deterministic persistence helper).
- Phase 17L-1 extends that same contract to more verified industry/theme sources.

## Included APIs

- `stock_industry_clf_hist_sw`
- `index_component_sw`
- `index_hist_sw`
- `stock_industry_change_cninfo`
- `sw_index_first_info`
- `sw_index_second_info`
- `sw_index_third_info`
- `stock_board_industry_index_ths`
- `stock_board_concept_index_ths`
- `stock_board_concept_summary_ths`

## PIT / look-ahead limitations

- `index_component_sw` field `最新权重` is a latest snapshot weight, not historical weight time series.
- THS concept/theme sources are vendor theme-style datasets and should be treated carefully in PIT workflows.
- `stock_board_concept_summary_ths` is an event summary source, not concept membership history.

## Forward path

Future phases may build industry/theme factor families using these raw sources,
but Phase 17L-1 itself is strictly raw adapter expansion.
