# Phase 17L-2 / 17L-3: Ownership, Corporate Action, Trading Attention, Event Raw Adapters

## Purpose

Phase 17L-2 / 17L-3 expands raw-source adapter coverage for manually verified free AkShare ownership,
corporate action, trading-attention, and event-attention sources.

## Scope boundary

These are raw adapters, not factor builders.
This phase does not compute factors, generate signals, run diagnostics, or run backtests.

## Relationship to prior phases

- Phase 17C: source inventory map and source roles.
- Phase 17D: base raw adapter contract (`SourceFetchResult`, metadata contract).
- Phase 17L-2 / 17L-3: additional raw-source coverage using the same adapter contract.

## Included source families

- ownership structure
- governance / pledge risk
- corporate actions
- restricted-share unlock
- block trade
- LHB / abnormal trading attention
- institution attention
- fundamental event / earnings guidance

## PIT / look-ahead limitations

- `解禁后20日涨跌幅` is a post-event outcome field and must not be used as signal input.
- `上榜后1日/2日/5日/10日` are post-event outcome fields and must not be used as signal input.
- Dividend-related multi-date fields require careful event-time interpretation.
- Announcement dates should generally be preferred when available for timing alignment.

## Forward path

Future phases may transform these raw sources into factor families,
but this phase is strictly raw adapter expansion.
