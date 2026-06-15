"""Contracts for local-only Tushare raw acquisition."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TushareRawIngestConfig:
    """Runtime configuration supplied by the operator for a Tushare raw run."""

    symbols_file: Path
    universe_name: str
    start_date: str
    end_date: str
    output_root: Path
    dataset_version: str
    api_names: tuple[str, ...] = ()
    families: tuple[str, ...] = ()
    expected_symbol_count: int | None = None
    max_workers: int = 1
    request_sleep: float = 0.3
    request_jitter: float = 0.0
    retry: int = 2
    dry_run: bool = True
    resume: bool = False
    allow_candidate_sources: bool = False
    heartbeat_sec: float | None = 30.0
    dates_file: Path | None = None
    provider: str = "tushare"


@dataclass(frozen=True)
class TushareSourceSpec:
    """Source registry row for a Tushare raw API."""

    source_family: str
    api_name: str
    partition_key: str = "trade_date"
    query_mode: str = "by_trade_date"
    fetch_mode: str = "trade_date_full_market"
    primary_key: tuple[str, ...] = ("ts_code", "trade_date")
    fields: tuple[str, ...] = ()
    calendar_mode: str = "trading_days"
    universe_filter_mode: str = "ts_code"
    compact_bucket: str = "year_from_trade_date"
    status: str = "approved"
    production_enabled: bool = True
