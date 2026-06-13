"""Provider-neutral Tushare raw-ingest contract skeletons.

This module intentionally contains no universe-specific constants and performs no
historical API pulls.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TushareRawIngestConfig:
    """Runtime configuration supplied by the operator for a Tushare dry run."""

    symbols_file: Path
    universe_name: str
    expected_symbol_count: int
    start_date: str
    end_date: str
    output_root: Path
    provider: str = "tushare"
    storage_schema_version: str = "v1"
    dry_run: bool = True


@dataclass(frozen=True)
class TushareSourceSpec:
    """Minimal source registry row for future Tushare raw APIs."""

    source_family: str
    api_name: str
    bucket_kind: str = "year"
