"""Tushare Raw acquisition dry-run skeleton."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qsys.data.sources.tushare_client import read_tushare_token
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig
from qsys.data.sources.tushare_sources import TUSHARE_SOURCE_SPECS


def load_symbols(path: str | Path) -> list[str]:
    """Load non-empty symbols from a text/CSV-like external universe file."""
    rows = Path(path).read_text(encoding="utf-8-sig").splitlines()
    symbols: list[str] = []
    for row in rows:
        first = row.split(",", 1)[0].strip()
        if first and first.lower() not in {"symbol", "ts_code"}:
            symbols.append(first)
    return symbols


def staging_root(config: TushareRawIngestConfig) -> Path:
    """Return the local staging root for a Tushare raw run."""
    return config.output_root / "data" / "raw" / config.provider


def run_tushare_raw_ingest_dry_run(config: TushareRawIngestConfig, *, require_token: bool = True) -> dict[str, Any]:
    """Validate operator inputs and return a token-free dry-run manifest."""
    if require_token:
        read_tushare_token(allow_prompt=False)
    symbols = load_symbols(config.symbols_file)
    if len(symbols) != int(config.expected_symbol_count):
        raise ValueError(f"expected_symbol_count mismatch: expected {config.expected_symbol_count}, got {len(symbols)}")
    manifest = {
        "provider": config.provider,
        "storage_schema_version": config.storage_schema_version,
        "universe_name": config.universe_name,
        "symbol_count": len(symbols),
        "start_date": config.start_date,
        "end_date": config.end_date,
        "output_root": str(config.output_root),
        "local_staging_root": str(staging_root(config)),
        "dry_run": True,
        "sources": [spec.__dict__ for spec in TUSHARE_SOURCE_SPECS],
    }
    return manifest


def manifest_json(manifest: dict[str, Any]) -> str:
    """Serialize a token-free dry-run manifest for console output."""
    return json.dumps(manifest, ensure_ascii=False, indent=2)
