"""Tushare Raw acquisition dry-run skeleton."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from qsys.data.factor_lake.raw_compact import validate_path_segment
from qsys.data.sources.tushare_client import read_tushare_token
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig
from qsys.data.sources.tushare_sources import TUSHARE_SOURCE_SPECS

TUSHARE_SYMBOL_RE = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")
DATE_RE = re.compile(r"^\d{8}$")


def file_sha256(path: str | Path) -> str:
    """Compute SHA-256 for the external universe file without logging contents."""
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_symbols(path: str | Path) -> list[str]:
    """Load and validate non-empty Tushare symbols from an external universe file."""
    rows = Path(path).read_text(encoding="utf-8-sig").splitlines()
    symbols: list[str] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows, start=1):
        first = row.split(",", 1)[0].strip()
        if idx == 1 and first.lower() in {"symbol", "ts_code"}:
            continue
        if not first:
            raise ValueError(f"empty symbol at line {idx}")
        if not TUSHARE_SYMBOL_RE.fullmatch(first):
            raise ValueError(f"illegal Tushare symbol at line {idx}: {first!r}")
        if first in seen:
            raise ValueError(f"duplicate symbol in universe file: {first}")
        seen.add(first)
        symbols.append(first)
    if not symbols:
        raise ValueError("symbols_file must contain at least one symbol")
    return symbols


def staging_root(config: TushareRawIngestConfig) -> Path:
    """Return the local staging root for a Tushare raw run."""
    return config.output_root / "data" / "raw" / config.provider


def run_tushare_raw_ingest_dry_run(config: TushareRawIngestConfig, *, require_token: bool = True) -> dict[str, Any]:
    """Validate operator inputs and return a token-free dry-run manifest."""
    if config.provider != "tushare":
        raise ValueError("Tushare raw ingest config provider must be 'tushare'")
    if not str(config.universe_name or "").strip():
        raise ValueError("universe_name is required")
    if int(config.expected_symbol_count) <= 0:
        raise ValueError("expected_symbol_count must be > 0")
    if not DATE_RE.fullmatch(config.start_date or "") or not DATE_RE.fullmatch(config.end_date or ""):
        raise ValueError("start_date and end_date must be YYYYMMDD")
    if config.start_date > config.end_date:
        raise ValueError("start_date must be <= end_date")
    storage_schema_version = validate_path_segment(config.storage_schema_version, label="storage_schema_version")
    if require_token:
        read_tushare_token(allow_prompt=False)
    symbols = load_symbols(config.symbols_file)
    if len(symbols) != int(config.expected_symbol_count):
        raise ValueError(f"expected_symbol_count mismatch: expected {config.expected_symbol_count}, got {len(symbols)}")
    manifest = {
        "provider": config.provider,
        "storage_schema_version": storage_schema_version,
        "universe_name": config.universe_name,
        "symbols_file": str(config.symbols_file),
        "universe_sha256": file_sha256(config.symbols_file),
        "symbol_row_count": len(symbols),
        "unique_symbol_count": len(set(symbols)),
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
