from __future__ import annotations

import os
from pathlib import Path

import pytest

from qsys.data.sources.tushare_acquisition import run_tushare_raw_ingest_dry_run
from qsys.data.sources.tushare_client import read_tushare_token
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig


def _cfg(tmp_path: Path, symbols_file: Path) -> TushareRawIngestConfig:
    return TushareRawIngestConfig(
        symbols_file=symbols_file,
        universe_name="external_universe",
        expected_symbol_count=2,
        start_date="20240101",
        end_date="20240131",
        output_root=tmp_path / "out",
    )


def test_tushare_token_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN"):
        read_tushare_token()


def test_tushare_dry_run_token_free_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("symbol\n000001.SZ\n000002.SZ\n", encoding="utf-8")
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))
    printed = capsys.readouterr().out
    assert "secret-token-value" not in str(manifest)
    assert "secret-token-value" not in printed
    assert manifest["provider"] == "tushare"
    assert manifest["storage_schema_version"] == "v1"
    assert manifest["local_staging_root"].endswith("data/raw/tushare")


def test_tushare_expected_symbol_count_validates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("000001.SZ\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expected_symbol_count mismatch"):
        run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))
