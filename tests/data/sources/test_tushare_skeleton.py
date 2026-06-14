from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

from qsys.data.sources.tushare_acquisition import canonical_symbol_from_ts_code, run_tushare_raw_ingest_dry_run
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
        dataset_version="v1_csi500_2021_2025_union",
    )


def test_tushare_token_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN"):
        read_tushare_token()


def test_tushare_dry_run_token_free_manifest_with_universe_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    content = "symbol\n000008\n600000\n"
    symbols.write_text(content, encoding="utf-8")
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))
    printed = capsys.readouterr().out
    assert "secret-token-value" not in str(manifest)
    assert "secret-token-value" not in printed
    assert manifest["provider"] == "tushare"
    assert manifest["dataset_version"] == "v1_csi500_2021_2025_union"
    assert manifest["symbols_file"] == str(symbols)
    assert manifest["universe_sha256"] == hashlib.sha256(content.encode("utf-8")).hexdigest()
    assert manifest["symbol_row_count"] == 2
    assert manifest["unique_symbol_count"] == 2
    assert manifest["symbol_input_format"] == "canonical_symbol"
    assert manifest["local_staging_root"].endswith("data/raw/tushare")


def test_tushare_expected_symbol_count_validates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("000008\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expected_symbol_count mismatch"):
        run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))


@pytest.mark.parametrize("content, message", [
    ("000008\n000008\n", "duplicate canonical symbol"),
    ("000008\n\n600000\n", "empty symbol"),
    ("000008.SZ\n600000\n", "illegal canonical symbol"),
])
def test_tushare_universe_rejects_bad_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str, message: str) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))


def test_tushare_cli_requires_dataset_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("000008\n", encoding="utf-8")
    proc = subprocess.run([
        sys.executable,
        "-m",
        "qsys.utils.run_tushare_raw_ingest",
        "--dry-run",
        "--symbols-file",
        str(symbols),
        "--universe-name",
        "external_universe",
        "--expected-symbol-count",
        "1",
        "--start-date",
        "20240101",
        "--end-date",
        "20240131",
        "--output-root",
        str(tmp_path / "out"),
    ], capture_output=True, text=True, env={**__import__("os").environ, "PYTHONPATH": "src"})
    assert proc.returncode != 0
    assert "--dataset-version" in proc.stderr


def test_tushare_cli_does_not_accept_provider_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("000008\n", encoding="utf-8")
    proc = subprocess.run([
        sys.executable,
        "-m",
        "qsys.utils.run_tushare_raw_ingest",
        "--dry-run",
        "--symbols-file",
        str(symbols),
        "--universe-name",
        "external_universe",
        "--expected-symbol-count",
        "1",
        "--start-date",
        "20240101",
        "--end-date",
        "20240131",
        "--output-root",
        str(tmp_path / "out"),
        "--dataset-version",
        "v1_csi500_2021_2025_union",
        "--provider",
        "akshare",
    ], capture_output=True, text=True, env={**__import__("os").environ, "PYTHONPATH": "src"})
    assert proc.returncode != 0
    assert "unrecognized arguments: --provider" in proc.stderr


def test_canonical_symbol_from_ts_code_helper() -> None:
    assert canonical_symbol_from_ts_code("000008.SZ") == "000008"
    assert canonical_symbol_from_ts_code("600000.SH") == "600000"


def test_official_style_canonical_symbols_need_no_rewrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.txt"
    content = "000008\n600000\n"
    symbols.write_text(content, encoding="utf-8")
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols))
    assert manifest["symbol_input_format"] == "canonical_symbol"
    assert manifest["universe_sha256"] == hashlib.sha256(content.encode("utf-8")).hexdigest()
    assert not (tmp_path / "derived_tushare_ts_codes.txt").exists()


@pytest.mark.parametrize("field, value, message", [
    ("start_date", "2024-01-01", "YYYYMMDD"),
    ("end_date", "2024-01-31", "YYYYMMDD"),
    ("date_order", "bad", "start_date must be <= end_date"),
    ("dataset_version", "../v1_bad", "dataset_version"),
    ("dataset_version", "", "dataset_version"),
    ("expected_symbol_count", 0, "expected_symbol_count must be > 0"),
    ("universe_name", "", "universe_name is required"),
])
def test_tushare_dry_run_rejects_bad_runtime_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: object, message: str) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token-value")
    symbols = tmp_path / "symbols.csv"
    symbols.write_text("000008\n600000\n", encoding="utf-8")
    cfg = _cfg(tmp_path, symbols)
    kwargs = cfg.__dict__.copy()
    if field == "date_order":
        kwargs["start_date"] = "20240201"
        kwargs["end_date"] = "20240131"
    else:
        kwargs[field] = value
    with pytest.raises(ValueError, match=message):
        run_tushare_raw_ingest_dry_run(TushareRawIngestConfig(**kwargs))
