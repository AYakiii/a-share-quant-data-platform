from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.sources.tushare_acquisition import run_tushare_raw_ingest, run_tushare_raw_ingest_dry_run
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig
from qsys.utils.run_tushare_raw_ingest import build_parser, config_from_args


class MockClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def query(self, api_name: str, **params: str) -> pd.DataFrame:
        self.calls.append((api_name, params["trade_date"]))
        return pd.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ", "000002.SZ", "600000.SH"],
            "trade_date": [params["trade_date"]] * 4,
            "close": [1.0, 2.0, 2.0, 3.0],
        })


def _symbols(tmp_path: Path) -> Path:
    path = tmp_path / "symbols.txt"
    path.write_text("000001\n000002\n", encoding="utf-8")
    return path


def _cfg(tmp_path: Path, symbols: Path, **kwargs: object) -> TushareRawIngestConfig:
    data = dict(
        symbols_file=symbols,
        universe_name="stock_universe_vx",
        dataset_version="v1_csi500_2021_2025_union",
        start_date="20260612",
        end_date="20260612",
        output_root=tmp_path / "out",
        api_names=("daily",),
        request_sleep=0.0,
        dry_run=False,
    )
    data.update(kwargs)
    return TushareRawIngestConfig(**data)


def test_cli_parse_api_names_and_families(tmp_path: Path) -> None:
    args = build_parser().parse_args([
        "--symbols-file", str(tmp_path / "symbols.txt"), "--universe-name", "u", "--dataset-version", "v1",
        "--start-date", "20260612", "--end-date", "20260612", "--output-root", str(tmp_path / "out"),
        "--api-names", "daily,daily_basic", "--families", "market_price", "--dry-run", "--resume",
    ])
    cfg = config_from_args(args)
    assert cfg.api_names == ("daily", "daily_basic")
    assert cfg.families == ("market_price",)
    assert cfg.dry_run is True
    assert cfg.resume is True


def test_missing_dataset_version_fails(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--symbols-file", "s", "--universe-name", "u", "--start-date", "20260612", "--end-date", "20260612", "--output-root", str(tmp_path)])


def test_illegal_dataset_version_fails(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="dataset_version"):
        run_tushare_raw_ingest_dry_run(_cfg(tmp_path, _symbols(tmp_path), dataset_version="../bad", dry_run=True), require_token=False)


def test_symbols_file_sha_recorded_and_dry_run_does_not_call_api(tmp_path: Path) -> None:
    symbols = _symbols(tmp_path)
    client = MockClient()
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, symbols, dry_run=True), require_token=False)
    assert manifest["universe_sha256"] == hashlib.sha256(symbols.read_bytes()).hexdigest()
    assert client.calls == []
    assert "daily" in manifest["api_names"]


def test_mock_api_writes_parquet_metadata_filters_and_detects_duplicates(tmp_path: Path) -> None:
    symbols = _symbols(tmp_path)
    client = MockClient()
    cfg = _cfg(tmp_path, symbols)
    run_tushare_raw_ingest(cfg, client=client)
    part = tmp_path / "out" / "data" / "raw" / "tushare" / "market_price" / "daily" / "trade_date=20260612"
    assert (part / "data.parquet").exists()
    assert (part / "metadata.json").exists()
    df = pd.read_parquet(part / "data.parquet")
    assert set(df["canonical_symbol"]) == {"000001", "000002"}
    assert len(df) == 3
    meta = (part / "metadata.json").read_text(encoding="utf-8")
    assert '"duplicate_key_count": 1' in meta
    assert '"post_filter_symbol_count": 2' in meta
    artifacts = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition"
    for name in ["tushare_acquisition_manifest.json", "raw_ingest_catalog.csv", "source_coverage_summary.csv", "field_presence_summary.csv", "duplicate_key_summary.csv", "universe_filter_summary.csv", "operation_events.jsonl"]:
        assert (artifacts / name).exists()


def test_family_subset_selects_registry_sources(tmp_path: Path) -> None:
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, _symbols(tmp_path), api_names=(), families=("market_flow",), dry_run=True), require_token=False)
    assert manifest["api_names"] == ["daily", "daily_basic", "moneyflow", "margin_detail"]
    assert [s["api_name"] for s in manifest["sources"]] == ["moneyflow"]


def test_resume_skips_complete_partition(tmp_path: Path) -> None:
    symbols = _symbols(tmp_path)
    cfg = _cfg(tmp_path, symbols)
    run_tushare_raw_ingest(cfg, client=MockClient())
    client = MockClient()
    run_tushare_raw_ingest(_cfg(tmp_path, symbols, resume=True), client=client)
    assert client.calls == []
    catalog = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition" / "raw_ingest_catalog.csv"
    assert "already_exists" in catalog.read_text(encoding="utf-8")


def test_output_root_must_not_be_drive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Google Drive"):
        run_tushare_raw_ingest_dry_run(_cfg(tmp_path, _symbols(tmp_path), output_root=Path("/content/gdrive/MyDrive/out"), dry_run=True), require_token=False)
