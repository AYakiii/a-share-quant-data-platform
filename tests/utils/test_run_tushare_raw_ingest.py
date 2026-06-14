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
    assert manifest["requested_api_names"] == []
    assert manifest["requested_families"] == ["market_flow"]
    assert manifest["api_names"] == ["moneyflow"]
    assert manifest["families"] == ["market_flow"]
    assert [s["api_name"] for s in manifest["sources"]] == ["moneyflow"]


def test_api_and_family_selection_intersect(tmp_path: Path) -> None:
    manifest = run_tushare_raw_ingest_dry_run(_cfg(tmp_path, _symbols(tmp_path), api_names=("daily", "moneyflow"), families=("market_flow",), dry_run=True), require_token=False)
    assert manifest["requested_api_names"] == ["daily", "moneyflow"]
    assert manifest["requested_families"] == ["market_flow"]
    assert manifest["api_names"] == ["moneyflow"]
    assert manifest["families"] == ["market_flow"]
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


def test_fields_contract_passes_fields_and_trims_output(tmp_path: Path) -> None:
    class FieldsClient:
        def __init__(self) -> None:
            self.params: dict[str, str] = {}

        def query(self, api_name: str, **params: str) -> pd.DataFrame:
            self.params = params
            return pd.DataFrame({
                "ts_code": ["000001.SZ"],
                "trade_date": [params["trade_date"]],
                "total_share": [1.0],
                "float_share": [1.0],
                "free_share": [1.0],
                "total_mv": [99.0],
                "pe": [10.0],
            })

    client = FieldsClient()
    run_tushare_raw_ingest(_cfg(tmp_path, _symbols(tmp_path), api_names=("daily_basic",)), client=client)
    assert client.params["fields"] == "ts_code,trade_date,total_share,float_share,free_share"
    part = tmp_path / "out" / "data" / "raw" / "tushare" / "market_basic" / "daily_basic" / "trade_date=20260612"
    df = pd.read_parquet(part / "data.parquet")
    assert list(df.columns) == ["ts_code", "trade_date", "total_share", "float_share", "free_share", "canonical_symbol"]


def test_resume_already_exists_backfills_qa_summaries(tmp_path: Path) -> None:
    symbols = _symbols(tmp_path)
    run_tushare_raw_ingest(_cfg(tmp_path, symbols), client=MockClient())
    run_tushare_raw_ingest(_cfg(tmp_path, symbols, resume=True), client=MockClient())
    artifacts = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition"
    assert "already_exists" in (artifacts / "raw_ingest_catalog.csv").read_text(encoding="utf-8")
    for name in ["source_coverage_summary.csv", "duplicate_key_summary.csv", "universe_filter_summary.csv", "field_presence_summary.csv"]:
        df = pd.read_csv(artifacts / name)
        assert not df.empty


def test_heartbeat_live_progress_and_events_flush(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfg = _cfg(tmp_path, _symbols(tmp_path), heartbeat_sec=0.001)
    run_tushare_raw_ingest(cfg, client=MockClient())
    out = capsys.readouterr().out
    assert "[heartbeat]" in out
    assert "TOKEN" not in out
    artifacts = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition"
    assert (artifacts / "live_progress.json").exists()
    events = [line for line in (artifacts / "operation_events.jsonl").read_text(encoding="utf-8").splitlines() if line]
    assert any('"event": "task_started"' in line for line in events)
    assert any('"event": "partition_written"' in line for line in events)
    for line in events:
        assert isinstance(__import__("json").loads(line), dict)


def test_cli_summary_default_and_print_manifest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from qsys.utils.run_tushare_raw_ingest import main

    symbols = _symbols(tmp_path)
    base = [
        "--dry-run", "--symbols-file", str(symbols), "--universe-name", "u", "--dataset-version", "v1",
        "--start-date", "20260612", "--end-date", "20260612", "--output-root", str(tmp_path / "out"), "--api-names", "daily",
    ]
    assert main(base) == 0
    short = capsys.readouterr().out
    assert "[tushare] output_root=" in short
    assert "api_names=daily" in short
    assert "planned_partitions=1" in short
    assert '"planned_partitions"' not in short
    assert main(base + ["--print-manifest"]) == 0
    full = capsys.readouterr().out
    assert '"planned_partitions"' in full


def test_operator_summary_artifacts_are_fixed_size_and_token_free(tmp_path: Path) -> None:
    class CleanClient:
        def query(self, api_name: str, **params: str) -> pd.DataFrame:
            return pd.DataFrame({
                "ts_code": ["000001.SZ", "000002.SZ"],
                "trade_date": [params["trade_date"]] * 2,
                "close": [1.0, 2.0],
            })

    symbols = _symbols(tmp_path)
    manifest = run_tushare_raw_ingest(
        _cfg(tmp_path, symbols, start_date="20260611", end_date="20260612", api_names=("daily", "daily_basic")),
        client=CleanClient(),
    )
    artifacts = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition"
    summary_path = artifacts / "operator_summary.json"
    by_api_path = artifacts / "operator_summary_by_api.csv"
    assert summary_path.exists()
    assert by_api_path.exists()
    text = summary_path.read_text(encoding="utf-8")
    assert "token" not in text.lower()
    summary = __import__("json").loads(text)
    assert summary["planned_partitions"] == 4
    assert summary["status_counts"]["ok"] == 4
    assert summary["abnormal_counts"] == {
        "bad_status_partitions": 0,
        "empty_partitions": 0,
        "failed_partitions": 0,
        "duplicate_partitions": 0,
        "missing_data_files": 0,
        "missing_metadata_files": 0,
    }
    assert summary["rough_check"] == "PASS"
    assert "planned_partitions" not in "\n".join(str(x) for x in summary.values())
    by_api = pd.read_csv(by_api_path)
    assert len(by_api) == len(manifest["api_names"]) == 2
    assert set(by_api["api_name"]) == {"daily", "daily_basic"}
    assert set([
        "status_ok", "status_empty", "status_request_failed", "total_return_rows", "total_filtered_rows",
        "min_filtered_rows", "max_filtered_rows", "min_symbols", "max_symbols", "data_files", "metadata_files",
        "missing_data_files", "missing_metadata_files", "rough_check",
    ]).issubset(by_api.columns)
    assert by_api["planned_partitions"].tolist() == [2, 2]


def test_operator_summary_abnormal_counts_are_aggregate_only(tmp_path: Path) -> None:
    from qsys.data.sources.tushare_acquisition import _write_operator_summaries

    class MixedClient:
        def query(self, api_name: str, **params: str) -> pd.DataFrame:
            if api_name == "daily_basic":
                raise RuntimeError("boom")
            if params["trade_date"] == "20260611":
                return pd.DataFrame(columns=["ts_code", "trade_date", "close"])
            return MockClient().query(api_name, **params)

    cfg = _cfg(tmp_path, _symbols(tmp_path), start_date="20260611", end_date="20260612", api_names=("daily", "daily_basic"), retry=0)
    manifest = run_tushare_raw_ingest(cfg, client=MixedClient())
    artifacts = tmp_path / "out" / "artifacts" / "tushare_raw_acquisition"
    catalog = pd.read_csv(artifacts / "raw_ingest_catalog.csv")
    ok_row = catalog[catalog["status"] == "ok"].iloc[0]
    Path(ok_row["data_path"]).unlink()
    Path(ok_row["metadata_path"]).unlink()
    summary = _write_operator_summaries(cfg, manifest, artifacts)
    assert summary["abnormal_counts"]["failed_partitions"] == 2
    assert summary["abnormal_counts"]["empty_partitions"] == 1
    assert summary["abnormal_counts"]["duplicate_partitions"] == 1
    assert summary["abnormal_counts"]["missing_data_files"] == 1
    assert summary["abnormal_counts"]["missing_metadata_files"] == 1
    assert summary["rough_check"] == "FAIL"
    assert "partition_path" not in __import__("json").dumps(summary)
    by_api = pd.read_csv(artifacts / "operator_summary_by_api.csv")
    assert len(by_api) == 2
    assert by_api["status_request_failed"].sum() == 2


def test_cli_default_uses_operator_summary_without_manifest_or_paths(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from qsys.utils.run_tushare_raw_ingest import main

    symbols = _symbols(tmp_path)
    base = [
        "--dry-run", "--symbols-file", str(symbols), "--universe-name", "u", "--dataset-version", "v1",
        "--start-date", "20260612", "--end-date", "20260612", "--output-root", str(tmp_path / "out2"), "--api-names", "daily",
    ]
    assert main(base) == 0
    short = capsys.readouterr().out
    assert "rough_check" in short
    assert "status_counts" in short
    assert "abnormal_counts" in short
    assert '"planned_partitions"' not in short
    assert "trade_date=20260612" not in short
    assert "tushare_acquisition_manifest" not in short
    assert main(base + ["--print-manifest"]) == 0
    full = capsys.readouterr().out
    assert '"planned_partitions"' in full


def test_cli_manual_default_review_cell_is_summary_only() -> None:
    text = Path("docs/operator/cli_manual.md").read_text(encoding="utf-8")
    assert "operator_summary.json" in text
    assert "operator_summary_by_api.csv" in text
    assert "live_progress.json" in text
    assert "clear_output(wait=True)" in text
    assert "Tushare local acquisition progress" in text
    assert "**5) Compact summary review cell**" in text
    assert 'manifest["planned_partitions"]' not in text
    assert "pd.read_parquet" not in text
