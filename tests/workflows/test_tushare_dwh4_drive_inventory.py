from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qsys.workflows import tushare_dwh4_drive_inventory as inventory_module
from qsys.workflows.tushare_dwh4_drive_inventory import (
    build_drive_inventory,
    parse_drive_asset_path,
    write_drive_inventory_artifacts,
)
from qsys.workflows.tushare_dwh4_task import Dwh4DriveInventoryPolicy


DATASET_VERSION = "v1_csi500_2021_2025_union"
MOCK_PARQUET_FRAMES: dict[Path, pd.DataFrame] = {}


def _policy(*, fail_on_unreadable: bool = True) -> Dwh4DriveInventoryPolicy:
    return Dwh4DriveInventoryPolicy(
        enabled=True,
        scan_raw_tushare=True,
        read_parquet_metadata=True,
        compute_sha256=True,
        fail_on_unreadable_existing_asset=fail_on_unreadable,
    )


def _asset_path(drive: Path, family: str, api: str, dataset_version: str, bucket: str) -> Path:
    return drive / "raw" / "tushare" / family / api / dataset_version / bucket / "data.parquet"


def _write_parquet(path: Path, frame: pd.DataFrame, *, metadata: dict[str, object] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(("mock parquet " + path.as_posix()).encode("utf-8"))
    MOCK_PARQUET_FRAMES[path.resolve()] = frame
    if metadata is not None:
        path.with_name("metadata.json").write_text(json.dumps(metadata), encoding="utf-8")


def _patch_read_parquet(monkeypatch) -> None:
    def fake_read_parquet(path):
        return MOCK_PARQUET_FRAMES[Path(path).resolve()]

    monkeypatch.setattr(inventory_module.pd, "read_parquet", fake_read_parquet)


def _drive_snapshot(root: Path) -> list[tuple[str, int]]:
    return sorted((str(path.relative_to(root)), path.stat().st_size) for path in root.rglob("*") if path.is_file())


def test_drive_inventory_scans_mock_tree_extracts_rows_columns_dates_and_sha(tmp_path: Path, monkeypatch) -> None:
    _patch_read_parquet(monkeypatch)
    drive = tmp_path / "drive"
    daily = _asset_path(drive, "market_basic", "daily_basic", DATASET_VERSION, "year=2026")
    trade_cal = _asset_path(drive, "market_calendar", "trade_cal", DATASET_VERSION, "window=latest")
    _write_parquet(
        daily,
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "trade_date": ["20260612", "20260615"],
                "turnover_rate": [1.2, 2.3],
            }
        ),
        metadata={"duplicate_key_count": 0, "post_filter_symbol_count": 2},
    )
    _write_parquet(
        trade_cal,
        pd.DataFrame({"cal_date": ["20220101", "20260628"], "is_open": [0, 1]}),
    )
    active_manifest_path = f"catalog/active/tushare/{DATASET_VERSION}/dwh4_tushare_active_manifest.json"
    active_manifest = drive / active_manifest_path
    active_manifest.parent.mkdir(parents=True, exist_ok=True)
    active_manifest.write_text(
        json.dumps({"provider": "tushare", "active_assets": [{"relative_path": daily.relative_to(drive).as_posix()}]}),
        encoding="utf-8",
    )

    before = _drive_snapshot(drive)
    result = build_drive_inventory(
        drive,
        dataset_version=DATASET_VERSION,
        policy=_policy(),
        active_manifest_path=active_manifest_path,
    )
    written = write_drive_inventory_artifacts(result, tmp_path / "artifacts")
    after = _drive_snapshot(drive)

    assert before == after
    assert len(result.assets) == 2
    daily_asset = next(asset for asset in result.assets if asset.api_name == "daily_basic")
    assert daily_asset.bucket_kind == "year"
    assert daily_asset.bucket_value == "2026"
    assert daily_asset.rows == 2
    assert daily_asset.columns == ("ts_code", "trade_date", "turnover_rate")
    assert daily_asset.date_column == "trade_date"
    assert daily_asset.min_date == "20260612"
    assert daily_asset.max_date == "20260615"
    assert daily_asset.metadata_exists is True
    assert daily_asset.metadata_keys == ("duplicate_key_count", "post_filter_symbol_count")
    assert len(daily_asset.sha256) == 64
    assert result.summary["asset_count"] == 2
    assert result.summary["api_counts"] == {"daily_basic": 1, "trade_cal": 1}
    assert result.summary["bucket_kind_counts"] == {"window": 1, "year": 1}
    assert result.summary["active_manifest_exists"] is True
    assert result.summary["active_manifest_asset_count"] == 1
    assert result.summary["drive_write_executed"] is False
    assert result.summary["drive_delete_executed"] is False
    assert written["drive_inventory"].exists()
    assert written["drive_inventory_summary"].exists()
    assert "daily_basic" in written["drive_inventory"].read_text(encoding="utf-8-sig")


def test_drive_inventory_filters_dataset_version_and_parses_snapshot(tmp_path: Path, monkeypatch) -> None:
    _patch_read_parquet(monkeypatch)
    drive = tmp_path / "drive"
    wanted = _asset_path(drive, "security_master", "stock_basic", DATASET_VERSION, "snapshot=latest")
    ignored = _asset_path(drive, "security_master", "stock_basic", "v2", "snapshot=latest")
    _write_parquet(wanted, pd.DataFrame({"ts_code": ["000001.SZ"], "snapshot_date": ["20260628"]}))
    _write_parquet(ignored, pd.DataFrame({"ts_code": ["000002.SZ"], "snapshot_date": ["20260628"]}))

    result = build_drive_inventory(drive, dataset_version=DATASET_VERSION, policy=_policy())

    assert [asset.dataset_version for asset in result.assets] == [DATASET_VERSION]
    assert result.assets[0].bucket_kind == "snapshot"
    assert result.assets[0].bucket_value == "latest"
    assert result.assets[0].snapshot_date == ""
    assert result.assets[0].min_date == "20260628"
    assert result.assets[0].max_date == "20260628"
    assert result.summary["ignored_dataset_version_count"] == 1


def test_drive_inventory_records_unreadable_asset_when_policy_allows(tmp_path: Path) -> None:
    drive = tmp_path / "drive"
    bad = _asset_path(drive, "market_basic", "daily_basic", DATASET_VERSION, "year=2026")
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("not parquet", encoding="utf-8")

    result = build_drive_inventory(drive, dataset_version=DATASET_VERSION, policy=_policy(fail_on_unreadable=False))

    assert len(result.assets) == 1
    assert result.assets[0].status == "unreadable"
    assert result.assets[0].rows is None
    assert result.assets[0].columns == ()
    assert len(result.assets[0].sha256) == 64
    assert result.summary["unreadable_asset_count"] == 1


def test_drive_inventory_raises_on_unreadable_asset_when_policy_requires(tmp_path: Path) -> None:
    drive = tmp_path / "drive"
    bad = _asset_path(drive, "market_basic", "daily_basic", DATASET_VERSION, "year=2026")
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("not parquet", encoding="utf-8")

    try:
        build_drive_inventory(drive, dataset_version=DATASET_VERSION, policy=_policy(fail_on_unreadable=True))
    except ValueError as exc:
        assert "unreadable Drive asset" in str(exc)
    else:
        raise AssertionError("expected unreadable parquet to fail")


def test_parse_drive_asset_path_rejects_non_canonical_paths(tmp_path: Path) -> None:
    drive = tmp_path / "drive"
    path = drive / "raw" / "akshare" / "fam" / "api" / DATASET_VERSION / "year=2026" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")

    try:
        parse_drive_asset_path(drive, path)
    except ValueError as exc:
        assert "not a canonical Drive raw/tushare asset" in str(exc)
    else:
        raise AssertionError("expected non-tushare path to fail")
