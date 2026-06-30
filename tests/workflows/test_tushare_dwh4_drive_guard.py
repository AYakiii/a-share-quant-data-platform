from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pytest

from qsys.workflows.tushare_dwh4_drive_guard import (
    DELETE_REQUEST_STATUS,
    DriveDeleteBlocked,
    build_drive_delete_request,
    guarded_rmtree,
    guarded_unlink,
    is_under_drive_dwh_root,
    write_drive_delete_request_artifacts,
)


def test_guarded_unlink_blocks_drive_file_without_calling_unlink(tmp_path: Path, monkeypatch) -> None:
    drive_root = tmp_path / "drive" / "a_share_quant_data"
    target = drive_root / "raw" / "tushare" / "daily_basic" / "trade_date=20260601" / "data.parquet"
    target.parent.mkdir(parents=True)
    target.write_text("keep", encoding="utf-8")

    def fail_unlink(self, *args, **kwargs):
        raise AssertionError("Path.unlink must not be called for Drive paths")

    monkeypatch.setattr(Path, "unlink", fail_unlink)
    with pytest.raises(DriveDeleteBlocked):
        guarded_unlink(target, drive_dwh_root=drive_root)

    assert target.exists()
    assert target.read_text(encoding="utf-8") == "keep"


def test_guarded_rmtree_blocks_drive_directory_without_calling_rmtree(tmp_path: Path, monkeypatch) -> None:
    drive_root = tmp_path / "drive" / "a_share_quant_data"
    target_dir = drive_root / "raw" / "tushare" / "daily_basic"
    nested = target_dir / "trade_date=20260601" / "data.parquet"
    nested.parent.mkdir(parents=True)
    nested.write_text("keep", encoding="utf-8")

    def fail_rmtree(path, *args, **kwargs):
        raise AssertionError("shutil.rmtree must not be called for Drive paths")

    monkeypatch.setattr(shutil, "rmtree", fail_rmtree)
    with pytest.raises(DriveDeleteBlocked):
        guarded_rmtree(target_dir, drive_dwh_root=drive_root)

    assert nested.exists()
    assert nested.read_text(encoding="utf-8") == "keep"


def test_drive_delete_request_requires_target_under_drive_root(tmp_path: Path) -> None:
    drive_root = tmp_path / "drive" / "a_share_quant_data"
    target = drive_root / "raw" / "tushare" / "old.parquet"
    outside = tmp_path / "outside.parquet"

    request = build_drive_delete_request(
        drive_root,
        target,
        operation="Path.unlink",
        reason="superseded by verified replacement",
    )
    assert request.relative_path == "raw/tushare/old.parquet"
    assert request.status == DELETE_REQUEST_STATUS
    assert request.delete_executed is False
    assert is_under_drive_dwh_root(drive_root, target) is True
    assert is_under_drive_dwh_root(drive_root, outside) is False

    with pytest.raises(ValueError):
        build_drive_delete_request(drive_root, outside, operation="Path.unlink")


def test_write_drive_delete_request_artifacts_are_explicit_and_non_mutating(tmp_path: Path) -> None:
    drive_root = tmp_path / "drive" / "a_share_quant_data"
    target = drive_root / "raw" / "tushare" / "daily_basic" / "year=2026" / "data.parquet"
    target.parent.mkdir(parents=True)
    target.write_text("keep", encoding="utf-8")
    request = build_drive_delete_request(
        drive_root,
        target,
        operation="Path.unlink",
        reason="local candidate superseded this bucket",
    )

    written = write_drive_delete_request_artifacts(drive_root, [request], tmp_path / "artifacts")
    assert set(written) == {"drive_delete_request", "drive_delete_plan", "drive_delete_summary"}
    markdown = written["drive_delete_request"].read_text(encoding="utf-8")
    assert "DRIVE DELETE REQUEST - NOT EXECUTED" in markdown
    assert "No files were deleted." in markdown
    assert "raw/tushare/daily_basic/year=2026/data.parquet" in markdown

    with written["drive_delete_plan"].open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "drive_dwh_root": str(drive_root.resolve()),
            "relative_path": "raw/tushare/daily_basic/year=2026/data.parquet",
            "path": str(target.resolve()),
            "operation": "Path.unlink",
            "reason": "local candidate superseded this bucket",
            "status": DELETE_REQUEST_STATUS,
            "delete_executed": "false",
        }
    ]

    summary = json.loads(written["drive_delete_summary"].read_text(encoding="utf-8"))
    assert summary["delete_request_generated"] is True
    assert summary["drive_delete_executed"] is False
    assert summary["drive_write_executed"] is False
    assert summary["request_count"] == 1
    assert target.exists()
