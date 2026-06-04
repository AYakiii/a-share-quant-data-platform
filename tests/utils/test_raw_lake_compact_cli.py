import json
from pathlib import Path

import pandas as pd
import pytest

from qsys.utils import raw_lake_compact_cli as cli


def _raw(root: Path, parts=None, rows=2, cols=("a", "b"), family="fam", api="api") -> None:
    p = root / "data" / "raw" / "akshare" / family / api
    for k, v in (parts or {"trade_date": "20220103"}).items():
        p = p / f"{k}={v}"
    p.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({c: list(range(rows)) for c in cols}).to_parquet(p / "data.parquet", index=False)


def _prepare(tmp_path: Path, *, parts=None, promotion="promo"):
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts=parts)
    rc = cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", promotion])
    pkg = Path("outputs/raw_acquisition_compact") / promotion
    return rc, out, drive, pkg


def test_prepare_writes_ready_and_never_writes_drive_raw_parquet(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rc, _out, drive, pkg = _prepare(tmp_path)
    assert rc == 0
    ready = json.loads((pkg / "READY_FOR_PROMOTION.json").read_text())
    assert ready["ready_for_promotion"] is True
    assert not (drive / "data" / "raw" / "akshare").exists()
    assert (pkg / "drive_collision_plan.csv").exists()


def test_collision_dry_run_blocks_non_identical_drive_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, rows=2)
    # First compact to learn the relative path, then put different bytes on Drive.
    cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "promo"])
    pkg = Path("outputs/raw_acquisition_compact/promo")
    rel = json.loads((pkg / "compact_manifest.json").read_text())["compact_assets"][0]["relative_path"]
    dst = drive / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": [99], "b": [99]}).to_parquet(dst, index=False)
    cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "promo2"])
    ready = json.loads((Path("outputs/raw_acquisition_compact/promo2") / "READY_FOR_PROMOTION.json").read_text())
    assert ready["ready_for_promotion"] is False
    assert ready["blocked_collisions"][0]["action"] == "block_non_identical"


def test_promote_refuses_missing_and_incorrect_confirmation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    with pytest.raises(ValueError, match="confirm-promotion is required"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive)])
    with pytest.raises(ValueError, match="exactly match"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "wrong"])


def test_promote_permits_copy_new_skip_identical_and_audit_read_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"]) == 0
    report = json.loads((pkg / "promotion_report.json").read_text())
    assert report["copied"]
    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"]) == 0
    report = json.loads((pkg / "promotion_report.json").read_text())
    assert report["skipped_identical"]
    before = sorted(str(p.relative_to(drive)) for p in drive.rglob("*"))
    assert cli.main(["audit", "--promotion-name", "promo", "--drive-dwh-root", str(drive)]) == 0
    after = sorted(str(p.relative_to(drive)) for p in drive.rglob("*"))
    assert before == after


def test_promote_refuses_non_identical_overwrite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    rel = json.loads((pkg / "compact_manifest.json").read_text())["compact_assets"][0]["relative_path"]
    dst = drive / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": [9], "b": [9]}).to_parquet(dst, index=False)
    with pytest.raises(FileExistsError, match="non-identical"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])


def test_scope_and_snapshot_promotion_require_reviewed_opt_in(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path, parts={"snapshot": "latest"})
    with pytest.raises(ValueError, match="explicit review"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])
    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo", "--allow-reviewed-bucket-kinds", "snapshot"]) == 0


def test_promote_reopens_drive_parquet_and_verifies_rows_columns_sha256(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"]) == 0
    manifest = json.loads((pkg / "compact_manifest.json").read_text())
    asset = manifest["compact_assets"][0]
    df = pd.read_parquet(drive / asset["relative_path"])
    assert len(df) == asset["rows"]
    assert list(df.columns) == asset["columns"]
