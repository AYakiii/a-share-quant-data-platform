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


def _manifest(pkg: Path) -> dict:
    return json.loads((pkg / "compact_manifest.json").read_text())


def _ready(pkg: Path) -> dict:
    return json.loads((pkg / "READY_FOR_PROMOTION.json").read_text())


def test_prepare_writes_ready_and_never_writes_drive_raw_parquet(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rc, _out, drive, pkg = _prepare(tmp_path)
    assert rc == 0
    ready = _ready(pkg)
    assert ready["ready_for_promotion"] is True
    assert not (drive / "data" / "raw" / "akshare").exists()
    assert not (drive / "raw" / "akshare").exists()
    assert (pkg / "drive_collision_plan.csv").exists()


def test_collision_dry_run_blocks_non_identical_drive_files_at_canonical_raw_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, rows=2)
    cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "promo"])
    pkg = Path("outputs/raw_acquisition_compact/promo")
    rel = _manifest(pkg)["compact_assets"][0]["relative_path"]
    assert rel.startswith("raw/akshare/")
    dst = drive / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": [99], "b": [99]}).to_parquet(dst, index=False)
    cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "promo2"])
    ready = _ready(Path("outputs/raw_acquisition_compact/promo2"))
    assert ready["ready_for_promotion"] is False
    assert ready["blocked_collisions"][0]["action"] == "block_non_identical"


def test_promote_refuses_missing_and_incorrect_confirmation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    with pytest.raises(ValueError, match="confirm-promotion is required"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive)])
    with pytest.raises(ValueError, match="exactly match"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "wrong"])


def test_promote_permits_copy_new_skip_identical_uses_canonical_drive_raw_and_audit_read_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"]) == 0
    reports = sorted(pkg.glob("promotion_attempt_*.json"))
    assert reports
    report = json.loads(reports[-1].read_text())
    assert report["copied"]
    manifest = _manifest(pkg)
    asset = manifest["compact_assets"][0]
    assert (drive / asset["relative_path"]).exists()
    assert asset["relative_path"].startswith("raw/akshare/")
    assert not (drive / "data" / "raw" / "akshare").exists()

    assert cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"]) == 0
    reports = sorted(pkg.glob("promotion_attempt_*.json"))
    report = json.loads(reports[-1].read_text())
    assert report["skipped_identical"]
    before = sorted(str(p.relative_to(drive)) for p in drive.rglob("*"))
    assert cli.main(["audit", "--promotion-name", "promo", "--drive-dwh-root", str(drive)]) == 0
    after = sorted(str(p.relative_to(drive)) for p in drive.rglob("*"))
    assert before == after


def test_promote_refuses_non_identical_overwrite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    rel = _manifest(pkg)["compact_assets"][0]["relative_path"]
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
    asset = _manifest(pkg)["compact_assets"][0]
    df = pd.read_parquet(drive / asset["relative_path"])
    assert len(df) == asset["rows"]
    assert list(df.columns) == asset["columns"]


def test_unsafe_promotion_names_are_rejected(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out)
    for name in ["", "../bad", "bad/name", "bad\\name", "/abs", "..", "a..b"]:
        with pytest.raises(ValueError, match="promotion_name"):
            cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", name])


def test_relative_path_traversal_in_manifest_is_rejected(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    manifest = _manifest(pkg)
    manifest["compact_assets"][0]["relative_path"] = "raw/akshare/../evil.parquet"
    (pkg / "compact_manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="unsafe|under"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])
    assert not (drive / "raw" / "akshare").exists()


def test_mutated_local_compact_fails_before_any_drive_raw_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    rel = _manifest(pkg)["compact_assets"][0]["relative_path"]
    pd.DataFrame({"a": [42], "b": [42]}).to_parquet(pkg / rel, index=False)
    with pytest.raises(ValueError, match="row count mismatch|sha256 mismatch"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])
    assert not (drive / "raw" / "akshare").exists()


def test_stale_ready_cannot_bypass_snapshot_review_opt_in(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path, parts={"snapshot": "latest"})
    ready = _ready(pkg)
    ready["review_required_bucket_kinds"] = []
    (pkg / "READY_FOR_PROMOTION.json").write_text(json.dumps(ready))
    with pytest.raises(ValueError, match="explicit review"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])
    assert not (drive / "raw" / "akshare").exists()


def test_drive_catalog_artifact_overwrite_is_immutable(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path)
    catalog = drive / "catalog" / "promotions" / "promo"
    catalog.mkdir(parents=True)
    (catalog / "compact_manifest.json").write_text('{"different": true}', encoding="utf-8")
    with pytest.raises(FileExistsError, match="catalog artifact"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "promo"])
    assert not (drive / "raw" / "akshare").exists()
