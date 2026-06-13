import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from qsys.utils import raw_lake_compact_cli as cli


def _raw(root: Path, parts=None, rows=2, cols=("a", "b"), family="fam", api="api", provider="akshare") -> None:
    p = root / "data" / "raw" / provider / family / api
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


def _rewrite_ready_from_current_plan(pkg: Path, drive: Path) -> dict:
    manifest = _manifest(pkg)
    collisions = cli.write_collision_plan(pkg, drive)
    ready = cli._ready_payload(manifest, collisions, drive_root=drive.resolve(), package_root=pkg.resolve(), collision_plan_path=(pkg / "drive_collision_plan.csv").resolve())
    (pkg / "READY_FOR_PROMOTION.json").write_text(json.dumps(ready), encoding="utf-8")
    return ready


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

    # A skip-identical promotion remains supported when the operator-reviewed
    # prepare plan already observed the byte-identical Drive asset.
    shutil.rmtree(drive / "catalog")
    _rewrite_ready_from_current_plan(pkg, drive)
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
    with pytest.raises(ValueError, match="collision plan differs"):
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


def test_prepare_rejects_empty_staging_without_ready_or_drive_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    (out / "data" / "raw" / "akshare").mkdir(parents=True)
    drive = tmp_path / "drive"
    drive.mkdir()
    with pytest.raises(FileNotFoundError, match="No landed Raw parquet assets found under"):
        cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "empty"])
    assert not (Path("outputs/raw_acquisition_compact/empty") / "READY_FOR_PROMOTION.json").exists()
    assert not (drive / "raw" / "akshare").exists()


def test_prepare_rejects_unknown_window_without_explicit_dates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "plain_output_root"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts={"symbol": "000001"})
    with pytest.raises(ValueError, match="acquisition window"):
        cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "unknown_window"])
    assert not (Path("outputs/raw_acquisition_compact/unknown_window") / "compact_manifest.json").exists()


def test_prepare_accepts_explicit_dates_when_output_root_has_no_embedded_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "plain_output_root"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts={"symbol": "000001"})
    assert cli.main([
        "prepare",
        "--output-root",
        str(out),
        "--drive-dwh-root",
        str(drive),
        "--promotion-name",
        "explicit_window",
        "--start-date",
        "20220101",
        "--end-date",
        "20241231",
    ]) == 0
    manifest = _manifest(Path("outputs/raw_acquisition_compact/explicit_window"))
    assert manifest["acquisition_window"] == {"start_date": "20220101", "end_date": "20241231"}
    assert manifest["compact_assets"][0]["bucket_kind"] == "scope"
    assert manifest["compact_assets"][0]["bucket_value"] == "run_20220101_20241231"


def test_prepare_rejects_start_date_after_end_date(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "plain_output_root"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts={"symbol": "000001"})
    with pytest.raises(ValueError, match="start_date must be <= end_date"):
        cli.main([
            "prepare",
            "--output-root",
            str(out),
            "--drive-dwh-root",
            str(drive),
            "--promotion-name",
            "bad_window",
            "--start-date",
            "20241231",
            "--end-date",
            "20220101",
        ])
    assert not (Path("outputs/raw_acquisition_compact/bad_window") / "compact_manifest.json").exists()


def test_prepare_binds_custom_drive_roots_and_semantic_collision_plan(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "custom_drive_root"
    drive.mkdir()
    _raw(out)
    assert cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "custom_drive"]) == 0
    pkg = Path("outputs/raw_acquisition_compact/custom_drive")
    ready = _ready(pkg)
    assert ready["prepared_drive_dwh_root"] == str(drive.resolve())
    assert ready["prepared_drive_raw_root"] == str((drive / "raw" / "akshare").resolve())
    assert ready["prepared_drive_catalog_root"] == str((drive / "catalog" / "promotions" / "custom_drive").resolve())
    assert ready["drive_collision_plan_path"] == str((pkg / "drive_collision_plan.csv").resolve())
    assert ready["drive_collision_plan_sha256"] == cli.file_sha256(pkg / "drive_collision_plan.csv")
    assert ready["planned_asset_count"] == 1
    assert ready["planned_copy_new_count"] == 1
    assert ready["planned_skip_identical_count"] == 0
    assert ready["planned_block_non_identical_count"] == 0

    plan = pd.read_csv(pkg / "drive_collision_plan.csv")
    for col in ["source_family", "api_name", "bucket_kind", "bucket_value", "rows", "relative_path", "drive_path"]:
        assert col in plan.columns
    assert plan.loc[0, "drive_path"] == str(drive.resolve() / "raw" / "akshare" / "fam" / "api" / "year=2022" / "data.parquet")
    assert not (drive / "raw" / "akshare").exists()


def test_promote_refuses_drive_root_changed_after_prepare_before_raw_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive_a, pkg = _prepare(tmp_path, promotion="bound_root")
    drive_b = tmp_path / "drive_b"
    drive_b.mkdir()
    with pytest.raises(ValueError, match="Drive DWH root differs"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive_b), "--confirm-promotion", "bound_root"])
    assert not (drive_a / "raw" / "akshare").exists()
    assert not (drive_b / "raw" / "akshare").exists()


def test_promote_refuses_mutated_reviewed_collision_plan_before_raw_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path, promotion="mutated_plan")
    plan_path = pkg / "drive_collision_plan.csv"
    plan_path.write_text(plan_path.read_text(encoding="utf-8") + "\n# mutated", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "mutated_plan"])
    assert not (drive / "raw" / "akshare").exists()


def test_promote_refuses_rebuilt_target_set_change_before_raw_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _rc, _out, drive, pkg = _prepare(tmp_path, promotion="target_set")
    original = cli.build_collision_plan

    def changed_plan(package_root, drive_dwh_root):
        rows = original(package_root, drive_dwh_root)
        rows[0] = dict(rows[0])
        rows[0]["drive_path"] = str((drive / "raw" / "akshare" / "different" / "data.parquet").resolve())
        return rows

    monkeypatch.setattr(cli, "build_collision_plan", changed_plan)
    with pytest.raises(ValueError, match="target path set differs"):
        cli.main(["promote", "--package-root", str(pkg), "--drive-dwh-root", str(drive), "--confirm-promotion", "target_set"])
    assert not (drive / "raw" / "akshare").exists()


def test_akshare_legacy_default_compact_path_has_no_schema_version(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts={"year": "2022"})
    assert cli.main(["prepare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "ak_legacy"]) == 0
    manifest = _manifest(Path("outputs/raw_acquisition_compact/ak_legacy"))
    rel = manifest["compact_assets"][0]["relative_path"]
    assert rel == "raw/akshare/fam/api/year=2022/data.parquet"
    assert manifest["storage_schema_version"] == ""


def test_tushare_prepare_requires_and_uses_v1_schema_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, parts={"year": "2022"}, provider="tushare")
    with pytest.raises(ValueError, match="storage-schema-version"):
        cli.main(["prepare", "--provider", "tushare", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "ts_missing_schema"])
    assert cli.main(["prepare", "--provider", "tushare", "--storage-schema-version", "v1", "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "ts_v1"]) == 0
    manifest = _manifest(Path("outputs/raw_acquisition_compact/ts_v1"))
    assert manifest["provider"] == "tushare"
    assert manifest["storage_schema_version"] == "v1"
    assert manifest["compact_assets"][0]["relative_path"] == "raw/tushare/fam/api/v1/year=2022/data.parquet"
    ready = _ready(Path("outputs/raw_acquisition_compact/ts_v1"))
    assert ready["provider"] == "tushare"
    assert ready["storage_schema_version"] == "v1"
    assert ready["prepared_drive_raw_root"] == str((drive / "raw" / "tushare").resolve())


@pytest.mark.parametrize("bad", ["", ".", "..", "../x", "x/y", r"x\y", "/abs", "a b"])
def test_provider_path_segment_traversal_is_rejected(tmp_path, monkeypatch, bad):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out)
    with pytest.raises(ValueError, match="provider"):
        cli.main(["prepare", "--provider", bad, "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "bad_provider"])


@pytest.mark.parametrize("bad", [".", "..", "../x", "x/y", r"x\y", "/abs", "a b"])
def test_storage_schema_path_segment_traversal_is_rejected(tmp_path, monkeypatch, bad):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "wave_20220101_20241231"
    drive = tmp_path / "drive"
    drive.mkdir()
    _raw(out, provider="tushare")
    with pytest.raises(ValueError, match="storage_schema_version"):
        cli.main(["prepare", "--provider", "tushare", "--storage-schema-version", bad, "--output-root", str(out), "--drive-dwh-root", str(drive), "--promotion-name", "bad_schema"])
