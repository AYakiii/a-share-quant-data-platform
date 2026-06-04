from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.raw_compact import classify_bucket, compact_raw_lake, scan_raw_assets


def _raw(root: Path, family="fam", api="api", parts=None, rows=2, cols=("a", "b")) -> Path:
    parts = parts or {}
    p = root / "data" / "raw" / "akshare" / family / api
    for k, v in parts.items():
        p = p / f"{k}={v}"
    p.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({c: list(range(rows)) for c in cols})
    df.to_parquet(p / "data.parquet", index=False)
    return p / "data.parquet"


def test_classifies_year_bucket():
    assert classify_bucket({"year": "2024"}, start_date="20220101", end_date="20241231") == ("year", "2024")


def test_classifies_trade_date_report_date_same_year_range_cross_year_snapshot_since_and_scope():
    assert classify_bucket({"trade_date": "20230105"}, start_date="20220101", end_date="20241231") == ("year", "2023")
    assert classify_bucket({"report_date": "20221231"}, start_date="20220101", end_date="20241231") == ("year", "2022")
    assert classify_bucket({"start_date": "20240101", "end_date": "20241231"}, start_date="20220101", end_date="20241231") == ("year", "2024")
    assert classify_bucket({"start_date": "20231201", "end_date": "20240131"}, start_date="20220101", end_date="20241231") == ("window", "20231201_20240131")
    assert classify_bucket({"snapshot": "latest"}, start_date="20220101", end_date="20241231") == ("snapshot", "latest")
    assert classify_bucket({"since_date": "20220101"}, start_date="20220101", end_date="20241231") == ("since", "20220101")
    assert classify_bucket({"symbol": "000001"}, start_date="20220101", end_date="20241231") == ("scope", "run_20220101_20241231")


def test_compact_preserves_rows_columns_writes_one_parquet_and_lineage(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "wave_20220101_20241231"
    _raw(root, family="fam", api="api", parts={"trade_date": "20220103"}, rows=2)
    _raw(root, family="fam", api="api", parts={"trade_date": "20220506"}, rows=3)
    _raw(root, family="fam", api="api", parts={"trade_date": "20230103"}, rows=1)
    pkg = Path("outputs/raw_acquisition_compact/promo").resolve()
    manifest = compact_raw_lake(root, promotion_name="promo")
    assets = manifest["compact_assets"]
    assert len([a for a in assets if a["source_family"] == "fam" and a["api_name"] == "api"]) == 2
    year2022 = next(a for a in assets if a["bucket_value"] == "2022")
    out = pkg / year2022["relative_path"]
    df = pd.read_parquet(out)
    assert len(df) == 5
    assert list(df.columns) == ["a", "b"]
    assert (pkg / "compact_source_lineage.csv").exists()
    lineage = pd.read_csv(pkg / "compact_source_lineage.csv")
    assert len(lineage) == 3
    assert (pkg / "compact_manifest.json").exists()
    assert (pkg / "compact_qa_report.csv").exists()
    assert (pkg / "raw_asset_inventory.csv").exists()
    assert (pkg / "known_gap_manifest.json").exists()
    assert (pkg / "raw_compact_classification.csv").exists()
    assert (pkg / "_LOCAL_COMPACT_READY.txt").exists()


def test_scan_raw_assets_parses_key_value_segments(tmp_path):
    root = tmp_path / "wave_20220101_20241231"
    _raw(root, parts={"symbol": "000001", "year": "2024"})
    assets = scan_raw_assets(root)
    assert assets[0].partitions == {"symbol": "000001", "year": "2024"}


def test_failed_backlog_uses_task_level_raw_ingest_catalog_not_api_summary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "wave_20220101_20241231"
    _raw(root, parts={"trade_date": "20220103"})
    rows = []
    for idx, status in enumerate(["failed", "timeout", "pending_adapter", "skipped", "success"], start=1):
        rows.append({
            "source_family": "fam",
            "api_name": "api",
            "status": status,
            "original_symbol": f"00000{idx}",
            "akshare_symbol": f"00000{idx}",
            "params_json": "{}",
            "partition_json": "{}",
            "task_key_json": f"task-{idx}",
            "error_type": "err" if status != "success" else "",
            "error_message": "bad" if status != "success" else "",
            "elapsed_sec": idx,
            "output_path": "",
        })
    pd.DataFrame(rows).to_csv(root / "raw_ingest_catalog.csv", index=False)
    op = root / "_operation_review"
    op.mkdir(parents=True)
    pd.DataFrame({"api_name": [f"api{i}" for i in range(6)], "status": ["failed"] * 6}).to_csv(op / "recovery_tasks.csv", index=False)

    manifest = compact_raw_lake(root, promotion_name="task_backlog")
    assert manifest["failed_backlog_task_count"] == 4
    assert len(manifest["failed_backlog_tasks"]) == 4
    assert {row["task_key_json"] for row in manifest["failed_backlog_tasks"]} == {"task-1", "task-2", "task-3", "task-4"}
