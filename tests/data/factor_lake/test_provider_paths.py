from __future__ import annotations

from qsys.data.factor_lake.io import raw_partition_path
from qsys.data.factor_lake.raw_compact import compact_asset_relative_root, drive_raw_relative_root, local_ingest_raw_relative_root


def test_default_provider_paths_are_akshare(tmp_path):
    path = raw_partition_path(tmp_path, "market_price", "daily", {"year": "2024"})
    assert str(path.relative_to(tmp_path)) == "data/raw/akshare/market_price/daily/year=2024"
    assert str(local_ingest_raw_relative_root()) == "data/raw/akshare"
    assert str(drive_raw_relative_root()) == "raw/akshare"


def test_tushare_provider_paths_include_provider_and_schema():
    assert str(local_ingest_raw_relative_root("tushare")) == "data/raw/tushare"
    assert str(compact_asset_relative_root("tushare") / "market_price" / "daily" / "v1" / "year=2024" / "data.parquet") == "raw/tushare/market_price/daily/v1/year=2024/data.parquet"
    assert str(drive_raw_relative_root("tushare")) == "raw/tushare"
