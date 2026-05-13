from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def raw_partition_path(root: str | Path, source_family: str, api_name: str, partition: dict[str, str]) -> Path:
    path = Path(root) / "data" / "raw" / "akshare" / source_family / api_name
    for k, v in partition.items():
        path = path / f"{k}={v}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_raw_partition(root: str | Path, source_family: str, api_name: str, partition: dict[str, str], raw: pd.DataFrame, metadata: dict[str, Any]) -> tuple[Path, Path]:
    out_dir = raw_partition_path(root, source_family, api_name, partition)
    data_path = out_dir / "data.parquet"
    meta_path = out_dir / "metadata.json"
    raw.to_parquet(data_path, index=False)
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path, meta_path
