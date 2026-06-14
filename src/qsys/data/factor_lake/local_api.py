from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.io import raw_partition_path
from qsys.data.factor_lake.registry import DATASET_REGISTRY


def read_raw_partition(root: str | Path, dataset: str, api_name: str, partition: dict[str, str], *, provider: str = "akshare") -> pd.DataFrame:
    """Read a local Raw partition from data/raw/<provider>; defaults to legacy AkShare."""
    family = DATASET_REGISTRY[dataset].source_family
    path = raw_partition_path(root, family, api_name, partition, provider=provider) / "data.parquet"
    return pd.read_parquet(path)


def read_partition_metadata(root: str | Path, dataset: str, api_name: str, partition: dict[str, str], *, provider: str = "akshare") -> dict:
    """Read local Raw partition metadata from data/raw/<provider>; defaults to legacy AkShare."""
    family = DATASET_REGISTRY[dataset].source_family
    meta_path = raw_partition_path(root, family, api_name, partition, provider=provider) / "metadata.json"
    return json.loads(meta_path.read_text(encoding="utf-8"))
