from __future__ import annotations

import json
from pathlib import Path
from dataclasses import asdict
from typing import Any

import pandas as pd

from qsys.data.factor_lake.schemas import SourceCase, SourceRunResult

def raw_partition_path(root: str | Path, source_family: str, api_name: str, partition: dict[str, str]) -> Path:
    path = Path(root) / "data" / "raw" / "akshare" / source_family / api_name
    for k, v in partition.items():
        path = path / f"{k}={v}"
    path.mkdir(parents=True, exist_ok=True)
    return path

def ensure_layout(output_root: str | Path, run_name: str | None = None) -> dict[str, Path]:
    root = Path(output_root)
    paths = {
        "root": root,
        "catalogs": root / "catalogs",
        "manifests": root / "manifests",
        "samples": root / "samples",
        "metadata": root / "metadata",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def write_inventory(cases: list[SourceCase], path: Path) -> None:
    pd.DataFrame([asdict(c) for c in cases]).to_csv(path, index=False, encoding="utf-8-sig")


def write_manifest(manifest: dict, path: Path) -> None:
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def write_catalog(results: list[SourceRunResult], path: Path) -> pd.DataFrame:
    columns = list(SourceRunResult.__dataclass_fields__.keys())
    if results:
        df = pd.DataFrame([asdict(r) for r in results])
    else:
        df = pd.DataFrame(columns=columns)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


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
