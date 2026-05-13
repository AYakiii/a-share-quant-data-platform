from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .schemas import SourceCase, SourceRunResult


def ensure_layout(output_root: str | Path) -> dict[str, Path]:
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
    df = pd.DataFrame([asdict(r) for r in results])
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df
