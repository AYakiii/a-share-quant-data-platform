"""Base contract and deterministic persistence for raw source adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

SOURCE_INVENTORY_VERSION = "akshare_free_factor_source_inventory_v0"


@dataclass(slots=True)
class SourceFetchResult:
    """Container for a raw-source fetch result and normalized metadata."""

    api_name: str
    source_family: str
    raw: pd.DataFrame
    metadata: dict[str, Any]


def build_source_metadata(
    *,
    api_name: str,
    source_family: str,
    request_params: dict[str, Any],
    raw: pd.DataFrame,
    normalized_columns: list[str] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    """Build normalized metadata for raw source fetches."""

    out: dict[str, Any] = {
        "api_name": api_name,
        "source_family": source_family,
        "request_params": request_params,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(raw)),
        "column_count": int(raw.shape[1]),
        "raw_columns": [str(c) for c in raw.columns],
        "normalized_columns": normalized_columns or [],
        "source_inventory_version": SOURCE_INVENTORY_VERSION,
    }
    if notes:
        out["notes"] = notes
    return out


def write_source_fetch_result(
    result: SourceFetchResult,
    output_root: str | Path,
    dataset_name: str,
    partition_values: dict[str, str] | None = None,
) -> dict[str, Path]:
    """Persist raw data and metadata with deterministic directory/key ordering."""

    root = Path(output_root) / f"dataset_name={dataset_name}"
    if partition_values:
        for k, v in sorted(partition_values.items()):
            root = root / f"{k}={v}"
    root.mkdir(parents=True, exist_ok=True)

    data_fp = root / "data.csv"
    meta_fp = root / "metadata.json"

    result.raw.to_csv(data_fp, index=False, encoding="utf-8")
    meta_fp.write_text(json.dumps(result.metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return {"data": data_fp, "metadata": meta_fp}
