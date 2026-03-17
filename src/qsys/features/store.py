"""Feature materialization and storage for Feature Store v1."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import pandas as pd

from qsys.data.panel.daily_panel import load_daily_panel
from qsys.features.compute import compute_features


@dataclass(frozen=True)
class FeatureStoreConfig:
    """Runtime configuration for Feature Store v1."""

    dataset_root: Path = Path("data/standardized/market/daily_bars")
    output_root: Path = Path("data/processed/feature_store/v1")
    metadata_db: Path = Path("data/processed/feature_store/metadata.db")
    source_dataset: str = "standardized_daily_bars"
    version: str = "v1"


def materialize_features(
    feature_names: Sequence[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Sequence[str] | None = None,
    config: FeatureStoreConfig | None = None,
) -> pd.DataFrame:
    """Load panel data and compute selected features."""

    cfg = config or FeatureStoreConfig()
    panel_columns = ["close", "amount", "market_cap"]
    panel = load_daily_panel(
        dataset_root=cfg.dataset_root,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        columns=panel_columns,
    )
    return compute_features(panel, feature_names)


def write_feature_store(df: pd.DataFrame, *, config: FeatureStoreConfig | None = None) -> Path:
    """Write feature data under `data/processed/feature_store/v1/` partitioned by date."""

    cfg = config or FeatureStoreConfig()
    out_root = cfg.output_root
    out_root.mkdir(parents=True, exist_ok=True)

    if df.empty:
        return out_root

    materialized = df.reset_index().copy()
    materialized["date"] = pd.to_datetime(materialized["date"]).dt.strftime("%Y-%m-%d")

    for d, part in materialized.groupby("date"):
        part_dir = out_root / f"trade_date={d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part.to_parquet(part_dir / "data.parquet", index=False)

    return out_root


def record_feature_metadata(
    feature_names: Sequence[str],
    *,
    config: FeatureStoreConfig | None = None,
) -> None:
    """Record minimal feature metadata in sqlite sidecar db."""

    cfg = config or FeatureStoreConfig()
    cfg.metadata_db.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(cfg.metadata_db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_registry (
                feature_name TEXT,
                version TEXT,
                source_dataset TEXT,
                created_at TEXT
            )
            """
        )

        created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = [(name, cfg.version, cfg.source_dataset, created_at) for name in feature_names]
        conn.executemany(
            "INSERT INTO feature_registry (feature_name, version, source_dataset, created_at) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def materialize_and_store_features(
    feature_names: Sequence[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Sequence[str] | None = None,
    config: FeatureStoreConfig | None = None,
) -> pd.DataFrame:
    """Compute features, persist to parquet store, and append metadata records."""

    cfg = config or FeatureStoreConfig()
    features = materialize_features(
        feature_names,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        config=cfg,
    )
    write_feature_store(features, config=cfg)
    record_feature_metadata(feature_names, config=cfg)
    return features
