"""Point-in-time index member snapshot loaders."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.universe.baostock import STANDARD_COLUMNS


def load_index_member_snapshots(
    root: str | Path,
    index_name: str = "csi500",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Load saved index member snapshots from parquet partitions."""

    base = Path(root) / f"index_name={index_name}"
    files = sorted(base.glob("year=*/data.parquet"))
    if not files:
        raise FileNotFoundError(f"No snapshot parquet files found under: {base}")

    frames = [pd.read_parquet(fp) for fp in files]
    out = pd.concat(frames, ignore_index=True)
    for col in STANDARD_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out["snapshot_date"] = pd.to_datetime(out["snapshot_date"], errors="coerce")
    if start_date:
        out = out[out["snapshot_date"] >= pd.Timestamp(start_date)]
    if end_date:
        out = out[out["snapshot_date"] <= pd.Timestamp(end_date)]

    return out[STANDARD_COLUMNS].sort_values(["snapshot_date", "asset"]).reset_index(drop=True)


def load_index_members_asof(
    root: str | Path,
    as_of_date: str,
    index_name: str = "csi500",
) -> pd.DataFrame:
    """Load the latest snapshot not later than ``as_of_date``."""

    snaps = load_index_member_snapshots(root=root, index_name=index_name)
    asof = pd.Timestamp(as_of_date)
    eligible = snaps[snaps["snapshot_date"] <= asof]
    if eligible.empty:
        raise ValueError(f"No snapshot found on/before as_of_date={as_of_date}")

    latest = eligible["snapshot_date"].max()
    out = eligible[eligible["snapshot_date"] == latest].copy()
    return out.sort_values("asset").reset_index(drop=True)
