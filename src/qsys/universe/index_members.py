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


def apply_pit_index_universe_mask(
    features: pd.DataFrame,
    *,
    universe_root: str | Path,
    index_name: str = "csi500",
) -> pd.DataFrame:
    """Filter feature rows to point-in-time index members per trade date.

    Features must be indexed by MultiIndex ['date', 'asset'].
    For each trade date, membership uses latest snapshot_date <= trade date.
    """

    if not isinstance(features.index, pd.MultiIndex) or list(features.index.names) != ["date", "asset"]:
        raise ValueError("features index must be MultiIndex ['date', 'asset']")

    snaps = load_index_member_snapshots(root=universe_root, index_name=index_name)
    snaps = snaps[snaps["is_member"] == 1].copy()
    if snaps.empty:
        return features.iloc[0:0].copy()

    dates = pd.to_datetime(features.index.get_level_values("date")).unique()
    mapping_rows: list[dict[str, object]] = []
    for d in pd.Series(dates).sort_values():
        eligible = snaps[snaps["snapshot_date"] <= pd.Timestamp(d)]
        if eligible.empty:
            continue
        latest = eligible["snapshot_date"].max()
        assets = eligible.loc[eligible["snapshot_date"] == latest, "asset"].dropna().astype(str).unique()
        for a in assets:
            mapping_rows.append({"date": pd.Timestamp(d), "asset": a})

    if not mapping_rows:
        return features.iloc[0:0].copy()

    allowed = pd.DataFrame(mapping_rows).drop_duplicates()
    allowed_idx = pd.MultiIndex.from_frame(allowed[["date", "asset"]], names=["date", "asset"])
    return features.loc[features.index.intersection(allowed_idx)].sort_index()
