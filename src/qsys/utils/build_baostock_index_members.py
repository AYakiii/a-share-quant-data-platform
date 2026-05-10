"""Build CSI500 historical constituent snapshots from BaoStock."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from qsys.universe.baostock import STANDARD_COLUMNS, fetch_csi500_members


def _normalize_freq(freq: str) -> str:
    f = freq.upper()
    mapping = {"ME": "M", "QE": "Q", "YE": "Y"}
    try:
        pd.date_range("2024-01-01", "2024-12-31", freq=f)
        return f
    except Exception:
        return mapping.get(f, f)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build CSI500 snapshots from BaoStock")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--output-root", default="data/raw/index_constituents/baostock")
    p.add_argument("--freq", default="ME")
    p.add_argument("--sleep-seconds", type=float, default=0.2)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    import baostock as bs

    lg = bs.login()
    if str(getattr(lg, "error_code", "1")) != "0":
        raise RuntimeError(f"BaoStock login failed: error_code={lg.error_code}, error_msg={getattr(lg, 'error_msg', '')}")

    all_frames: list[pd.DataFrame] = []
    freq = _normalize_freq(args.freq)
    dates = pd.date_range(args.start_date, args.end_date, freq=freq)

    try:
        for d in dates:
            snap = fetch_csi500_members(d.strftime("%Y-%m-%d"))
            if not snap.empty:
                all_frames.append(snap)
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    if not all_frames:
        print("total rows: 0")
        print("number of snapshots: 0")
        print("min snapshot_date: NA")
        print("max snapshot_date: NA")
        print(f"output path: {Path(args.output_root)}")
        return

    full = pd.concat(all_frames, ignore_index=True)
    full = full.drop_duplicates(subset=["index_name", "snapshot_date", "asset"]) \
        .sort_values(["snapshot_date", "asset"]).reset_index(drop=True)

    out_root = Path(args.output_root)
    for year, g in full.groupby(full["snapshot_date"].dt.year):
        part = out_root / "index_name=csi500" / f"year={year}"
        part.mkdir(parents=True, exist_ok=True)
        g[STANDARD_COLUMNS].to_parquet(part / "data.parquet", index=False)

    print(f"total rows: {len(full)}")
    print(f"number of snapshots: {full['snapshot_date'].nunique()}")
    print(f"min snapshot_date: {full['snapshot_date'].min()}")
    print(f"max snapshot_date: {full['snapshot_date'].max()}")
    print(f"output path: {out_root}")


if __name__ == "__main__":
    main()
