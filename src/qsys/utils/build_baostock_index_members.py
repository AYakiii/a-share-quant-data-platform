"""Build CSI500 historical constituent snapshots from BaoStock."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from qsys.universe.baostock import fetch_csi500_members


def _normalize_freq(freq: str) -> str:
    f = freq.upper()
    mapping = {"ME": "M", "QE": "Q", "YE": "Y"}
    f2 = mapping.get(f, f)
    if f2 not in {"M", "Q", "Y"}:
        raise ValueError("freq must be one of ME,M,QE,Q,YE,Y")
    return f2


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build BaoStock CSI500 constituent snapshots")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--output-root", default="data/raw/index_constituents/baostock")
    p.add_argument("--freq", default="ME")
    p.add_argument("--sleep-seconds", type=float, default=0.2)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    freq = _normalize_freq(args.freq)
    snapshot_dates = pd.date_range(args.start_date, args.end_date, freq=freq)

    import baostock as bs

    login_rs = bs.login()
    if str(getattr(login_rs, "error_code", "1")) != "0":
        raise RuntimeError(
            f"BaoStock login failed: error_code={getattr(login_rs, 'error_code', '')}, error_msg={getattr(login_rs, 'error_msg', '')}"
        )

    frames: list[pd.DataFrame] = []
    try:
        for d in snapshot_dates:
            df = fetch_csi500_members(d.strftime("%Y-%m-%d"))
            if not df.empty:
                frames.append(df)
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    if not frames:
        print("total rows: 0")
        print("number of snapshots: 0")
        print("min snapshot_date: None")
        print("max snapshot_date: None")
        print(f"output path: {args.output_root}")
        return

    out = pd.concat(frames, ignore_index=True)
    out["snapshot_date"] = pd.to_datetime(out["snapshot_date"])  # ensure dtype
    out = out.drop_duplicates(subset=["index_name", "snapshot_date", "asset"])
    out = out.sort_values(["snapshot_date", "asset"], kind="mergesort").reset_index(drop=True)

    root = Path(args.output_root)
    for year, g in out.groupby(out["snapshot_date"].dt.year):
        part = root / "index_name=csi500" / f"year={int(year)}"
        part.mkdir(parents=True, exist_ok=True)
        g.to_parquet(part / "data.parquet", index=False)

    print(f"total rows: {len(out)}")
    print(f"number of snapshots: {out['snapshot_date'].nunique()}")
    print(f"min snapshot_date: {out['snapshot_date'].min()}")
    print(f"max snapshot_date: {out['snapshot_date'].max()}")
    print(f"output path: {root}")


if __name__ == "__main__":
    main()
