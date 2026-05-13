from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.raw_ingest import run_raw_ingest


def main() -> None:
    parser = argparse.ArgumentParser(description="Factor Lake tiny ingest runner")
    parser.add_argument("--root", default=".")
    parser.add_argument("--db", default="data/factor_lake/metastore.sqlite")
    args = parser.parse_args()

    root = Path(args.root)
    metastore = FactorLakeMetastore(root / args.db)
    run_raw_ingest("daily_bar_raw", root=str(root), metastore=metastore, symbol="000001", year="2024")
    run_raw_ingest("index_bar_raw", root=str(root), metastore=metastore, index_symbol="000300", year="2024")
    run_raw_ingest("margin_detail_raw", root=str(root), metastore=metastore, exchanges=["sse", "szse"], trade_date="2024-03-29")


if __name__ == "__main__":
    main()
