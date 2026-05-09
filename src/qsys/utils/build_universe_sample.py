"""Build configurable universe sample from one or more index components."""

from __future__ import annotations

import argparse
from pathlib import Path

from qsys.universe.csindex import build_universe_sample


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build sampled universe from index components")
    p.add_argument("--index-list", nargs="+", required=True)
    p.add_argument("--n", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-dir", default="data/universe")
    p.add_argument("--name", default="csi_sample")
    p.add_argument("--output-symbols", default=None)
    p.add_argument("--output-metadata", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    symbols, meta = build_universe_sample(args.index_list, n=args.n, seed=args.seed)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols_path = Path(args.output_symbols) if args.output_symbols else out_dir / f"{args.name}_n{args.n}_seed{args.seed}_symbols.txt"
    metadata_path = Path(args.output_metadata) if args.output_metadata else out_dir / f"{args.name}_n{args.n}_seed{args.seed}_metadata.csv"

    symbols_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    symbols_path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    meta.to_csv(metadata_path, index=False)

    print(symbols_path)
    print(metadata_path)


if __name__ == "__main__":
    main()
