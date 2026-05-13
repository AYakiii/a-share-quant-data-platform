from __future__ import annotations

import argparse
from pathlib import Path

from qsys.data.warehouse.normalized_margin_panel import MarginPanelBuildConfig, build_normalized_margin_panel


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build normalized monthly margin panel from raw margin_detail parquet partitions.")
    p.add_argument("--raw-root", required=True)
    p.add_argument("--raw-dataset-path", default=None)
    p.add_argument("--output-root", required=True)
    p.add_argument("--output-dataset-path", default=None)
    p.add_argument("--artifact-dir", default=None)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--exchanges", default="both", choices=["sse", "szse", "both"])
    p.add_argument("--source-version", default="v1")
    p.add_argument("--normalized-version", default="v1")
    p.add_argument("--output-partition", default="month")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--strict-schema", dest="strict_schema", action="store_true")
    p.add_argument("--no-strict-schema", dest="strict_schema", action="store_false")
    p.set_defaults(strict_schema=True)
    p.add_argument("--allow-empty-raw-files", dest="allow_empty_raw_files", action="store_true")
    p.add_argument("--no-allow-empty-raw-files", dest="allow_empty_raw_files", action="store_false")
    p.set_defaults(allow_empty_raw_files=True)
    p.add_argument("--include-name", dest="include_name", action="store_true")
    p.add_argument("--no-include-name", dest="include_name", action="store_false")
    p.set_defaults(include_name=True)
    p.add_argument("--show-progress", action="store_true")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    ex = ("SSE", "SZSE") if a.exchanges == "both" else (("SSE",) if a.exchanges == "sse" else ("SZSE",))
    cfg = MarginPanelBuildConfig(
        raw_root=Path(a.raw_root),
        raw_dataset_path=Path(a.raw_dataset_path) if a.raw_dataset_path else None,
        output_root=Path(a.output_root),
        output_dataset_path=Path(a.output_dataset_path) if a.output_dataset_path else None,
        artifact_dir=Path(a.artifact_dir) if a.artifact_dir else None,
        start_date=a.start_date,
        end_date=a.end_date,
        exchanges=ex,
        source_version=a.source_version,
        normalized_version=a.normalized_version,
        output_partition=a.output_partition,
        overwrite=a.overwrite,
        strict_schema=a.strict_schema,
        allow_empty_raw_files=a.allow_empty_raw_files,
        include_name=a.include_name,
    )
    result = build_normalized_margin_panel(cfg, show_progress=a.show_progress)
    print({"rows": result.summary["n_rows"], "files": result.summary["n_output_files"], "output": result.summary["output_dataset_path"]})


if __name__ == "__main__":
    main()
