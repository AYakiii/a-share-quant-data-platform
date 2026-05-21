from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_official
from qsys.data.warehouse import RawWarehouseRunner, get_source_spec

P0_STAGE = "U1-M5 Step 3"
LOCAL_ONLY_NOTE = "local staging only; not Drive ingestion"

P0_GROUPS: dict[str, dict[str, Any]] = {
    "index_market_data": {
        "source_family": "index_market",
        "api_names": ["stock_zh_index_hist_csindex", "index_stock_cons_csindex", "index_stock_cons_weight_csindex"],
    },
    "sw_industry_data": {
        "source_family": "industry_concept",
        "api_names": ["sw_index_first_info", "sw_index_second_info", "sw_index_third_info", "index_component_sw", "index_hist_sw"],
    },
    "rescue_sources": {
        "source_specs": ["sw_industry_membership_rescue", "tradability_mask_v0"],
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument("--show-progress", action="store_true")
    return p.parse_args(argv)


def _validate_local_output_root(output_root: str) -> None:
    norm = output_root.lower().replace("\\", "/")
    blocked = ["/content/drive", "/content/gdrive", "mydrive", "/content/drive/mydrive/a_share_quant_cache", "/content/gdrive/mydrive/a_share_quant_cache"]
    if any(b in norm for b in blocked):
        raise ValueError(f"Drive path is not allowed for this runner: {output_root}")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _run_rescue_source(
    source_name: str,
    raw_root: Path,
    run_dir: Path,
    start_date: str,
    end_date: str,
    max_workers: int,
    show_progress: bool,
) -> list[dict[str, Any]]:
    started = datetime.now(UTC)
    runner = RawWarehouseRunner(
        source_spec=get_source_spec(source_name),
        raw_root=raw_root,
        output_dir=run_dir,
        run_name=source_name,
        max_workers=max_workers,
        show_progress=show_progress,
    )
    kwargs: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if source_name == "tradability_mask_v0":
        kwargs["raw_root"] = str(raw_root)
    out = runner.run(**kwargs)
    inv = pd.read_csv(out["inventory_csv"]) if Path(out["inventory_csv"]).exists() else pd.DataFrame()
    if inv.empty:
        return [{
            "source_group": "rescue_sources",
            "source_family": "trading_event" if source_name == "tradability_mask_v0" else "industry",
            "source_spec": source_name,
            "api_name": "",
            "status": "empty",
            "rows": 0,
            "output_path": "",
            "metadata_path": "",
            "error_type": "",
            "error_message": "",
            "started_at": started.isoformat(),
            "finished_at": _utc_now(),
            "elapsed_sec": max((datetime.now(UTC) - started).total_seconds(), 0.0),
        }]
    rows: list[dict[str, Any]] = []
    for _, rec in inv.iterrows():
        rows.append({
            "source_group": "rescue_sources",
            "source_family": "trading_event" if source_name == "tradability_mask_v0" else "industry",
            "source_spec": source_name,
            "api_name": str(rec.get("actual_api_name") or rec.get("requested_api_name") or source_name),
            "status": str(rec.get("status", "failed")),
            "rows": int(rec.get("rows", 0) or 0),
            "output_path": str(rec.get("path", "")),
            "metadata_path": "",
            "error_type": str(rec.get("error_type", "") or ""),
            "error_message": str(rec.get("error_message", "") or ""),
            "started_at": str(rec.get("started_at", started.isoformat())),
            "finished_at": str(rec.get("finished_at", _utc_now())),
            "elapsed_sec": float(rec.get("elapsed_seconds", 0.0) or 0.0),
        })
    return rows


def run_p0_wave(args: argparse.Namespace, ingest_fn: Callable[..., dict[str, Any]] = run_raw_ingest_official) -> dict[str, Path]:
    _validate_local_output_root(args.output_root)
    started = datetime.now(UTC)
    output_root = Path(args.output_root)
    run_dir = output_root / f"p0_wave_{started.strftime('%Y%m%dT%H%M%SZ')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []

    for group_name in ("index_market_data", "sw_industry_data"):
        group = P0_GROUPS[group_name]
        result = ingest_fn(
            output_root=str(run_dir),
            families=[group["source_family"]],
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=max(1, args.max_workers),
            continue_on_error=args.continue_on_error,
            show_progress=args.show_progress,
            selected_api_names=group["api_names"],
        )
        frame = pd.read_csv(result["catalog_csv"]) if Path(result["catalog_csv"]).exists() else pd.DataFrame(result.get("task_records", []))
        for _, rec in frame.iterrows():
            all_rows.append({
                "source_group": group_name,
                "source_family": str(rec.get("source_family", group["source_family"])),
                "source_spec": "",
                "api_name": str(rec.get("api_name", "")),
                "status": str(rec.get("status", "failed")),
                "rows": int(rec.get("rows", 0) or 0),
                "output_path": str(rec.get("output_path", "")),
                "metadata_path": str(rec.get("metadata_path", "")),
                "error_type": str(rec.get("error_type", "") or ""),
                "error_message": str(rec.get("error_message", "") or ""),
                "started_at": str(rec.get("started_at", _utc_now())),
                "finished_at": str(rec.get("finished_at", _utc_now())),
                "elapsed_sec": float(rec.get("elapsed_sec", 0.0) or 0.0),
            })

    raw_root = run_dir / "data" / "raw"
    for source_name in P0_GROUPS["rescue_sources"]["source_specs"]:
        rows = _run_rescue_source(source_name, raw_root=raw_root, run_dir=run_dir, start_date=args.start_date, end_date=args.end_date, max_workers=max(1, args.max_workers), show_progress=args.show_progress)
        all_rows.extend(rows)

    catalog = pd.DataFrame(all_rows)
    for col in ["source_group", "source_family", "source_spec", "api_name", "status", "rows", "output_path", "metadata_path", "error_type", "error_message", "started_at", "finished_at", "elapsed_sec"]:
        if col not in catalog.columns:
            catalog[col] = "" if col not in {"rows", "elapsed_sec"} else 0

    counts = Counter(catalog["status"].tolist()) if not catalog.empty else Counter()
    summary = {
        "total_tasks": int(len(catalog)),
        "success_count": int(counts.get("success", 0)),
        "failed_count": int(counts.get("failed", 0)),
        "empty_count": int(counts.get("empty", 0)),
        "skipped_count": int(counts.get("skipped", 0)),
        "rows_by_source_group": {k: int(v) for k, v in catalog.groupby("source_group")["rows"].sum().to_dict().items()} if not catalog.empty else {},
        "failed_sources": sorted(set((catalog[catalog["status"] == "failed"]["api_name"].fillna("") + catalog[catalog["status"] == "failed"]["source_spec"].fillna("")).tolist())),
    }

    finished = datetime.now(UTC)
    manifest = {
        "stage": P0_STAGE,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "output_root": str(output_root),
        "max_workers": max(1, args.max_workers),
        "continue_on_error": bool(args.continue_on_error),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_sec": max((finished - started).total_seconds(), 0.0),
        "status_counts": {k: int(v) for k, v in counts.items()},
        "source_groups": list(P0_GROUPS.keys()),
        "note": LOCAL_ONLY_NOTE,
    }

    catalog_fp = run_dir / "p0_wave_catalog.csv"
    summary_fp = run_dir / "p0_wave_summary.json"
    manifest_fp = run_dir / "p0_wave_manifest.json"
    catalog.to_csv(catalog_fp, index=False)
    summary_fp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest_fp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    if counts.get("failed", 0) and not args.continue_on_error:
        raise RuntimeError("P0 wave completed with failures and --continue-on-error is not set")
    return {"run_dir": run_dir, "catalog_csv": catalog_fp, "summary_json": summary_fp, "manifest_json": manifest_fp}


def main() -> None:
    args = parse_args()
    out = run_p0_wave(args)
    print(json.dumps({k: str(v) for k, v in out.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
