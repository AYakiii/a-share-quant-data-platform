from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import akshare as ak
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
        # NOTE: tradability_mask_v0 is intentionally excluded from the default P0 raw wave.
        # It is a derived dataset built from stock_zh_a_daily after daily raw data is available.
        "source_specs": ["sw_industry_membership_rescue"],
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
    p.add_argument("--request-sleep", type=float, default=0.0)
    p.add_argument("--task-timeout-sec", type=float, default=None)
    p.add_argument("--task-retry-attempts", type=int, default=0)
    p.add_argument("--task-retry-sleep-sec", type=float, default=0.0)
    p.add_argument("--task-retry-backoff", type=float, default=1.0)
    p.add_argument("--task-retry-jitter-sec", type=float, default=0.0)
    p.add_argument("--symbols", default="")
    p.add_argument("--symbols-file", default="")
    p.add_argument("--index-symbols", default="")
    p.add_argument("--trade-dates", default="")
    p.add_argument("--report-dates", default="")
    p.add_argument("--industry-names", default="")
    p.add_argument("--concept-names", default="")
    p.add_argument("--universe-root", default="config/factor_sources/acquisition_universe")
    p.add_argument("--include-disabled", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--auto-recover-failed", action="store_true")
    p.add_argument("--recovery-max-workers", type=int, default=1)
    p.add_argument("--recovery-request-sleep", type=float, default=0.5)
    p.add_argument("--recovery-task-timeout-sec", type=float, default=120.0)
    p.add_argument("--recovery-task-retry-attempts", type=int, default=2)
    p.add_argument("--recovery-task-retry-sleep-sec", type=float, default=1.0)
    p.add_argument("--recovery-task-retry-backoff", type=float, default=1.5)
    p.add_argument("--recovery-task-retry-jitter-sec", type=float, default=0.2)
    return p.parse_args(argv)


def _split_csv(v: str) -> list[str]:
    return [x.strip() for x in str(v or "").split(",") if x.strip()]


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_symbols_file(path_text: str) -> list[str]:
    if not path_text:
        return []
    rows = Path(path_text).read_text(encoding="utf-8").splitlines()
    items = [line.strip() for line in rows if line.strip() and not line.strip().startswith("#")]
    return _dedupe_keep_order(items)


def _validate_local_output_root(output_root: str) -> None:
    norm = output_root.lower().replace("\\", "/")
    blocked = ["/content/drive", "/content/gdrive", "mydrive", "/content/drive/mydrive/a_share_quant_cache", "/content/gdrive/mydrive/a_share_quant_cache"]
    if any(b in norm for b in blocked):
        raise ValueError(f"Drive path is not allowed for this runner: {output_root}")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        if pd.isna(value):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() in {"nan", "none", "null"}:
            return ""
        return cleaned
    if pd.isna(value):
        return ""
    return str(value).strip()


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
    rescue_run_dir = Path(str(out.get("run_dir") or (run_dir / source_name)))
    inventory_path = rescue_run_dir / "cache_inventory.csv"
    if not inventory_path.exists():
        return [{
            "source_group": "rescue_sources",
            "source_family": "trading_event" if source_name == "tradability_mask_v0" else "industry",
            "source_spec": source_name,
            "api_name": source_name,
            "status": "failed",
            "rows": 0,
            "output_path": "",
            "metadata_path": "",
            "error_type": "MissingArtifactError",
            "error_message": f"Expected rescue inventory not found: {inventory_path}",
            "started_at": started.isoformat(),
            "finished_at": _utc_now(),
            "elapsed_sec": max((datetime.now(UTC) - started).total_seconds(), 0.0),
        }]
    inv = pd.read_csv(inventory_path)
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
            "rows": _safe_int(rec.get("rows", 0), default=0),
            "output_path": str(rec.get("path", "")),
            "metadata_path": "",
            "error_type": str(rec.get("error_type", "") or ""),
            "error_message": str(rec.get("error_message", "") or ""),
            "started_at": str(rec.get("started_at", started.isoformat())),
            "finished_at": str(rec.get("finished_at", _utc_now())),
            "elapsed_sec": _safe_float(rec.get("elapsed_seconds", 0.0), default=0.0),
        })
    return rows


def run_p0_wave(args: argparse.Namespace, ingest_fn: Callable[..., dict[str, Any]] = run_raw_ingest_official) -> dict[str, Path]:
    _validate_local_output_root(args.output_root)
    started = datetime.now(UTC)
    output_root = Path(args.output_root)
    run_dir = output_root / f"p0_wave_{started.strftime('%Y%m%dT%H%M%SZ')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    symbols = _dedupe_keep_order(_split_csv(args.symbols) + _load_symbols_file(args.symbols_file))
    index_symbols = _dedupe_keep_order(_split_csv(args.index_symbols))
    trade_dates = _dedupe_keep_order(_split_csv(args.trade_dates))
    report_dates = _dedupe_keep_order(_split_csv(args.report_dates))
    industry_names = _dedupe_keep_order(_split_csv(args.industry_names))
    concept_names = _dedupe_keep_order(_split_csv(args.concept_names))

    all_rows: list[dict[str, Any]] = []

    for group_name in ("index_market_data", "sw_industry_data"):
        group = P0_GROUPS[group_name]
        result = ingest_fn(
            output_root=str(run_dir),
            families=[group["source_family"]],
            symbols=symbols,
            index_symbols=index_symbols,
            trade_dates=trade_dates,
            report_dates=report_dates,
            industry_names=industry_names,
            concept_names=concept_names,
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=max(1, args.max_workers),
            continue_on_error=args.continue_on_error,
            selected_api_names=group["api_names"],
            universe_root=args.universe_root,
            include_disabled=args.include_disabled,
            resume=args.resume,
            ak_module=ak,
            request_sleep=args.request_sleep,
            task_timeout_sec=args.task_timeout_sec,
            task_retry_attempts=args.task_retry_attempts,
            task_retry_sleep_sec=args.task_retry_sleep_sec,
            task_retry_backoff=args.task_retry_backoff,
            task_retry_jitter_sec=args.task_retry_jitter_sec,
        )
        catalog_path = str(result.get("catalog_csv") or result.get("catalog_path") or "")
        if not catalog_path:
            fallback_catalog = run_dir / "raw_ingest_catalog.csv"
            if fallback_catalog.exists():
                catalog_path = str(fallback_catalog)
        frame = pd.read_csv(catalog_path) if catalog_path and Path(catalog_path).exists() else pd.DataFrame(result.get("task_records", []))
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
    failed_sources: list[str] = []
    if not catalog.empty:
        failed_catalog = catalog[catalog["status"] == "failed"]
        for _, rec in failed_catalog.iterrows():
            api_name = _safe_text(rec.get("api_name"))
            source_spec = _safe_text(rec.get("source_spec"))
            label = api_name or source_spec
            if label:
                failed_sources.append(label)
    summary = {
        "total_tasks": int(len(catalog)),
        "success_count": int(counts.get("success", 0)),
        "failed_count": int(counts.get("failed", 0)),
        "empty_count": int(counts.get("empty", 0)),
        "skipped_count": int(counts.get("skipped", 0)),
        "rows_by_source_group": {k: int(v) for k, v in catalog.groupby("source_group")["rows"].sum().to_dict().items()} if not catalog.empty else {},
        "failed_sources": sorted(set(failed_sources)),
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

    if bool(getattr(args, "auto_recover_failed", False)):
        failed_mask = catalog["status"].astype(str).isin(["failed", "timeout"]) if not catalog.empty else pd.Series([], dtype=bool)
        failed_rows = catalog[failed_mask].copy() if not catalog.empty else pd.DataFrame()
        recoverable = failed_rows[failed_rows["source_group"].isin(["index_market_data", "sw_industry_data"])] if not failed_rows.empty else pd.DataFrame()

        recovery_rows: list[dict[str, Any]] = []
        all_failed_keys: list[tuple[str, str]] = []
        if not failed_rows.empty:
            for _, rec in failed_rows.iterrows():
                source_family = _safe_text(rec.get("source_family"))
                api_name = _safe_text(rec.get("api_name"))
                source_spec = _safe_text(rec.get("source_spec"))
                key_api = api_name or source_spec
                if source_family and key_api:
                    all_failed_keys.append((source_family, key_api))
        attempted_keys: list[tuple[str, str]] = []
        if not recoverable.empty:
            recoverable_pairs = (
                recoverable[["source_family", "api_name"]]
                .dropna()
                .assign(
                    source_family=lambda d: d["source_family"].astype(str),
                    api_name=lambda d: d["api_name"].astype(str),
                )
            )
            for source_family, api_name in recoverable_pairs.itertuples(index=False):
                attempted_keys.append((source_family, api_name))
            for source_family, api_names in recoverable_pairs.groupby("source_family")["api_name"]:
                result = ingest_fn(
                    output_root=str(run_dir),
                    families=[str(source_family)],
                    symbols=symbols,
                    index_symbols=index_symbols,
                    trade_dates=trade_dates,
                    report_dates=report_dates,
                    industry_names=industry_names,
                    concept_names=concept_names,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_workers=max(1, int(getattr(args, "recovery_max_workers", 1))),
                    continue_on_error=True,
                    selected_api_names=[str(x) for x in api_names.tolist()],
                    universe_root=args.universe_root,
                    include_disabled=args.include_disabled,
                    resume=args.resume,
                    ak_module=ak,
                    request_sleep=float(getattr(args, "recovery_request_sleep", 0.5)),
                    task_timeout_sec=getattr(args, "recovery_task_timeout_sec", 120.0),
                    task_retry_attempts=int(getattr(args, "recovery_task_retry_attempts", 2)),
                    task_retry_sleep_sec=float(getattr(args, "recovery_task_retry_sleep_sec", 1.0)),
                    task_retry_backoff=float(getattr(args, "recovery_task_retry_backoff", 1.5)),
                    task_retry_jitter_sec=float(getattr(args, "recovery_task_retry_jitter_sec", 0.2)),
                )
                catalog_path = str(result.get("catalog_csv") or result.get("catalog_path") or "")
                frame = pd.read_csv(catalog_path) if catalog_path and Path(catalog_path).exists() else pd.DataFrame(result.get("task_records", []))
                for _, rec in frame.iterrows():
                    rec_api_name = str(rec.get("api_name", ""))
                    status = str(rec.get("status", "failed"))
                    recovery_rows.append({
                        "source_family": str(rec.get("source_family", source_family)),
                        "api_name": rec_api_name,
                        "status": status,
                        "rows": int(rec.get("rows", 0) or 0),
                        "output_path": str(rec.get("output_path", "")),
                        "metadata_path": str(rec.get("metadata_path", "")),
                        "error_type": str(rec.get("error_type", "") or ""),
                        "error_message": str(rec.get("error_message", "") or ""),
                        "started_at": str(rec.get("started_at", _utc_now())),
                        "finished_at": str(rec.get("finished_at", _utc_now())),
                        "elapsed_sec": float(rec.get("elapsed_sec", 0.0) or 0.0),
                    })
        recovery_status_by_pair: dict[tuple[str, str], set[str]] = {}
        for rec in recovery_rows:
            key = (str(rec.get("source_family", "")), str(rec.get("api_name", "")))
            recovery_status_by_pair.setdefault(key, set()).add(str(rec.get("status", "")))

        recovered_keys: set[tuple[str, str]] = set()
        for pair in sorted(set(attempted_keys)):
            statuses = recovery_status_by_pair.get(pair, set())
            has_success = "success" in statuses
            has_failed_or_timeout = ("failed" in statuses) or ("timeout" in statuses)
            if has_success and not has_failed_or_timeout:
                recovered_keys.add(pair)

        unresolved = []
        for pair in sorted(set(all_failed_keys)):
            if pair not in recovered_keys:
                unresolved.append({"source_family": pair[0], "api_name": pair[1]})

        recovery_catalog_fp = run_dir / "p0_recovery_catalog.csv"
        pd.DataFrame(recovery_rows).to_csv(recovery_catalog_fp, index=False)
        report = {
            "main_total_tasks": int(len(catalog)),
            "main_failed_count": int((catalog["status"] == "failed").sum()) if not catalog.empty else 0,
            "main_timeout_count": int((catalog["status"] == "timeout").sum()) if not catalog.empty else 0,
            "recovery_attempted_count": int(len(set(attempted_keys))),
            "recovered_count": int(len(recovered_keys)),
            "recovery_failed_count": int(len(unresolved)),
            "unresolved_failed_count": int(len(unresolved)),
            "unresolved_failed_sources": unresolved,
            "final_status": "accepted" if len(unresolved) == 0 else "failed",
            "note": LOCAL_ONLY_NOTE,
        }
        (run_dir / "p0_final_acceptance_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if counts.get("failed", 0) and not args.continue_on_error:
        raise RuntimeError("P0 wave completed with failures and --continue-on-error is not set")
    return {"run_dir": run_dir, "catalog_csv": catalog_fp, "summary_json": summary_fp, "manifest_json": manifest_fp}


def main() -> None:
    args = parse_args()
    out = run_p0_wave(args)
    print(json.dumps({k: str(v) for k, v in out.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
