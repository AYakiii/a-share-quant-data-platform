from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

import akshare as ak
import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_official


def _split_csv(v: str) -> list[str]:
    return [x.strip() for x in v.split(",") if x.strip()]


def _parse_symbols(symbols_arg: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for symbol in _split_csv(symbols_arg):
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _build_symbol_batches(symbols: list[str], batch_size: int) -> list[tuple[int, int, list[str]]]:
    if batch_size <= 0:
        return []
    batches: list[tuple[int, int, list[str]]] = []
    for start in range(0, len(symbols), batch_size):
        end_exclusive = min(start + batch_size, len(symbols))
        batches.append((start, end_exclusive - 1, symbols[start:end_exclusive]))
    return batches


def _batch_label(start: int, end: int) -> str:
    return f"batch{start:04d}_{end:04d}"


def _rewrite_to_master_path(path_text: str, batch_output_root: Path, output_root: Path) -> str:
    raw = str(path_text or "")
    if not raw:
        return raw
    try:
        p = Path(raw)
        rel = p.relative_to(batch_output_root)
        return str(output_root / rel)
    except Exception:  # noqa: BLE001
        return raw.replace(str(batch_output_root), str(output_root), 1)


def _merge_batch_raw(batch_output_root: Path, output_root: Path, batch_label: str, conflicts: list[dict[str, str]]) -> tuple[int, int]:
    src_root = batch_output_root / "data" / "raw"
    dst_root = output_root / "data" / "raw"
    merged_files = 0
    conflict_files = 0
    if not src_root.exists():
        return merged_files, conflict_files
    for src_path in src_root.rglob("*"):
        if src_path.is_dir():
            continue
        rel = src_path.relative_to(src_root)
        dst_path = dst_root / rel
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if dst_path.exists():
            conflict_files += 1
            conflicts.append({
                "batch_label": batch_label,
                "source_path": str(src_path),
                "destination_path": str(dst_path),
                "action": "skipped_existing",
            })
            continue
        shutil.copy2(src_path, dst_path)
        merged_files += 1
    return merged_files, conflict_files


def _run_without_batching(args: argparse.Namespace) -> dict:
    return run_raw_ingest_official(
        output_root=args.output_root,
        families=_split_csv(args.families),
        symbols=_split_csv(args.symbols),
        index_symbols=_split_csv(args.index_symbols),
        trade_dates=_split_csv(args.trade_dates),
        report_dates=_split_csv(args.report_dates),
        industry_names=_split_csv(args.industry_names),
        concept_names=_split_csv(args.concept_names),
        selected_api_names=_split_csv(args.api_names),
        universe_root=args.universe_root,
        start_date=args.start_date,
        end_date=args.end_date,
        max_workers=args.max_workers,
        request_sleep=args.request_sleep,
        continue_on_error=args.continue_on_error,
        include_disabled=args.include_disabled,
        resume=args.resume,
        ak_module=ak,
    )


def _build_child_cmd(args: argparse.Namespace, batch_output_root: Path, batch_symbols: list[str]) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "qsys.utils.run_factor_lake_raw_ingest",
        "--output-root",
        str(batch_output_root),
        "--families",
        args.families,
        "--start-date",
        args.start_date,
        "--end-date",
        args.end_date,
        "--max-workers",
        str(args.max_workers),
        "--request-sleep",
        str(args.request_sleep),
        "--symbols",
        ",".join(batch_symbols),
        "--index-symbols",
        args.index_symbols,
        "--trade-dates",
        args.trade_dates,
        "--report-dates",
        args.report_dates,
        "--industry-names",
        args.industry_names,
        "--concept-names",
        args.concept_names,
        "--api-names",
        args.api_names,
        "--universe-root",
        args.universe_root,
        "--disable-symbol-batching",
    ]
    if args.continue_on_error:
        cmd.append("--continue-on-error")
    if args.include_disabled:
        cmd.append("--include-disabled")
    if args.resume:
        cmd.append("--resume")
    return cmd


def _run_with_symbol_batching(args: argparse.Namespace) -> dict:
    output_root = Path(args.output_root)
    op_review = output_root / "_operation_review"
    batch_logs_dir = op_review / "batch_logs"
    batches_root = output_root / "_batches"
    output_root.mkdir(parents=True, exist_ok=True)
    op_review.mkdir(parents=True, exist_ok=True)
    batch_logs_dir.mkdir(parents=True, exist_ok=True)
    batches_root.mkdir(parents=True, exist_ok=True)

    symbols = _parse_symbols(args.symbols)
    batches = _build_symbol_batches(symbols, int(args.symbol_batch_size))
    manifest = {
        "symbol_batch_size": int(args.symbol_batch_size),
        "batch_timeout_sec": args.batch_timeout_sec,
        "n_symbols": len(symbols),
        "symbols_head": symbols[:10],
        "symbols_tail": symbols[-10:] if len(symbols) > 10 else symbols,
        "symbols_arg_length": len(args.symbols or ""),
        "n_batches": len(batches),
        "batches": [
            {"batch_label": _batch_label(start, end), "start_index": start, "end_index": end, "n_symbols": len(chunk)}
            for start, end, chunk in batches
        ],
    }
    (op_review / "chunked_run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    merged_catalog_rows: list[pd.DataFrame] = []
    report_rows: list[dict[str, object]] = []
    conflicts: list[dict[str, str]] = []

    stop = False
    for start, end, chunk in batches:
        if stop:
            break
        label = _batch_label(start, end)
        batch_output_root = batches_root / label
        batch_output_root.mkdir(parents=True, exist_ok=True)
        cmd = _build_child_cmd(args, batch_output_root, chunk)
        log_path = batch_logs_dir / f"{label}.log"
        batch_status = "failed"
        return_code = -1
        merged_to_master = False
        merged_files = 0
        conflict_files = 0
        timed_out = False
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"batch_label={label}\n")
            lf.write(f"n_symbols={len(chunk)}\n")
            lf.write(f"symbols_head={chunk[:10]}\n")
            lf.write(f"symbols_tail={chunk[-10:] if len(chunk) > 10 else chunk}\n")
            lf.write(f"symbols_arg_length={len(','.join(chunk))}\n")
            lf.write("cmd=" + " ".join(cmd) + "\n")
            try:
                proc = subprocess.run(cmd, stdout=lf, stderr=lf, text=True, timeout=args.batch_timeout_sec)
                return_code = int(proc.returncode)
                batch_status = "success" if proc.returncode == 0 else "failed"
            except subprocess.TimeoutExpired:
                batch_status = "timeout"
                return_code = -999
                timed_out = True

        if batch_status == "success":
            merged_files, conflict_files = _merge_batch_raw(batch_output_root, output_root, label, conflicts)
            merged_to_master = True
            batch_catalog = batch_output_root / "raw_ingest_catalog.csv"
            if batch_catalog.exists():
                cdf = pd.read_csv(batch_catalog)
                cdf["batch_label"] = label
                cdf["batch_output_root"] = str(batch_output_root)
                cdf["output_path"] = cdf["output_path"].apply(lambda x: _rewrite_to_master_path(str(x), batch_output_root, output_root))
                cdf["metadata_path"] = cdf["metadata_path"].apply(lambda x: _rewrite_to_master_path(str(x), batch_output_root, output_root))
                merged_catalog_rows.append(cdf)
            if not args.keep_batch_outputs:
                raw_dir = batch_output_root / "data" / "raw"
                if raw_dir.exists():
                    shutil.rmtree(raw_dir)

        report_rows.append({
            "batch_label": label,
            "start_index": start,
            "end_index": end,
            "n_symbols": len(chunk),
            "batch_output_root": str(batch_output_root),
            "batch_status": batch_status,
            "return_code": return_code,
            "timed_out": timed_out,
            "merged_to_master": merged_to_master,
            "merged_files": merged_files,
            "conflict_files": conflict_files,
            "log_path": str(log_path),
        })

        if timed_out and args.stop_on_batch_timeout:
            stop = True

    if conflicts:
        pd.DataFrame(conflicts).to_csv(op_review / "merge_conflicts.csv", index=False)

    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(op_review / "batch_run_report.csv", index=False)

    if merged_catalog_rows:
        final_catalog = pd.concat(merged_catalog_rows, ignore_index=True)
    else:
        final_catalog = pd.DataFrame()
    final_catalog_path = output_root / "raw_ingest_catalog.csv"
    final_catalog.to_csv(final_catalog_path, index=False)

    if not final_catalog.empty and "status" in final_catalog.columns:
        final_summary = final_catalog.groupby(["source_family", "api_name", "status"], dropna=False).size().reset_index(name="count")
    else:
        final_summary = pd.DataFrame(columns=["source_family", "api_name", "status", "count"])
    final_summary_path = output_root / "raw_ingest_summary.csv"
    final_summary.to_csv(final_summary_path, index=False)

    return {
        "output_root": str(output_root),
        "catalog_path": str(final_catalog_path),
        "summary_path": str(final_summary_path),
        "rows": int(len(final_catalog)),
        "batch_report_path": str(op_review / "batch_run_report.csv"),
        "manifest_path": str(op_review / "chunked_run_manifest.json"),
        "merge_conflicts_path": str(op_review / "merge_conflicts.csv"),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Official Stage-1 dataset-centered Raw Data Lake ingest runner")
    p.add_argument("--output-root", default="outputs/factor_lake_raw")
    p.add_argument("--families", default="market_price,index_market,margin_leverage,financial_fundamental,industry_concept,event_ownership,corporate_action,disclosure_ir,trading_attention")
    p.add_argument("--start-date", default="20100101")
    p.add_argument("--end-date", default="20101231")
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--request-sleep", type=float, default=0.0)
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument("--include-disabled", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--symbols", default="")
    p.add_argument("--index-symbols", default="")
    p.add_argument("--trade-dates", default="")
    p.add_argument("--report-dates", default="")
    p.add_argument("--industry-names", default="")
    p.add_argument("--concept-names", default="")
    p.add_argument("--api-names", default="")
    p.add_argument("--universe-root", default="config/factor_sources/acquisition_universe")
    p.add_argument("--symbol-batch-size", type=int, default=0)
    p.add_argument("--batch-timeout-sec", type=float, default=None)
    p.add_argument("--stop-on-batch-timeout", action="store_true")
    p.add_argument("--keep-batch-outputs", action="store_true")
    p.add_argument("--disable-symbol-batching", action="store_true")
    args = p.parse_args()

    should_batch = (not args.disable_symbol_batching) and int(args.symbol_batch_size or 0) > 0
    out = _run_with_symbol_batching(args) if should_batch else _run_without_batching(args)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
