from __future__ import annotations

import inspect
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .io import ensure_layout, write_catalog, write_inventory, write_manifest
from .profiling import safe_filename, summarize_dataframe
from .registry import FACTOR_SOURCE_REGISTRY, filter_source_cases
from .schemas import SourceRunResult


def _write_dataframe_output(df: pd.DataFrame, base_path: Path) -> tuple[str, str, str]:
    parquet_path = base_path.with_suffix(".parquet")
    try:
        df.to_parquet(parquet_path, index=False)
        return str(parquet_path), "parquet", ""
    except Exception as exc:  # noqa: BLE001
        csv_path = base_path.with_suffix(".csv")
        df.astype(str).to_csv(csv_path, index=False, encoding="utf-8-sig")
        return str(csv_path), "csv", f"parquet_write_failed:{type(exc).__name__}:{exc}"


def _filter_kwargs(func: Any, kwargs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    sig = inspect.signature(func)
    accepted = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in accepted}
    ignored = {k: v for k, v in kwargs.items() if k not in accepted}
    return filtered, ignored


def run_probe(ak_module: Any, output_root: str = "outputs/factor_lake_probe", family: str | None = None, api_name: str | None = None, case_id: str | None = None, enabled_only: bool = False, max_cases: int | None = None, timeout_seconds: float = 30.0, request_sleep: float = 0.0, run_name: str | None = None) -> dict[str, Any]:
    paths = ensure_layout(output_root)
    selected = filter_source_cases(FACTOR_SOURCE_REGISTRY, family, api_name, case_id, enabled_only, max_cases)
    run_id = run_name or f"factor_probe_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    write_inventory(selected, paths["catalogs"] / "source_case_inventory.csv")

    results: list[SourceRunResult] = []
    for source_case in selected:
        started = datetime.now(UTC)
        status = "skipped" if not source_case.enabled else "missing"
        filtered_kwargs: dict[str, Any] = {}
        ignored_kwargs: dict[str, Any] = {}
        out_path = ""
        meta_path = ""
        output_format = ""
        write_warning = ""
        err_t = ""
        err_m = ""
        skipped_reason = "disabled" if not source_case.enabled else ""
        rows = n_cols = 0
        columns = date_like_columns = symbol_like_columns = announcement_like_columns = []
        has_date_like = has_symbol_like = has_announcement_like = False
        try:
            if source_case.enabled:
                fn = getattr(ak_module, source_case.api_name, None)
                if fn is None:
                    status = "missing"
                else:
                    filtered_kwargs, ignored_kwargs = _filter_kwargs(fn, source_case.kwargs)
                    with ThreadPoolExecutor(max_workers=1) as ex:
                        fut = ex.submit(fn, **filtered_kwargs)
                        result = fut.result(timeout=timeout_seconds)
                    if isinstance(result, pd.DataFrame):
                        summary = summarize_dataframe(result)
                        rows = summary["rows"]
                        n_cols = summary["n_cols"]
                        columns = summary["columns"]
                        date_like_columns = summary["date_like_columns"]
                        symbol_like_columns = summary["symbol_like_columns"]
                        announcement_like_columns = summary["announcement_like_columns"]
                        has_date_like = summary["has_date_like_column"]
                        has_symbol_like = summary["has_symbol_like_column"]
                        has_announcement_like = summary["has_announcement_like_column"]
                        status = "success" if rows > 0 else "empty"
                        base = Path(paths["samples"]) / safe_filename(source_case.case_id)
                        out_path, output_format, write_warning = _write_dataframe_output(result, base)
                        meta = {"source_case": asdict(source_case), "summary": summary, "output_format": output_format, "write_warning": write_warning}
                        mp = Path(paths["metadata"]) / safe_filename(f"{source_case.case_id}.json")
                        mp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                        meta_path = str(mp)
                    else:
                        status = "non_dataframe"
        except TimeoutError:
            status = "timeout"
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            err_t = type(exc).__name__
            err_m = str(exc)

        ended = datetime.now(UTC)
        results.append(SourceRunResult(
            run_id=run_id,
            case_id=source_case.case_id,
            source_family=source_case.source_family,
            api_name=source_case.api_name,
            enabled=source_case.enabled,
            status=status,
            rows=rows,
            n_cols=n_cols,
            columns=columns,
            date_like_columns=date_like_columns,
            symbol_like_columns=symbol_like_columns,
            announcement_like_columns=announcement_like_columns,
            has_date_like_column=has_date_like,
            has_symbol_like_column=has_symbol_like,
            has_announcement_like_column=has_announcement_like,
            kwargs_json=json.dumps(source_case.kwargs, ensure_ascii=False),
            filtered_kwargs_json=json.dumps(filtered_kwargs, ensure_ascii=False),
            ignored_kwargs_json=json.dumps(ignored_kwargs, ensure_ascii=False),
            output_path=out_path,
            output_format=output_format,
            metadata_path=meta_path,
            write_warning=write_warning,
            error_type=err_t,
            error_message=err_m,
            skipped_reason=skipped_reason,
            timeout_seconds=timeout_seconds,
            elapsed_seconds=(ended - started).total_seconds(),
            started_at=started.isoformat(),
            ended_at=ended.isoformat(),
        ))
        if request_sleep > 0:
            time.sleep(request_sleep)

    catalog_df = write_catalog(results, paths["catalogs"] / "api_call_catalog.csv")
    for s, fname in [("failed", "failed_cases.csv"), ("empty", "empty_cases.csv"), ("missing", "missing_cases.csv"), ("timeout", "timeout_cases.csv")]:
        catalog_df[catalog_df["status"] == s].to_csv(paths["catalogs"] / fname, index=False, encoding="utf-8-sig")

    if len(selected) != len(catalog_df):
        raise RuntimeError("selected SourceCase count must equal api_call_catalog row count")

    manifest = {
        "run_id": run_id,
        "selected_cases": len(selected),
        "catalog_rows": int(len(catalog_df)),
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "output_root": str(paths["root"]),
    }
    write_manifest(manifest, paths["manifests"] / "run_manifest.json")
    return manifest
