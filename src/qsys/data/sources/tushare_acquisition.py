"""Local-only Tushare Raw acquisition."""
from __future__ import annotations

import csv
import hashlib
import json
import random
import re
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from qsys.data.factor_lake.raw_compact import validate_path_segment
from qsys.data.sources.tushare_client import TushareClient, read_tushare_token
from qsys.data.sources.tushare_contracts import TushareRawIngestConfig, TushareSourceSpec
from qsys.data.sources.tushare_sources import TUSHARE_SOURCE_SPECS, source_specs_by_api

CANONICAL_SYMBOL_RE = re.compile(r"^\d{6}$")
TUSHARE_TS_CODE_RE = re.compile(r"^(\d{6})\.(SZ|SH|BJ)$")
DATE_RE = re.compile(r"^\d{8}$")
DRIVE_MARKERS = ("/content/gdrive", "MyDrive", "Google Drive")


class TushareQueryClient(Protocol):
    """Protocol for mockable Tushare clients."""

    def query(self, api_name: str, **params: Any) -> pd.DataFrame: ...


def file_sha256(path: str | Path) -> str:
    """Compute SHA-256 for the external universe file without logging contents."""
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_symbol_from_ts_code(ts_code: str) -> str:
    """Convert a provider-specific Tushare ts_code into a canonical six-digit symbol."""
    text = str(ts_code or "").strip().upper()
    match = TUSHARE_TS_CODE_RE.fullmatch(text)
    if not match:
        raise ValueError(f"illegal Tushare ts_code: {ts_code!r}")
    return match.group(1)


def load_symbols(path: str | Path) -> list[str]:
    """Load provider-neutral canonical six-digit symbols from an external Universe file."""
    rows = Path(path).read_text(encoding="utf-8-sig").splitlines()
    symbols: list[str] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows, start=1):
        first = row.split(",", 1)[0].strip()
        if idx == 1 and first.lower() in {"symbol", "canonical_symbol"}:
            continue
        if not first:
            raise ValueError(f"empty symbol at line {idx}")
        if not CANONICAL_SYMBOL_RE.fullmatch(first):
            raise ValueError(f"illegal canonical symbol at line {idx}: {first!r}")
        if first in seen:
            raise ValueError(f"duplicate canonical symbol in universe file: {first}")
        seen.add(first)
        symbols.append(first)
    if not symbols:
        raise ValueError("symbols_file must contain at least one symbol")
    return symbols


def staging_root(config: TushareRawIngestConfig) -> Path:
    """Return the local staging root for a Tushare raw run."""
    return config.output_root / "data" / "raw" / config.provider


def artifact_root(config: TushareRawIngestConfig) -> Path:
    """Return the local artifact root for token-free operation artifacts."""
    return config.output_root / "artifacts" / "tushare_raw_acquisition"


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    days: list[str] = []
    cur = start
    while cur <= end:
        days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


def _validate_config(config: TushareRawIngestConfig) -> tuple[str, list[str], list[str], list[str], list[TushareSourceSpec], list[str]]:
    if config.provider != "tushare":
        raise ValueError("Tushare raw ingest config provider must be 'tushare'")
    if not str(config.universe_name or "").strip():
        raise ValueError("universe_name is required")
    if config.expected_symbol_count is not None and int(config.expected_symbol_count) <= 0:
        raise ValueError("expected_symbol_count must be > 0")
    if not DATE_RE.fullmatch(config.start_date or "") or not DATE_RE.fullmatch(config.end_date or ""):
        raise ValueError("start_date and end_date must be YYYYMMDD")
    if config.start_date > config.end_date:
        raise ValueError("start_date must be <= end_date")
    dataset_version = validate_path_segment(config.dataset_version, label="dataset_version")
    output_text = str(config.output_root)
    if any(marker in output_text for marker in DRIVE_MARKERS):
        raise ValueError("output_root must be local-only and must not point to Google Drive")
    by_api = source_specs_by_api()
    requested_api_names = list(config.api_names)
    candidate_api_names = requested_api_names or list(by_api)
    unknown = sorted(set(candidate_api_names) - set(by_api))
    if unknown:
        raise ValueError(f"unknown Tushare api_names: {unknown}")
    requested_families = list(config.families)
    if requested_families:
        valid_families = {spec.source_family for spec in TUSHARE_SOURCE_SPECS}
        unknown_families = sorted(set(requested_families) - valid_families)
        if unknown_families:
            raise ValueError(f"unknown Tushare families: {unknown_families}")
    specs = [by_api[api] for api in candidate_api_names if not requested_families or by_api[api].source_family in requested_families]
    if not specs:
        raise ValueError("api_names/families selection produced no Tushare sources")
    actual_api_names = [spec.api_name for spec in specs]
    return dataset_version, requested_api_names, requested_families, actual_api_names, specs, _date_range(config.start_date, config.end_date)


def _partition_dir(config: TushareRawIngestConfig, spec: TushareSourceSpec, trade_date: str) -> Path:
    return staging_root(config) / spec.source_family / spec.api_name / f"{spec.partition_key}={trade_date}"


def build_manifest(config: TushareRawIngestConfig, *, symbols: list[str], requested_api_names: list[str], requested_families: list[str], specs: list[TushareSourceSpec], trade_dates: list[str]) -> dict[str, Any]:
    """Build the token-free acquisition manifest."""
    return {
        "provider": config.provider,
        "dataset_version": config.dataset_version,
        "universe_name": config.universe_name,
        "symbols_file": str(config.symbols_file),
        "universe_sha256": file_sha256(config.symbols_file),
        "symbol_row_count": len(symbols),
        "unique_symbol_count": len(set(symbols)),
        "symbol_count": len(symbols),
        "symbol_input_format": "canonical_symbol",
        "start_date": config.start_date,
        "end_date": config.end_date,
        "requested_api_names": requested_api_names,
        "requested_families": requested_families,
        "api_names": [spec.api_name for spec in specs],
        "families": sorted({spec.source_family for spec in specs}),
        "trade_dates": trade_dates,
        "output_root": str(config.output_root),
        "local_staging_root": str(staging_root(config)),
        "dry_run": config.dry_run,
        "resume": config.resume,
        "max_workers": config.max_workers,
        "request_sleep": config.request_sleep,
        "request_jitter": config.request_jitter,
        "retry": config.retry,
        "sources": [asdict(spec) for spec in specs],
        "planned_partitions": [str(_partition_dir(config, spec, d)) for spec in specs for d in trade_dates],
    }


def _validate_symbols(config: TushareRawIngestConfig) -> list[str]:
    symbols = load_symbols(config.symbols_file)
    if config.expected_symbol_count is not None and len(symbols) != int(config.expected_symbol_count):
        raise ValueError(f"expected_symbol_count mismatch: expected {config.expected_symbol_count}, got {len(symbols)}")
    return symbols


def run_tushare_raw_ingest_dry_run(config: TushareRawIngestConfig, *, require_token: bool = True) -> dict[str, Any]:
    """Validate inputs and return a token-free dry-run manifest without calling Tushare APIs."""
    cfg = TushareRawIngestConfig(**{**config.__dict__, "dry_run": True})
    _, requested_api_names, requested_families, _, specs, trade_dates = _validate_config(cfg)
    if require_token:
        read_tushare_token(allow_prompt=False)
    symbols = _validate_symbols(cfg)
    return build_manifest(cfg, symbols=symbols, requested_api_names=requested_api_names, requested_families=requested_families, specs=specs, trade_dates=trade_dates)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_json_atomic(path: Path, payload: Any) -> None:
    """Atomically write JSON for live operator polling."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for row in rows for k in row}) if rows else ["status"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


OPERATOR_STATUS_KEYS = (
    "ok",
    "empty",
    "already_exists",
    "request_failed",
    "incomplete_existing_partition",
    "already_exists_metadata_incomplete",
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _min_int(values: list[int]) -> int:
    return min(values) if values else 0


def _max_int(values: list[int]) -> int:
    return max(values) if values else 0


def _is_false(value: Any) -> bool:
    return str(value).strip().lower() in {"false", "0", "no"}


def _required_fields_by_api(manifest: dict[str, Any]) -> dict[str, set[str]]:
    fields_by_api: dict[str, set[str]] = {}
    for source in manifest.get("sources", []):
        if isinstance(source, dict):
            api_name = str(source.get("api_name", ""))
            fields = source.get("fields", [])
            if api_name and isinstance(fields, (list, tuple)):
                fields_by_api[api_name] = {str(field) for field in fields}
    return fields_by_api


def _required_field_missing_partitions(field_presence: list[dict[str, str]], manifest: dict[str, Any]) -> set[tuple[str, str]]:
    fields_by_api = _required_fields_by_api(manifest)
    missing: set[tuple[str, str]] = set()
    for row in field_presence:
        api_name = row.get("api_name", "")
        field = row.get("field", "")
        if field in fields_by_api.get(api_name, set()) and _is_false(row.get("present")):
            missing.add((api_name, row.get("partition_path") or row.get("trade_date", "")))
    return missing


def _write_operator_summaries(config: TushareRawIngestConfig, manifest: dict[str, Any], artifacts: Path) -> dict[str, Any]:
    """Write fixed-size token-free summaries for operator dashboards."""
    catalog = _read_csv_rows(artifacts / "raw_ingest_catalog.csv")
    coverage = _read_csv_rows(artifacts / "source_coverage_summary.csv")
    duplicates = _read_csv_rows(artifacts / "duplicate_key_summary.csv")
    field_presence = _read_csv_rows(artifacts / "field_presence_summary.csv")
    missing_required_fields = _required_field_missing_partitions(field_presence, manifest)
    api_names = list(manifest.get("api_names", []))
    status_counts = {key: 0 for key in OPERATOR_STATUS_KEYS}
    for row in catalog:
        status = row.get("status", "")
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1
    bad_statuses = {"request_failed", "incomplete_existing_partition", "already_exists_metadata_incomplete"}
    missing_data_files = sum(1 for row in catalog if row.get("data_path") and not Path(row["data_path"]).exists())
    missing_metadata_files = sum(1 for row in catalog if row.get("metadata_path") and not Path(row["metadata_path"]).exists())
    duplicate_partitions = sum(1 for row in duplicates if _to_int(row.get("duplicate_key_count"), 0) > 0)
    abnormal_counts = {
        "bad_status_partitions": sum(status_counts.get(status, 0) for status in bad_statuses),
        "failed_partitions": status_counts.get("request_failed", 0),
        "duplicate_partitions": duplicate_partitions,
        "missing_data_files": missing_data_files,
        "missing_metadata_files": missing_metadata_files,
        "required_contract_fields_missing": len(missing_required_fields),
    }
    rough_check = "PASS" if all(value == 0 for value in abnormal_counts.values()) else "FAIL"
    summary = {
        "provider": manifest.get("provider"),
        "dataset_version": manifest.get("dataset_version"),
        "start_date": manifest.get("start_date"),
        "end_date": manifest.get("end_date"),
        "api_names": api_names,
        "trade_date_count": len(manifest.get("trade_dates", [])),
        "planned_partitions": len(manifest.get("planned_partitions", [])),
        "status_counts": status_counts,
        "abnormal_counts": abnormal_counts,
        "rough_check": rough_check,
    }

    rows_by_api: list[dict[str, Any]] = []
    for api_name in api_names:
        api_catalog = [row for row in catalog if row.get("api_name") == api_name]
        api_coverage = [row for row in coverage if row.get("api_name") == api_name]
        api_duplicates = [row for row in duplicates if row.get("api_name") == api_name]
        api_required_missing = sum(1 for api, _partition in missing_required_fields if api == api_name)
        api_status = {key: sum(1 for row in api_catalog if row.get("status") == key) for key in OPERATOR_STATUS_KEYS}
        filtered = [_to_int(row.get("filtered_row_count")) for row in api_coverage]
        returned = [_to_int(row.get("return_row_count")) for row in api_coverage]
        symbols = [_to_int(row.get("post_filter_symbol_count")) for row in api_coverage]
        dup_counts = [_to_int(row.get("duplicate_key_count"), 0) for row in api_duplicates]
        api_missing_data = sum(1 for row in api_catalog if row.get("data_path") and not Path(row["data_path"]).exists())
        api_missing_meta = sum(1 for row in api_catalog if row.get("metadata_path") and not Path(row["metadata_path"]).exists())
        api_abnormal = sum(api_status.get(s, 0) for s in bad_statuses) + sum(1 for v in dup_counts if v > 0) + api_missing_data + api_missing_meta + api_required_missing
        rows_by_api.append({
            "api_name": api_name,
            "planned_partitions": len(manifest.get("trade_dates", [])),
            "status_ok": api_status["ok"],
            "status_empty": api_status["empty"],
            "status_already_exists": api_status["already_exists"],
            "status_request_failed": api_status["request_failed"],
            "status_incomplete_existing_partition": api_status["incomplete_existing_partition"],
            "status_already_exists_metadata_incomplete": api_status["already_exists_metadata_incomplete"],
            "total_return_rows": sum(returned),
            "total_filtered_rows": sum(filtered),
            "min_filtered_rows": _min_int(filtered),
            "max_filtered_rows": _max_int(filtered),
            "min_symbols": _min_int(symbols),
            "max_symbols": _max_int(symbols),
            "max_missing_symbols": _max_int([_to_int(row.get("universe_missing_count")) for row in api_coverage]),
            "max_duplicate_keys": _max_int(dup_counts),
            "data_files": sum(1 for row in api_catalog if row.get("data_path") and Path(row["data_path"]).exists()),
            "metadata_files": sum(1 for row in api_catalog if row.get("metadata_path") and Path(row["metadata_path"]).exists()),
            "missing_data_files": api_missing_data,
            "missing_metadata_files": api_missing_meta,
            "required_contract_fields_missing": api_required_missing,
            "rough_check": "PASS" if api_abnormal == 0 else "FAIL",
        })
    _write_json(artifacts / "operator_summary.json", summary)
    _write_csv(artifacts / "operator_summary_by_api.csv", rows_by_api)
    return summary

def _call_with_retry(client: TushareQueryClient, spec: TushareSourceSpec, trade_date: str, retry: int) -> tuple[pd.DataFrame, str | None]:
    last_error: str | None = None
    for attempt in range(max(0, retry) + 1):
        try:
            params: dict[str, Any] = {"trade_date": trade_date}
            if spec.fields:
                params["fields"] = ",".join(spec.fields)
            return client.query(spec.api_name, **params), None
        except Exception as exc:  # noqa: BLE001 - persist token-free failure summary
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt >= retry:
                break
    return pd.DataFrame(), last_error


def _progress_payload(config: TushareRawIngestConfig, started_at: str, start_ts: float, total: int, completed: int, status_counts: dict[str, int], current: dict[str, str] | None) -> dict[str, Any]:
    return {
        "provider": config.provider,
        "dataset_version": config.dataset_version,
        "started_at": started_at,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "elapsed_sec": round(time.monotonic() - start_ts, 1),
        "total_tasks": total,
        "completed_tasks": completed,
        "pending_or_running_tasks": max(0, total - completed),
        "current_task": current,
        "status_counts": status_counts,
        "artifacts_root": str(artifact_root(config)),
        "local_staging_root": str(staging_root(config)),
    }


def _metadata_complete(metadata: dict[str, Any]) -> bool:
    required = {"return_row_count", "filtered_row_count", "pre_filter_symbol_count", "post_filter_symbol_count", "universe_missing_count", "duplicate_key_count"}
    return required.issubset(metadata)


def _append_qa_from_metadata(
    *,
    base: dict[str, Any],
    spec: TushareSourceSpec,
    metadata: dict[str, Any],
    data_path: Path,
    coverage: list[dict[str, Any]],
    field_presence: list[dict[str, Any]],
    duplicates: list[dict[str, Any]],
    universe_rows: list[dict[str, Any]],
) -> None:
    """Backfill QA rows from an existing partition metadata file."""
    coverage.append(metadata)
    duplicates.append({**base, "duplicate_key_count": metadata.get("duplicate_key_count")})
    universe_rows.append({
        **base,
        "pre_filter_symbol_count": metadata.get("pre_filter_symbol_count"),
        "post_filter_symbol_count": metadata.get("post_filter_symbol_count"),
        "universe_missing_count": metadata.get("universe_missing_count"),
    })
    dtypes = metadata.get("dtypes") if isinstance(metadata.get("dtypes"), dict) else {}
    if not dtypes and data_path.exists():
        dtypes = {c: str(t) for c, t in pd.read_parquet(data_path).dtypes.items()}
    for column in sorted(set(dtypes) | set(spec.fields) | {"canonical_symbol"}):
        field_presence.append({**base, "field": column, "present": column in dtypes, "null_count": None, "dtype": dtypes.get(column)})


def run_tushare_raw_ingest(config: TushareRawIngestConfig, *, client: TushareQueryClient | None = None) -> dict[str, Any]:
    """Run local-only Tushare raw acquisition and write staging plus QA artifacts."""
    _, requested_api_names, requested_families, _, specs, trade_dates = _validate_config(config)
    symbols = _validate_symbols(config)
    universe = set(symbols)
    manifest = build_manifest(config, symbols=symbols, requested_api_names=requested_api_names, requested_families=requested_families, specs=specs, trade_dates=trade_dates)
    artifacts = artifact_root(config)
    _write_json(artifacts / "tushare_acquisition_manifest.json", manifest)
    if config.dry_run:
        _write_operator_summaries(config, manifest, artifacts)
        return manifest
    client = client or TushareClient()
    catalog: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    field_presence: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    universe_rows: list[dict[str, Any]] = []
    events_path = artifacts / "operation_events.jsonl"
    live_path = artifacts / "live_progress.json"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    total_tasks = len(specs) * len(trade_dates)
    completed = 0
    status_counts = {"ok": 0, "empty": 0, "request_failed": 0, "already_exists": 0, "incomplete_existing_partition": 0, "already_exists_metadata_incomplete": 0}
    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    start_ts = time.monotonic()
    heartbeat_sec = config.heartbeat_sec if config.heartbeat_sec and config.heartbeat_sec > 0 else None
    next_heartbeat = start_ts if heartbeat_sec is not None else float("inf")

    def update_progress(current: dict[str, str] | None, *, force_stdout: bool = False) -> None:
        nonlocal next_heartbeat
        payload = _progress_payload(config, started_at, start_ts, total_tasks, completed, status_counts, current)
        _write_json_atomic(live_path, payload)
        now = time.monotonic()
        if heartbeat_sec is not None and (force_stdout or now >= next_heartbeat):
            current_text = "none" if current is None else f"{current['api_name']}:{current['trade_date']}"
            print(
                f"[heartbeat] elapsed_sec={payload['elapsed_sec']} total_tasks={total_tasks} "
                f"completed_tasks={completed} pending_or_running_tasks={payload['pending_or_running_tasks']} "
                f"status_counts={status_counts} current={current_text}",
                flush=True,
            )
            next_heartbeat = now + heartbeat_sec

    def emit(events: Any, payload: dict[str, Any]) -> None:
        events.write(json.dumps(payload, ensure_ascii=False) + "\n")
        events.flush()

    update_progress(None)
    with events_path.open("w", encoding="utf-8") as events:
        for spec in specs:
            for trade_date in trade_dates:
                current = {"api_name": spec.api_name, "trade_date": trade_date}
                update_progress(current)
                part = _partition_dir(config, spec, trade_date)
                data_path = part / "data.parquet"
                meta_path = part / "metadata.json"
                base = {"api_name": spec.api_name, "family": spec.source_family, "trade_date": trade_date, "partition_path": str(part)}
                emit(events, {**base, "event": "task_started"})
                if config.resume and data_path.exists() and meta_path.exists():
                    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
                    status = "already_exists" if _metadata_complete(metadata) else "already_exists_metadata_incomplete"
                    catalog.append({**base, "status": status, "data_path": str(data_path), "metadata_path": str(meta_path), "row_count": metadata.get("filtered_row_count")})
                    _append_qa_from_metadata(base=base, spec=spec, metadata=metadata, data_path=data_path, coverage=coverage, field_presence=field_presence, duplicates=duplicates, universe_rows=universe_rows)
                    status_counts[status] = status_counts.get(status, 0) + 1
                    completed += 1
                    emit(events, {**base, "event": status})
                    update_progress(current)
                    continue
                if data_path.exists() or meta_path.exists():
                    catalog.append({**base, "status": "incomplete_existing_partition"})
                    status_counts["incomplete_existing_partition"] += 1
                    completed += 1
                    emit(events, {**base, "event": "incomplete_existing_partition"})
                    update_progress(current)
                    continue
                if config.request_sleep > 0:
                    time.sleep(config.request_sleep + random.uniform(0, max(0.0, config.request_jitter)))
                df, error = _call_with_retry(client, spec, trade_date, config.retry)
                if error:
                    catalog.append({**base, "status": "request_failed", "error": error})
                    status_counts["request_failed"] += 1
                    completed += 1
                    emit(events, {**base, "event": "request_failed", "error": error})
                    update_progress(current)
                    continue
                raw_rows = len(df)
                df = df.copy()
                if "ts_code" in df.columns:
                    df["canonical_symbol"] = df["ts_code"].map(canonical_symbol_from_ts_code)
                else:
                    df["canonical_symbol"] = pd.Series(dtype="object")
                if "trade_date" not in df.columns:
                    df["trade_date"] = trade_date
                allowed_columns = [c for c in (*spec.fields, "canonical_symbol") if c in df.columns]
                df = df.loc[:, allowed_columns]
                pre_symbols = int(df["canonical_symbol"].nunique()) if len(df) else 0
                filtered = df[df["canonical_symbol"].isin(universe)].copy()
                post_symbols = int(filtered["canonical_symbol"].nunique()) if len(filtered) else 0
                missing = len(universe - set(filtered["canonical_symbol"].dropna().astype(str)))
                duplicate_count = int(filtered.duplicated(list(spec.primary_key)).sum()) if set(spec.primary_key).issubset(filtered.columns) else -1
                part.mkdir(parents=True, exist_ok=False)
                filtered.to_parquet(data_path, index=False)
                status = "empty" if raw_rows == 0 else "ok"
                metadata = {**base, "status": status, "return_row_count": raw_rows, "filtered_row_count": len(filtered), "pre_filter_symbol_count": pre_symbols, "post_filter_symbol_count": post_symbols, "universe_missing_count": missing, "duplicate_key_count": duplicate_count, "dtypes": {c: str(t) for c, t in filtered.dtypes.items()}, "empty_result": raw_rows == 0}
                _write_json(meta_path, metadata)
                catalog.append({**base, "status": status, "data_path": str(data_path), "metadata_path": str(meta_path), "row_count": len(filtered)})
                coverage.append(metadata)
                duplicates.append({**base, "duplicate_key_count": duplicate_count})
                universe_rows.append({**base, "pre_filter_symbol_count": pre_symbols, "post_filter_symbol_count": post_symbols, "universe_missing_count": missing})
                for column in sorted(set(filtered.columns) | set(spec.fields) | {"canonical_symbol"}):
                    field_presence.append({**base, "field": column, "present": column in filtered.columns, "null_count": int(filtered[column].isna().sum()) if column in filtered.columns else None, "dtype": str(filtered[column].dtype) if column in filtered.columns else None})
                status_counts[status] += 1
                completed += 1
                emit(events, {**base, "event": "partition_written", "row_count": len(filtered)})
                update_progress(current)
    update_progress(None, force_stdout=False)
    _write_csv(artifacts / "raw_ingest_catalog.csv", catalog)
    _write_csv(artifacts / "source_coverage_summary.csv", coverage)
    _write_csv(artifacts / "field_presence_summary.csv", field_presence)
    _write_csv(artifacts / "duplicate_key_summary.csv", duplicates)
    _write_csv(artifacts / "universe_filter_summary.csv", universe_rows)
    _write_operator_summaries(config, manifest, artifacts)
    return manifest


def manifest_json(manifest: dict[str, Any]) -> str:
    """Serialize a token-free manifest for console output."""
    return json.dumps(manifest, ensure_ascii=False, indent=2)
