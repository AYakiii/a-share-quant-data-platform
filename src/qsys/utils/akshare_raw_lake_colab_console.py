from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from qsys.data.factor_lake.akshare_raw_ingest import format_api_inflight_limits_compact

try:  # pragma: no cover - exercised in notebooks, monkeypatched in tests.
    from IPython.display import clear_output, display
except ImportError:  # pragma: no cover
    def clear_output(wait: bool = False) -> None:  # noqa: ARG001
        return None

    def display(value: object) -> None:
        print(value)


REQUIRED_NOTEBOOK_VARIABLES = (
    "PROJECT_ROOT",
    "SYMBOLS_FILE",
    "OUTPUT_ROOT",
    "START_DATE",
    "END_DATE",
    "REPORT_DATES",
    "LANES",
    "MAX_WORKERS",
    "HEAVY_MAX_WORKERS",
    "LONG_RUN_MAX_WORKERS",
    "DEFERRED_MAX_WORKERS",
    "HEARTBEAT_SEC",
    "SYMBOL_BATCH_SIZE",
    "MAX_INFLIGHT_TASKS",
    "API_INFLIGHT_LIMITS",
    "TASK_TIMEOUT_SEC",
    "MANUAL_SELECTED_TASK_TIMEOUT_SEC",
    "HEAVY_TASK_TIMEOUT_SEC",
    "LONG_RUN_TASK_TIMEOUT_SEC",
    "DEFERRED_TASK_TIMEOUT_SEC",
    "REQUEST_SLEEP",
    "HEAVY_REQUEST_SLEEP",
    "LONG_RUN_REQUEST_SLEEP",
    "TASK_RETRY_ATTEMPTS",
    "TASK_RETRY_SLEEP_SEC",
    "TASK_RETRY_BACKOFF",
    "TASK_RETRY_JITTER_SEC",
    "ONLY_FAMILIES",
    "EXCLUDE_FAMILIES",
    "ONLY_APIS",
    "EXCLUDE_APIS",
    "RESUME",
    "RESET_OUTPUT",
    "REFRESH_UNIVERSE",
    "DRY_RUN",
)


@dataclass(frozen=True)
class RawLakeConsoleConfig:
    """Validated configuration extracted from existing Raw Lake Colab globals."""

    project_root: Path
    symbols_file: Path
    output_root: Path
    start_date: Any
    end_date: Any
    report_dates: Any
    lanes: Any
    max_workers: Any
    heavy_max_workers: Any
    long_run_max_workers: Any
    deferred_max_workers: Any
    heartbeat_sec: Any
    symbol_batch_size: Any
    max_inflight_tasks: Any
    api_inflight_limits: Any
    task_timeout_sec: Any
    manual_selected_task_timeout_sec: Any
    heavy_task_timeout_sec: Any
    long_run_task_timeout_sec: Any
    deferred_task_timeout_sec: Any
    request_sleep: Any
    heavy_request_sleep: Any
    long_run_request_sleep: Any
    task_retry_attempts: Any
    task_retry_sleep_sec: Any
    task_retry_backoff: Any
    task_retry_jitter_sec: Any
    only_families: Any
    exclude_families: Any
    only_apis: Any
    exclude_apis: Any
    resume: bool
    reset_output: bool
    refresh_universe: bool
    dry_run: bool

    @classmethod
    def from_namespace(cls, namespace: Mapping[str, Any]) -> "RawLakeConsoleConfig":
        """Build config from the existing notebook globals without inventing defaults."""
        missing = [name for name in REQUIRED_NOTEBOOK_VARIABLES if name not in namespace]
        if missing:
            raise KeyError(f"missing required Raw Lake notebook variable(s): {', '.join(missing)}")
        output_root = Path(namespace["OUTPUT_ROOT"])
        if _is_drive_like_path(output_root):
            raise ValueError(f"OUTPUT_ROOT must be local staging, not Drive-like: {output_root}")
        resume = bool(namespace["RESUME"])
        reset_output = bool(namespace["RESET_OUTPUT"])
        if resume and reset_output:
            raise ValueError("RESUME and RESET_OUTPUT cannot both be True")
        return cls(
            project_root=Path(namespace["PROJECT_ROOT"]),
            symbols_file=Path(namespace["SYMBOLS_FILE"]),
            output_root=output_root,
            start_date=namespace["START_DATE"],
            end_date=namespace["END_DATE"],
            report_dates=namespace["REPORT_DATES"],
            lanes=namespace["LANES"],
            max_workers=namespace["MAX_WORKERS"],
            heavy_max_workers=namespace["HEAVY_MAX_WORKERS"],
            long_run_max_workers=namespace["LONG_RUN_MAX_WORKERS"],
            deferred_max_workers=namespace["DEFERRED_MAX_WORKERS"],
            heartbeat_sec=namespace["HEARTBEAT_SEC"],
            symbol_batch_size=namespace["SYMBOL_BATCH_SIZE"],
            max_inflight_tasks=namespace["MAX_INFLIGHT_TASKS"],
            api_inflight_limits=namespace["API_INFLIGHT_LIMITS"],
            task_timeout_sec=namespace["TASK_TIMEOUT_SEC"],
            manual_selected_task_timeout_sec=namespace["MANUAL_SELECTED_TASK_TIMEOUT_SEC"],
            heavy_task_timeout_sec=namespace["HEAVY_TASK_TIMEOUT_SEC"],
            long_run_task_timeout_sec=namespace["LONG_RUN_TASK_TIMEOUT_SEC"],
            deferred_task_timeout_sec=namespace["DEFERRED_TASK_TIMEOUT_SEC"],
            request_sleep=namespace["REQUEST_SLEEP"],
            heavy_request_sleep=namespace["HEAVY_REQUEST_SLEEP"],
            long_run_request_sleep=namespace["LONG_RUN_REQUEST_SLEEP"],
            task_retry_attempts=namespace["TASK_RETRY_ATTEMPTS"],
            task_retry_sleep_sec=namespace["TASK_RETRY_SLEEP_SEC"],
            task_retry_backoff=namespace["TASK_RETRY_BACKOFF"],
            task_retry_jitter_sec=namespace["TASK_RETRY_JITTER_SEC"],
            only_families=namespace["ONLY_FAMILIES"],
            exclude_families=namespace["EXCLUDE_FAMILIES"],
            only_apis=namespace["ONLY_APIS"],
            exclude_apis=namespace["EXCLUDE_APIS"],
            resume=resume,
            reset_output=reset_output,
            refresh_universe=bool(namespace["REFRESH_UNIVERSE"]),
            dry_run=bool(namespace["DRY_RUN"]),
        )


def _is_drive_like_path(path: str | Path) -> bool:
    """Return True for Colab/Google Drive-like paths that should not be local staging."""
    text = str(path).replace("\\", "/").lower()
    drive_markers = ("/content/drive", "/gdrive", "/mydrive", "google drive", "/mnt/drive")
    return any(marker in text for marker in drive_markers)


def add_optional_arg(cmd: list[str], flag: str, value: Any) -> None:
    """Append CLI flag only when value is not empty, matching the reference cell."""
    if value is None:
        return
    text = str(value).strip()
    if text:
        cmd.extend([flag, text])


def build_preheat_command(config: RawLakeConsoleConfig) -> list[str]:
    """Build the Raw Lake preheat command used by the original Colab RUN cell."""
    cmd = [
        "/usr/bin/python3",
        "-u",
        "-m",
        "qsys.utils.run_akshare_raw_lake_preheat",
        "--symbols-file",
        str(config.symbols_file),
        "--output-root",
        str(config.output_root),
        "--start-date",
        str(config.start_date),
        "--end-date",
        str(config.end_date),
        "--report-dates",
        str(config.report_dates),
        "--lanes",
        str(config.lanes),
        "--max-workers",
        str(config.max_workers),
        "--heavy-max-workers",
        str(config.heavy_max_workers),
        "--long-run-max-workers",
        str(config.long_run_max_workers),
        "--deferred-max-workers",
        str(config.deferred_max_workers),
        "--heartbeat-sec",
        str(config.heartbeat_sec),
        "--symbol-batch-size",
        str(config.symbol_batch_size),
        "--max-inflight-tasks",
        str(config.max_inflight_tasks),
        "--api-inflight-limits",
        format_api_inflight_limits_compact(config.api_inflight_limits),
        "--task-timeout-sec",
        str(config.task_timeout_sec),
        "--manual-selected-task-timeout-sec",
        str(config.manual_selected_task_timeout_sec),
        "--heavy-task-timeout-sec",
        str(config.heavy_task_timeout_sec),
        "--long-run-task-timeout-sec",
        str(config.long_run_task_timeout_sec),
        "--deferred-task-timeout-sec",
        str(config.deferred_task_timeout_sec),
        "--request-sleep",
        str(config.request_sleep),
        "--heavy-request-sleep",
        str(config.heavy_request_sleep),
        "--long-run-request-sleep",
        str(config.long_run_request_sleep),
        "--task-retry-attempts",
        str(config.task_retry_attempts),
        "--task-retry-sleep-sec",
        str(config.task_retry_sleep_sec),
        "--task-retry-backoff",
        str(config.task_retry_backoff),
        "--task-retry-jitter-sec",
        str(config.task_retry_jitter_sec),
    ]
    add_optional_arg(cmd, "--only-families", config.only_families)
    add_optional_arg(cmd, "--exclude-families", config.exclude_families)
    add_optional_arg(cmd, "--only-apis", config.only_apis)
    add_optional_arg(cmd, "--exclude-apis", config.exclude_apis)
    if config.resume:
        cmd.append("--resume")
    if config.refresh_universe:
        cmd.append("--refresh-universe")
    if config.dry_run:
        cmd.append("--dry-run")
    return cmd


def read_json_safe(path: Path) -> dict[str, Any] | None:
    """Read JSON without crashing dashboard refresh."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - dashboard must tolerate partial writes.
        return None
    return payload if isinstance(payload, dict) else None


def tail_text(path: Path, n_lines: int = 120) -> str:
    """Read the tail of a local text log."""
    if not path.exists():
        return "(log file not found)"
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    return "\n".join(lines[-n_lines:])


def format_duration(seconds: Any) -> str:
    """Display seconds as human-readable duration, matching the reference cell."""
    if seconds is None:
        return "-"
    try:
        seconds = int(float(seconds))
    except Exception:  # noqa: BLE001
        return "-"
    if seconds < 0:
        return "-"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def progress_bar(completed: Any, total: Any, width: int = 46) -> str:
    """Draw one compact total-progress bar."""
    try:
        completed = int(completed)
        total = int(total)
    except Exception:  # noqa: BLE001
        completed = 0
        total = 0
    if total <= 0:
        ratio = 0.0
    else:
        ratio = min(max(completed / total, 0.0), 1.0)
    filled = int(round(ratio * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {ratio * 100:6.2f}%"


def render_dashboard(payload: Mapping[str, Any] | None, process_state: str, run_log_path: Path) -> None:
    """Render one compact dashboard screen only."""
    clear_output(wait=True)
    print("=== RAW LAKE PREHEAT DASHBOARD ===")
    print()
    if not payload:
        print(f"state:    {process_state}")
        print("stage:    preflight / waiting for first heartbeat")
        print()
        print("console log:")
        print(" ", run_log_path)
        return

    completed = payload.get("completed_tasks", 0)
    total = payload.get("total_tasks", 0)
    try:
        default_remaining = max(int(total or 0) - int(completed or 0), 0)
    except Exception:  # noqa: BLE001
        default_remaining = 0
    remaining = payload.get("pending_or_running_tasks", default_remaining)

    print("state:      ", process_state)
    print("lane:       ", payload.get("lane", "-"))
    print("event:      ", payload.get("event", "-"))
    print()
    print(progress_bar(completed, total))
    print()
    print("completed:  ", f"{completed} / {total}")
    print("remaining:  ", remaining)
    print()
    print("success:         ", payload.get("success_tasks", 0))
    print("empty:           ", payload.get("empty_tasks", 0))
    print("failed:          ", payload.get("failed_tasks", 0))
    print("timeout:         ", payload.get("timeout_tasks", 0))
    print("already_exists:  ", payload.get("already_exists_tasks", 0))
    print("skipped:         ", payload.get("skipped_tasks", 0))
    print("pending_adapter: ", payload.get("pending_adapter_tasks", 0))
    print()
    print("batch:      ", f"{payload.get('current_batch_id', '-')} / {payload.get('total_batches', '-')}")
    print("scope:      ", payload.get("current_batch_scope", "-"))
    print("batch task: ", f"{payload.get('current_batch_completed_tasks', 0)} / {payload.get('current_batch_task_count', 0)}")
    print("batches completed:", payload.get("completed_batches", 0))
    print()
    print("elapsed:    ", format_duration(payload.get("elapsed_sec")))
    print("throughput: ", f"{payload.get('throughput_tasks_per_min', 0)} tasks/min")
    print("eta:        ", format_duration(payload.get("eta_sec")))
    print()
    print("console log:")
    print(" ", run_log_path)


def run_preheat_with_compact_dashboard(
    config: RawLakeConsoleConfig,
    *,
    refresh_sec: float = 5,
    pre_run_pause_sec: float = 2,
) -> int:
    """Run Raw Lake preheat subprocess with the reference compact dashboard behavior."""
    op_root = config.output_root / "_operation_review"
    live_progress_path = op_root / "live_progress.json"
    run_log_path = op_root / "raw_lake_preheat_console.log"
    op_root.mkdir(parents=True, exist_ok=True)
    cmd = build_preheat_command(config)
    assert "--symbol-batch-size" in cmd
    assert "--max-inflight-tasks" in cmd
    assert "--api-inflight-limits" in cmd

    print("=== RAW LAKE PREHEAT COMMAND ===")
    print()
    print(" ".join(cmd))
    print()
    print("Hybrid Chunk controls:")
    print("  SYMBOL_BATCH_SIZE: ", config.symbol_batch_size)
    print("  MAX_INFLIGHT_TASKS:", config.max_inflight_tasks)
    print("  API_INFLIGHT_LIMITS:", format_api_inflight_limits_compact(config.api_inflight_limits) or "{}")
    print()
    print("OUTPUT_ROOT:")
    print(" ", config.output_root)
    print()
    if pre_run_pause_sec > 0:
        time.sleep(pre_run_pause_sec)

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    with run_log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            cmd,
            cwd=str(config.project_root),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        while process.poll() is None:
            render_dashboard(read_json_safe(live_progress_path), "RUNNING", run_log_path)
            time.sleep(refresh_sec)

    returncode = int(process.returncode or 0)
    render_dashboard(read_json_safe(live_progress_path), "FINISHED", run_log_path)
    print()
    print("=== PROCESS FINISHED ===")
    print("returncode:", returncode)
    if returncode != 0:
        print()
        print("=== LOG TAIL ===")
        print(tail_text(run_log_path, n_lines=160))
        raise subprocess.CalledProcessError(returncode, cmd)
    print()
    print("Run completed successfully.")
    print("Next: run Cell 5 acceptance audit.")
    return returncode


def _status_counts(catalog: pd.DataFrame) -> pd.DataFrame:
    if "status" not in catalog.columns:
        return pd.DataFrame(columns=["status", "tasks"])
    return catalog["status"].value_counts(dropna=False).rename_axis("status").reset_index(name="tasks")


def review_preheat_output(output_root: str | Path, *, lane_name: str = "main") -> dict[str, Any]:
    """Print a compact Raw Lake preheat acceptance review."""
    root = Path(output_root)
    op_root = root / "_operation_review"
    catalog_path = root / "raw_ingest_catalog.csv"
    recovery_path = op_root / "recovery_tasks.csv"
    observation_path = op_root / "source_concurrency_observation.csv"
    print("=== HYBRID CHUNK ACCEPTANCE REVIEW ===")
    print("ROOT:", root)
    if not catalog_path.exists():
        raise FileNotFoundError(catalog_path)
    catalog = pd.read_csv(
    catalog_path,
    dtype={
        "original_symbol": "string",
        "akshare_symbol": "string",
    },
)
    print("\n=== CATALOG ===")
    print("rows:", len(catalog))
    duplicate_count = 0
    if {"task_key_json", "partition_json"}.issubset(catalog.columns):
        duplicate_rows = catalog[catalog.duplicated(subset=["task_key_json", "partition_json"], keep=False)].copy()
        duplicate_count = len(duplicate_rows)
        print("duplicated logical partitions:", duplicate_count)
        if duplicate_count > 0:
            raise AssertionError("Catalog contains duplicated logical partitions.")
    print("\n=== STATUS COUNTS ===")
    status = _status_counts(catalog)
    display(status)

    lane_root = op_root / "hybrid_batches" / lane_name
    report_path = lane_root / "hybrid_batch_report.csv"
    checkpoint_path = lane_root / "hybrid_batch_checkpoint.json"
    completed_batches = 0
    total_batches = 0
    print(f"\n=== HYBRID BATCHES: {lane_name} ===")
    if report_path.exists():
        report = pd.read_csv(report_path)
        total_batches = len(report)
        completed_batches = int((report.get("status", pd.Series(dtype=str)) == "completed").sum())
        print("clean batches / total:", f"{completed_batches} / {total_batches}")
    else:
        print("report missing:", report_path)
    if checkpoint_path.exists():
        checkpoint = read_json_safe(checkpoint_path) or {}
        fingerprint_lane = (checkpoint.get("fingerprint_payload") or {}).get("lane_name")
        print("fingerprint lane:", fingerprint_lane)

    recovery_rows = 0
    print("\n=== RECOVERY QUEUE ===")
    if recovery_path.exists():
        recovery_rows = len(pd.read_csv(recovery_path))
    print("recovery rows:", recovery_rows)

    observation_grouped = pd.DataFrame()
    print("\n=== SOURCE CONCURRENCY OBSERVATION ===")
    if observation_path.exists():
        observation = pd.read_csv(observation_path)
        required = {"source_key", "workload_class"}
        if required.issubset(observation.columns):
            for col in ["peak_inflight_tasks", "latency_sample_count", "median_task_elapsed_sec", "p90_task_elapsed_sec"]:
                if col not in observation.columns:
                    observation[col] = 0
            observation_grouped = (
                observation.groupby(["source_key", "workload_class"], dropna=False)
                .agg(
                    peak_inflight_tasks=("peak_inflight_tasks", "max"),
                    latency_sample_count=("latency_sample_count", "sum"),
                    median_task_elapsed_sec=("median_task_elapsed_sec", "median"),
                    p90_task_elapsed_sec=("p90_task_elapsed_sec", "median"),
                )
                .reset_index()
                .sort_values(["source_key", "workload_class"])
            )
            display(observation_grouped)
        else:
            print("source observation columns missing")
    else:
        print("source observation missing:", observation_path)
    print("\n=== ACCEPTANCE REVIEW COMPLETE ===")
    return {
        "catalog_rows": len(catalog),
        "duplicated_logical_partitions": duplicate_count,
        "completed_batches": completed_batches,
        "total_batches": total_batches,
        "recovery_rows": recovery_rows,
        "source_concurrency_observation": observation_grouped,
    }


def audit_preheat_failures(output_root: str | Path, *, display_limit: int = 200) -> dict[str, Path]:
    """Summarize Raw Lake preheat failures and write audit CSV files only."""
    root = Path(output_root)
    op_root = root / "_operation_review"
    audit_root = op_root / "failure_audit"
    catalog_path = root / "raw_ingest_catalog.csv"
    task_events_path = op_root / "task_events.jsonl"
    audit_root.mkdir(parents=True, exist_ok=True)
    print("ROOT:")
    print(" ", root)
    print("\nCATALOG_PATH:")
    print(" ", catalog_path)
    print("\nTASK_EVENTS_PATH:")
    print(" ", task_events_path)
    if not catalog_path.exists():
        raise FileNotFoundError("raw_ingest_catalog.csv was not found. Run this audit after the acquisition lane has completed.")
    catalog = pd.read_csv(catalog_path, dtype={"original_symbol": str, "akshare_symbol": str})
    print("\n=== CATALOG LOADED ===")
    print("rows:", len(catalog))
    print("columns:", len(catalog.columns))
    required_columns = [
        "source_family",
        "api_name",
        "status",
        "rows",
        "error_type",
        "error_message",
        "original_symbol",
        "akshare_symbol",
        "params_json",
        "partition_json",
        "task_key_json",
        "elapsed_sec",
        "output_path",
    ]
    for col in required_columns:
        if col not in catalog.columns:
            catalog[col] = ""
    catalog["status"] = catalog["status"].fillna("").astype(str)
    catalog["error_type"] = catalog["error_type"].fillna("").astype(str)
    catalog["error_message"] = catalog["error_message"].fillna("").astype(str)
    catalog["rows_num"] = pd.to_numeric(catalog["rows"], errors="coerce").fillna(0)
    catalog["elapsed_sec_num"] = pd.to_numeric(catalog["elapsed_sec"], errors="coerce").fillna(0.0)

    status_summary = catalog["status"].value_counts(dropna=False).rename_axis("status").reset_index(name="tasks")
    print("\n=== OVERALL STATUS SUMMARY ===")
    display(status_summary)

    api_summary = (
        catalog.groupby(["source_family", "api_name"], dropna=False)
        .agg(
            total_tasks=("status", "size"),
            success_tasks=("status", lambda x: (x == "success").sum()),
            empty_tasks=("status", lambda x: (x == "empty").sum()),
            failed_tasks=("status", lambda x: (x == "failed").sum()),
            timeout_tasks=("status", lambda x: (x == "timeout").sum()),
            skipped_tasks=("status", lambda x: (x == "skipped").sum()),
            already_exists_tasks=("status", lambda x: (x == "already_exists").sum()),
            pending_adapter_tasks=("status", lambda x: (x == "pending_adapter").sum()),
            rows=("rows_num", "sum"),
            elapsed_sec=("elapsed_sec_num", "sum"),
        )
        .reset_index()
    )
    api_summary["success_rate"] = api_summary["success_tasks"] / api_summary["total_tasks"]
    api_summary["failure_rate"] = api_summary["failed_tasks"] / api_summary["total_tasks"]
    api_summary["empty_rate"] = api_summary["empty_tasks"] / api_summary["total_tasks"]
    api_summary["avg_elapsed_sec"] = api_summary["elapsed_sec"] / api_summary["total_tasks"]
    api_summary = api_summary.sort_values(["failed_tasks", "failure_rate", "total_tasks"], ascending=[False, False, False])
    api_summary_path = audit_root / "failure_summary_by_api.csv"
    api_summary.to_csv(api_summary_path, index=False, encoding="utf-8-sig")
    print("\n=== API SUMMARY ===")
    display(api_summary)
    print("\nsaved:")
    print(" ", api_summary_path)

    failure_statuses = {"failed", "timeout", "pending_adapter", "skipped"}
    failure_detail = catalog[catalog["status"].isin(failure_statuses)].copy()
    failure_detail = failure_detail[
        [
            "source_family",
            "api_name",
            "status",
            "original_symbol",
            "akshare_symbol",
            "params_json",
            "partition_json",
            "task_key_json",
            "error_type",
            "error_message",
            "elapsed_sec_num",
            "output_path",
        ]
    ].rename(columns={"elapsed_sec_num": "elapsed_sec"})
    failure_detail = failure_detail.sort_values(["source_family", "api_name", "status", "original_symbol"])
    failure_detail_path = audit_root / "failure_detail.csv"
    failure_detail.to_csv(failure_detail_path, index=False, encoding="utf-8-sig")
    print("\n=== FAILURE DETAIL ===")
    print("rows:", len(failure_detail))
    display(failure_detail.head(display_limit))
    print("\nsaved:")
    print(" ", failure_detail_path)

    error_summary = (
        failure_detail.groupby(["source_family", "api_name", "status", "error_type", "error_message"], dropna=False)
        .size()
        .reset_index(name="tasks")
        .sort_values(["tasks", "source_family", "api_name"], ascending=[False, True, True])
    )
    error_summary_path = audit_root / "failure_summary_by_error_type.csv"
    error_summary.to_csv(error_summary_path, index=False, encoding="utf-8-sig")
    print("\n=== ERROR TYPE SUMMARY ===")
    display(error_summary.head(300))
    print("\nsaved:")
    print(" ", error_summary_path)

    high_failure = api_summary[
        (api_summary["failed_tasks"] > 0) | (api_summary["timeout_tasks"] > 0) | (api_summary["pending_adapter_tasks"] > 0)
    ].copy()
    high_failure["review_priority"] = "P3"
    high_failure.loc[high_failure["failure_rate"] >= 0.80, "review_priority"] = "P0"
    high_failure.loc[(high_failure["failure_rate"] >= 0.30) & (high_failure["failure_rate"] < 0.80), "review_priority"] = "P1"
    high_failure.loc[(high_failure["failure_rate"] > 0) & (high_failure["failure_rate"] < 0.30), "review_priority"] = "P2"
    high_failure["priority_order"] = high_failure["review_priority"].map({"P0": 0, "P1": 1, "P2": 2, "P3": 3})
    high_failure = high_failure.sort_values(["priority_order", "failed_tasks", "failure_rate"], ascending=[True, False, False]).drop(columns=["priority_order"])
    shortlist_path = audit_root / "high_failure_api_shortlist.csv"
    high_failure.to_csv(shortlist_path, index=False, encoding="utf-8-sig")
    print("\n=== HIGH-FAILURE API SHORTLIST ===")
    display(high_failure)
    print("\nsaved:")
    print(" ", shortlist_path)

    print("\n=== AUDIT COMPLETE ===")
    paths = {
        "failure_summary_by_api": api_summary_path,
        "failure_summary_by_error_type": error_summary_path,
        "failure_detail": failure_detail_path,
        "high_failure_api_shortlist": shortlist_path,
    }
    for path in paths.values():
        print(" ", path)
    return paths


def inspect_local_output(output_root: str | Path) -> dict[str, Any]:
    """Inspect local Raw Lake output cache and review artifacts."""
    root = Path(output_root)
    op_root = root / "_operation_review"
    print("CACHE_ROOT:", root)
    exists = root.exists()
    parquet_count = 0
    disk_usage = "-"
    if exists:
        try:
            usage = shutil.disk_usage(root)
            disk_usage = f"free={usage.free} total={usage.total}"
        except Exception:  # noqa: BLE001
            disk_usage = "unavailable"
        parquet_count = len(list(root.rglob("*.parquet")))
        print("disk usage:", disk_usage)
        print("parquet files:", parquet_count)
    else:
        print("cache does not exist")
    review_paths = [
        op_root / "live_progress.json",
        op_root / "raw_lake_preheat_console.log",
        op_root / "preheat_plan_by_api.csv",
        op_root / "recovery_tasks.csv",
        op_root / "source_concurrency_observation.csv",
    ]
    print("review artifacts:")
    for path in review_paths:
        print(" ", path, "exists=" + str(path.exists()))
    return {"exists": exists, "parquet_files": parquet_count, "disk_usage": disk_usage, "review_artifacts": review_paths}


def clear_local_output(output_root: str | Path, *, confirm: bool) -> None:
    """Delete only explicit local output_root after an explicit confirmation."""
    root = Path(output_root)
    if not confirm:
        raise ValueError("clear_local_output requires confirm=True")
    if _is_drive_like_path(root):
        raise ValueError(f"refusing to clear Drive-like path: {root}")
    if str(root).strip() in {"", ".", "/"}:
        raise ValueError(f"refusing to clear unsafe output root: {root}")
    shutil.rmtree(root, ignore_errors=True)
    print("deleted:", root)
