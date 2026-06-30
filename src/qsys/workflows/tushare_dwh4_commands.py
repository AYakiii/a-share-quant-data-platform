"""Command builders for the DWH4 dual-entry Tushare workflow.

This module builds explicit command specifications only. It does not spawn
subprocesses, call Tushare, compact data, prepare Drive plans, promote, or
audit.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

PYTHONPATH_ENV_KEY = "PYTHONPATH"
COMPACT_PACKAGE_PARENT = Path("outputs") / "raw_acquisition_compact"


@dataclass(frozen=True)
class CommandSpec:
    """A token-free subprocess command plan."""

    stage: str
    cwd: Path
    argv: tuple[str, ...]
    env_updates: tuple[tuple[str, str], ...] = ()

    def env_update_mapping(self) -> dict[str, str]:
        """Return environment updates needed to execute the command."""
        return dict(self.env_updates)


def _base_env(task: Dwh4TushareTask) -> tuple[tuple[str, str], ...]:
    return ((PYTHONPATH_ENV_KEY, str(task.execution_repo / "src")),)


def _python_module_argv(python_executable: str, module: str, *args: str) -> tuple[str, ...]:
    return (python_executable, "-u", "-m", module, *args)


def default_package_root(task: Dwh4TushareTask) -> Path:
    """Return the raw_lake_compact_cli package root for a task promotion."""
    return COMPACT_PACKAGE_PARENT / task.promotion_name


def build_ingest_command(task: Dwh4TushareTask, *, python_executable: str = "python") -> CommandSpec:
    """Build the existing Tushare raw ingest command for a task."""
    args = [
        "--symbols-file",
        str(task.symbols_file),
        "--universe-name",
        task.universe_name,
        "--dataset-version",
        task.dataset_version,
        "--start-date",
        task.start_date,
        "--end-date",
        task.end_date,
        "--api-names",
        ",".join(task.api_names),
        "--output-root",
        str(task.output_root),
        "--max-workers",
        str(task.execution.max_workers),
        "--request-sleep",
        str(task.execution.request_sleep),
        "--request-jitter",
        str(task.execution.request_jitter),
        "--retry",
        str(task.execution.retry),
        "--heartbeat-sec",
        str(task.execution.heartbeat_sec),
    ]
    if task.expected_symbol_count is not None:
        args.extend(["--expected-symbol-count", str(task.expected_symbol_count)])
    if task.execution.resume:
        args.append("--resume")
    if task.allow_candidate_sources:
        args.append("--allow-candidate-sources")
    return CommandSpec(
        stage="ingest",
        cwd=task.execution_repo,
        argv=_python_module_argv(python_executable, "qsys.utils.run_tushare_raw_ingest", *args),
        env_updates=_base_env(task),
    )


def build_prepare_command(
    task: Dwh4TushareTask,
    *,
    python_executable: str = "python",
    replace_local_package: bool = True,
) -> CommandSpec:
    """Build the existing compact prepare command for a task."""
    args = [
        "prepare",
        "--provider",
        task.provider,
        "--dataset-version",
        task.dataset_version,
        "--output-root",
        str(task.output_root),
        "--drive-dwh-root",
        str(task.drive_dwh_root),
        "--promotion-name",
        task.promotion_name,
        "--start-date",
        task.start_date,
        "--end-date",
        task.end_date,
    ]
    if replace_local_package:
        args.append("--replace-local-package")
    return CommandSpec(
        stage="prepare",
        cwd=task.execution_repo,
        argv=_python_module_argv(python_executable, "qsys.utils.raw_lake_compact_cli", *args),
        env_updates=_base_env(task),
    )


def build_promote_command(
    task: Dwh4TushareTask,
    *,
    confirm_promotion: str,
    required_reviewed_bucket_kinds: tuple[str, ...] = (),
    package_root: str | Path | None = None,
    python_executable: str = "python",
) -> CommandSpec:
    """Build the human-gated compact promote command.

    The command is returned only when the supplied confirmation exactly matches
    the task promotion name and every required reviewed bucket kind is
    authorized by the task.
    """
    if confirm_promotion != task.promotion_name:
        raise ValueError("--confirm-promotion must exactly match task.promotion_name")
    required = tuple(dict.fromkeys(required_reviewed_bucket_kinds))
    authorized = set(task.promotion_policy.allow_reviewed_bucket_kinds)
    missing = [kind for kind in required if kind not in authorized]
    if missing:
        raise ValueError(f"reviewed bucket kinds not authorized by task: {','.join(missing)}")
    package = Path(package_root) if package_root is not None else default_package_root(task)
    args = [
        "promote",
        "--package-root",
        str(package),
        "--drive-dwh-root",
        str(task.drive_dwh_root),
        "--confirm-promotion",
        confirm_promotion,
    ]
    if required:
        ordered = [kind for kind in task.promotion_policy.allow_reviewed_bucket_kinds if kind in set(required)]
        args.extend(["--allow-reviewed-bucket-kinds", ",".join(ordered)])
    return CommandSpec(
        stage="promote",
        cwd=task.execution_repo,
        argv=_python_module_argv(python_executable, "qsys.utils.raw_lake_compact_cli", *args),
        env_updates=_base_env(task),
    )


def build_audit_command(task: Dwh4TushareTask, *, python_executable: str = "python") -> CommandSpec:
    """Build the read-only compact audit command for a task."""
    args = [
        "audit",
        "--promotion-name",
        task.promotion_name,
        "--drive-dwh-root",
        str(task.drive_dwh_root),
    ]
    return CommandSpec(
        stage="audit",
        cwd=task.execution_repo,
        argv=_python_module_argv(python_executable, "qsys.utils.raw_lake_compact_cli", *args),
        env_updates=_base_env(task),
    )


def build_run_to_prepare_commands(task: Dwh4TushareTask, *, python_executable: str = "python") -> tuple[CommandSpec, ...]:
    """Build external commands for run-to-prepare.

    In-process validation and review gates are intentionally not represented as
    subprocess commands here. The returned sequence must never include promote.
    """
    return (
        build_ingest_command(task, python_executable=python_executable),
        build_prepare_command(task, python_executable=python_executable),
    )


def run_artifact_dir(task: Dwh4TushareTask, run_id: str) -> Path:
    """Return the agent run artifact directory for a task/run id."""
    if not run_id or "/" in run_id or "\\" in run_id:
        raise ValueError("run_id must be a non-empty path-safe leaf")
    return task.ops_workspace / "runs" / task.workflow_name / run_id


def run_artifact_paths(task: Dwh4TushareTask, run_id: str) -> dict[str, Path]:
    """Return the standard agent artifact paths for a task/run id."""
    root = run_artifact_dir(task, run_id)
    return {
        "workflow_state": root / "workflow_state.json",
        "commands_executed": root / "commands_executed.jsonl",
        "gate_decisions": root / "gate_decisions.json",
        "planned_commands": root / "planned_commands.json",
        "console_log": root / "console.log",
        "agent_report": root / "dwh4_agent_report.md",
        "final_promotion_review": root / "final_promotion_review.md",
        "drive_inventory": root / "drive_inventory.csv",
        "drive_inventory_summary": root / "drive_inventory_summary.json",
        "incremental_plan": root / "incremental_plan.csv",
        "incremental_plan_summary": root / "incremental_plan_summary.json",
        "incremental_merge_report": root / "incremental_merge_report.csv",
        "incremental_merge_summary": root / "incremental_merge_summary.json",
        "candidate_active_manifest": root / "candidate_active_manifest.json",
        "stable_latest_report": root / "stable_latest_report.csv",
        "active_manifest_summary": root / "active_manifest_summary.json",
        "drive_delete_request": root / "DRIVE_DELETE_REQUEST.md",
        "drive_delete_plan": root / "drive_delete_plan.csv",
        "drive_delete_summary": root / "drive_delete_summary.json",
        "promotion_execution_state": root / "promotion_execution_state.json",
        "promotion_execution_report": root / "promotion_execution_report.md",
        "audit_execution_state": root / "audit_execution_state.json",
        "audit_execution_report": root / "audit_execution_report.md",
    }


def command_execution_record(
    command: CommandSpec,
    *,
    started_at: str,
    finished_at: str,
    return_code: int,
    token_present: bool,
) -> dict[str, object]:
    """Build a token-free commands_executed.jsonl record."""
    return {
        "stage": command.stage,
        "cwd": str(command.cwd),
        "argv": list(command.argv),
        "started_at": started_at,
        "finished_at": finished_at,
        "return_code": return_code,
        "token_present": token_present,
    }
