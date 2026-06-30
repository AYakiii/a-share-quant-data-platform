"""CLI for the DWH4 dual-entry Tushare agent route.

The default mode is plan-only. External command execution requires an explicit
run-to-prepare flag plus an exact run-id confirmation.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from qsys.workflows.tushare_dwh4_artifacts import (
    write_plan_only_artifacts,
    write_audit_execution_artifacts,
    write_promotion_execution_artifacts,
    write_run_to_prepare_execution_artifacts,
)
from qsys.workflows.tushare_dwh4_executor import CommandExecutionBlocked, execute_audit_with_runner, execute_promotion_with_review_gate, execute_run_to_prepare_with_ingest_gate, run_command_subprocess
from qsys.workflows.tushare_dwh4_orchestrator import (
    build_run_to_prepare_plan,
    collect_run_to_prepare_review_decisions,
)
from qsys.workflows.tushare_dwh4_task import load_dwh4_tushare_task


def build_parser() -> argparse.ArgumentParser:
    """Build the DWH4 agent parser."""
    parser = argparse.ArgumentParser(
        description=(
            "DWH4 Tushare agent route. Defaults to plan-only; run-to-prepare "
            "execution is explicit. DWH4.1 incremental tasks use the integrated v2 stage plan."
        )
    )
    parser.add_argument("--task", required=True, help="Path to DWH4 Tushare task JSON.")
    parser.add_argument("--stage", default="run-to-prepare", choices=["run-to-prepare"], help="Workflow stage to plan.")
    parser.add_argument("--run-id", required=True, help="Path-safe run id for artifact output.")
    parser.add_argument("--python-executable", default="python", help="Python executable to record in planned commands.")
    parser.add_argument(
        "--allow-missing-token-for-plan",
        action="store_true",
        help="Allow command planning even when TUSHARE_TOKEN is absent. This still does not execute commands.",
    )
    parser.add_argument(
        "--skip-review-artifact-scan",
        action="store_true",
        help="Plan-only: do not scan current ingest/compact/promotion artifacts. Execution still runs mandatory gates.",
    )
    parser.add_argument("--package-root", help="Optional compact package root used for review artifact scans.")
    parser.add_argument(
        "--execute-run-to-prepare",
        action="store_true",
        help="Execute the planned ingest and prepare commands. Requires --confirm-execute-run-to-prepare to equal --run-id.",
    )
    parser.add_argument(
        "--confirm-execute-run-to-prepare",
        help="Exact run-id confirmation required when --execute-run-to-prepare is set.",
    )
    parser.add_argument(
        "--execute-promotion",
        action="store_true",
        help="Execute compact promotion. Requires --confirm-promotion to exactly match task promotion_name.",
    )
    parser.add_argument(
        "--confirm-promotion",
        help="Exact promotion_name confirmation required when --execute-promotion is set.",
    )
    parser.add_argument(
        "--execute-audit",
        action="store_true",
        help="Execute the read-only compact audit command. Does not write Drive.",
    )
    return parser


def _print_summary(
    status: str,
    written: dict[str, Path],
    *,
    subprocess_executed: bool = False,
    drive_write_executed: bool = False,
) -> None:
    print(f"[dwh4-agent] status={status}")
    for name, path in written.items():
        print(f"[dwh4-agent] {name}={path}")
    print(f"[dwh4-agent] subprocess_executed={str(subprocess_executed).lower()}")
    print(f"[dwh4-agent] drive_write_executed={str(drive_write_executed).lower()}")


def _execution_args_error(args: argparse.Namespace) -> str | None:
    execution_modes = (args.execute_run_to_prepare, args.execute_promotion, args.execute_audit)
    if sum(1 for enabled in execution_modes if enabled) > 1:
        return "--execute-run-to-prepare, --execute-promotion, and --execute-audit cannot be combined"
    if args.execute_audit:
        return None
    if not args.execute_run_to_prepare:
        if args.execute_promotion and not args.confirm_promotion:
            return "--confirm-promotion is required when --execute-promotion is set"
        return None
    if args.allow_missing_token_for_plan:
        return "--allow-missing-token-for-plan cannot be combined with --execute-run-to-prepare"
    if args.confirm_execute_run_to_prepare != args.run_id:
        return "--confirm-execute-run-to-prepare must exactly equal --run-id when --execute-run-to-prepare is set"
    return None


def _run_to_prepare_exit_code(run_to_prepare_complete: bool) -> int:
    return 0 if run_to_prepare_complete else 1


def _promotion_execution_status(promotion_execution) -> str:
    if promotion_execution.result is None:
        return "PROMOTION_BLOCKED"
    if promotion_execution.result.return_code == 0:
        return "PROMOTION_EXECUTED"
    return "PROMOTION_FAILED"


def _audit_execution_status(audit_execution) -> str:
    if audit_execution.result.return_code == 0:
        return "AUDIT_EXECUTED"
    return "AUDIT_FAILED"


def main(argv: list[str] | None = None) -> int:
    """Run a DWH4 Tushare agent pass."""
    args = build_parser().parse_args(argv)
    error = _execution_args_error(args)
    if error:
        print(f"[dwh4-agent] ERROR {error}")
        return 2

    task = load_dwh4_tushare_task(args.task)
    if args.execute_promotion:
        if args.confirm_promotion != task.promotion_name:
            print("[dwh4-agent] ERROR --confirm-promotion must exactly match task promotion_name")
            return 2
        try:
            promotion_execution = execute_promotion_with_review_gate(
                task,
                runner=run_command_subprocess,
                confirm_promotion=args.confirm_promotion,
                package_root=args.package_root,
                python_executable=args.python_executable,
                base_env=os.environ,
                token_present=bool(os.environ.get("TUSHARE_TOKEN")),
            )
        except CommandExecutionBlocked as exc:
            print(f"[dwh4-agent] ERROR {exc}")
            return 2
        written = write_promotion_execution_artifacts(task, run_id=args.run_id, execution=promotion_execution)
        _print_summary(
            _promotion_execution_status(promotion_execution),
            written,
            subprocess_executed=promotion_execution.result is not None,
            drive_write_executed=promotion_execution.result is not None,
        )
        return 0 if promotion_execution.result is not None and promotion_execution.result.return_code == 0 else 1

    if args.execute_audit:
        audit_execution = execute_audit_with_runner(
            task,
            runner=run_command_subprocess,
            python_executable=args.python_executable,
            base_env=os.environ,
            token_present=bool(os.environ.get("TUSHARE_TOKEN")),
        )
        written = write_audit_execution_artifacts(task, run_id=args.run_id, execution=audit_execution)
        _print_summary(
            _audit_execution_status(audit_execution),
            written,
            subprocess_executed=audit_execution.audit_executed,
        )
        return 0 if audit_execution.result.return_code == 0 else 1

    plan = build_run_to_prepare_plan(
        task,
        run_id=args.run_id,
        env=os.environ,
        require_runtime_token=not args.allow_missing_token_for_plan,
        python_executable=args.python_executable,
    )
    if not args.execute_run_to_prepare:
        decisions = () if args.skip_review_artifact_scan else collect_run_to_prepare_review_decisions(task, package_root=args.package_root)
        written = write_plan_only_artifacts(task, plan, decisions)
        _print_summary(plan.status, written)
        return 0

    if not plan.ready:
        decisions = () if args.skip_review_artifact_scan else collect_run_to_prepare_review_decisions(task, package_root=args.package_root)
        written = write_plan_only_artifacts(task, plan, decisions)
        _print_summary(plan.status, written)
        print(f"[dwh4-agent] ERROR plan status {plan.status} is not executable")
        return 2

    try:
        execution = execute_run_to_prepare_with_ingest_gate(
            task,
            plan,
            runner=run_command_subprocess,
            base_env=os.environ,
            package_root=args.package_root,
        )
    except CommandExecutionBlocked as exc:
        decisions = () if args.skip_review_artifact_scan else collect_run_to_prepare_review_decisions(task, package_root=args.package_root)
        written = write_plan_only_artifacts(task, plan, decisions)
        _print_summary(plan.status, written)
        print(f"[dwh4-agent] ERROR {exc}")
        return 2

    written = write_run_to_prepare_execution_artifacts(
        task,
        plan,
        execution.decisions,
        execution.results,
        blocked_stage=execution.blocked_stage,
        blocked_reason=execution.blocked_reason,
        package_root=args.package_root,
        python_executable=args.python_executable,
    )
    _print_summary(plan.status, written, subprocess_executed=bool(execution.results))
    return _run_to_prepare_exit_code(execution.run_to_prepare_complete)


if __name__ == "__main__":
    raise SystemExit(main())
