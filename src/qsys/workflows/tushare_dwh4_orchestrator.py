"""Run-to-prepare planning skeleton for the DWH4 Tushare workflow.

This module performs in-process planning only. It does not run subprocesses,
call Tushare, prepare compact packages, promote, audit, or write artifacts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from qsys.workflows.tushare_dwh4_commands import (
    CommandSpec,
    build_run_to_prepare_commands,
    run_artifact_paths,
)
from qsys.workflows.tushare_dwh4_reviews import (
    ReviewGateDecision,
    review_compact_artifacts,
    review_ingest_artifacts,
    review_promotion_artifacts,
)
from qsys.workflows.tushare_dwh4_task import (
    Dwh4TushareTask,
    TaskValidationIssue,
    runtime_token_present,
    validate_dwh4_tushare_task,
)

RUN_TO_PREPARE_STAGE_SEQUENCE = (
    "validate",
    "ingest",
    "review-ingest",
    "prepare",
    "review-compact",
    "review-promotion",
)
DWH41_INCREMENTAL_MODE = "drive_aware_incremental"
DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE = (
    "validate",
    "drive-inventory",
    "incremental-plan",
    "ingest",
    "review-ingest",
    "incremental-merge",
    "prepare",
    "review-compact",
    "review-promotion",
    "final-review",
)

READY_TO_RUN_TO_PREPARE = "READY_TO_RUN_TO_PREPARE"
BLOCKED_VALIDATION = "BLOCKED_VALIDATION"
BLOCKED_TOKEN = "BLOCKED_TOKEN"


@dataclass(frozen=True)
class PlannedWorkflowStage:
    """One planned run-to-prepare stage."""

    stage: str
    kind: str
    status: str


@dataclass(frozen=True)
class RunToPreparePlan:
    """Token-free run-to-prepare plan."""

    workflow_name: str
    run_id: str
    status: str
    token_present: bool
    validation_issues: tuple[TaskValidationIssue, ...]
    stage_sequence: tuple[str, ...]
    stages: tuple[PlannedWorkflowStage, ...]
    commands: tuple[CommandSpec, ...]
    artifact_paths: dict[str, Path]

    @property
    def ready(self) -> bool:
        """Return whether the plan can proceed to external command execution."""
        return self.status == READY_TO_RUN_TO_PREPARE


def _stage_kind(stage: str) -> str:
    if stage in {"ingest", "prepare"}:
        return "command"
    if stage.startswith("review-"):
        return "review"
    return "in_process"


def run_to_prepare_stage_sequence(task: Dwh4TushareTask) -> tuple[str, ...]:
    """Return the legacy or DWH4.1 v2 run-to-prepare stage sequence."""
    policy = task.incremental_policy
    if policy is not None and policy.enabled and policy.mode == DWH41_INCREMENTAL_MODE:
        return DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE
    return RUN_TO_PREPARE_STAGE_SEQUENCE


def _stage_statuses(status: str, stage_sequence: tuple[str, ...]) -> tuple[PlannedWorkflowStage, ...]:
    if status == READY_TO_RUN_TO_PREPARE:
        return tuple(
            PlannedWorkflowStage(stage=stage, kind=_stage_kind(stage), status="PASS" if stage == "validate" else "PENDING")
            for stage in stage_sequence
        )
    validate_status = "BLOCKED_TOKEN" if status == BLOCKED_TOKEN else "FAIL"
    return tuple(
        PlannedWorkflowStage(
            stage=stage,
            kind=_stage_kind(stage),
            status=validate_status if stage == "validate" else "NOT_PLANNED",
        )
        for stage in stage_sequence
    )


def build_run_to_prepare_plan(
    task: Dwh4TushareTask,
    *,
    run_id: str,
    env: Mapping[str, str] | None = None,
    require_runtime_token: bool = True,
    python_executable: str = "python",
) -> RunToPreparePlan:
    """Build a token-free run-to-prepare plan without executing commands."""
    stage_sequence = run_to_prepare_stage_sequence(task)
    token_present = runtime_token_present(env)
    validation_issues = tuple(validate_dwh4_tushare_task(task, check_runtime_token=require_runtime_token, env=env))
    errors = [issue for issue in validation_issues if issue.severity == "ERROR"]
    missing_token = any(issue.code == "TOKEN_NOT_PRESENT" for issue in validation_issues)
    if errors:
        status = BLOCKED_VALIDATION
        commands: tuple[CommandSpec, ...] = ()
    elif missing_token:
        status = BLOCKED_TOKEN
        commands = ()
    else:
        status = READY_TO_RUN_TO_PREPARE
        commands = build_run_to_prepare_commands(task, python_executable=python_executable)
    return RunToPreparePlan(
        workflow_name=task.workflow_name,
        run_id=run_id,
        status=status,
        token_present=token_present,
        validation_issues=validation_issues,
        stage_sequence=stage_sequence,
        stages=_stage_statuses(status, stage_sequence),
        commands=commands,
        artifact_paths=run_artifact_paths(task, run_id),
    )


def validation_issue_payload(issue: TaskValidationIssue) -> dict[str, str]:
    """Return a JSON-safe validation issue payload."""
    return {
        "severity": issue.severity,
        "code": issue.code,
        "field": issue.field,
        "message": issue.message,
    }


def workflow_state_payload(plan: RunToPreparePlan) -> dict[str, object]:
    """Build a token-free workflow_state.json payload for a plan."""
    return {
        "workflow_name": plan.workflow_name,
        "run_id": plan.run_id,
        "status": plan.status,
        "token_present": plan.token_present,
        "stage_sequence": list(plan.stage_sequence),
        "run_to_prepare_v2": plan.stage_sequence == DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE,
        "stages": [
            {"stage": stage.stage, "kind": stage.kind, "status": stage.status}
            for stage in plan.stages
        ],
        "planned_command_stages": [command.stage for command in plan.commands],
        "validation": {
            "errors": [validation_issue_payload(issue) for issue in plan.validation_issues if issue.severity == "ERROR"],
            "warnings": [validation_issue_payload(issue) for issue in plan.validation_issues if issue.severity == "WARNING"],
        },
        "artifact_paths": {name: str(path) for name, path in plan.artifact_paths.items()},
        "promotion_executed": False,
        "drive_write_executed": False,
        "drive_delete_executed": False,
    }


def planned_commands_payload(plan: RunToPreparePlan) -> list[dict[str, object]]:
    """Build token-free planned command payloads."""
    return [
        {
            "stage": command.stage,
            "cwd": str(command.cwd),
            "argv": list(command.argv),
            "env_update_keys": [key for key, _value in command.env_updates],
        }
        for command in plan.commands
    ]


def collect_run_to_prepare_review_decisions(
    task: Dwh4TushareTask,
    *,
    package_root: str | Path | None = None,
) -> tuple[ReviewGateDecision, ...]:
    """Collect read-only run-to-prepare review decisions from current artifacts."""
    return (
        review_ingest_artifacts(task),
        review_compact_artifacts(task, package_root=package_root),
        review_promotion_artifacts(task, package_root=package_root),
    )


def review_issue_payload(issue: Any) -> dict[str, str]:
    """Return a JSON-safe review issue payload."""
    return {
        "code": str(issue.code),
        "field": str(issue.field),
        "message": str(issue.message),
    }


def gate_decisions_payload(decisions: tuple[ReviewGateDecision, ...]) -> dict[str, object]:
    """Build a gate_decisions.json payload from review decisions."""
    return {
        decision.stage: {
            "status": decision.status,
            "checked": list(decision.checked),
            "issues": [review_issue_payload(issue) for issue in decision.issues],
            "metadata": decision.metadata or {},
        }
        for decision in decisions
    }
