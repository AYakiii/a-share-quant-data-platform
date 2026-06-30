"""Controlled command execution primitives for the DWH4 Tushare workflow.

This module only executes through an injected runner. It does not choose or
expose a real subprocess implementation, so callers must opt into any external
process behavior explicitly.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping, Protocol, Sequence

from qsys.workflows.tushare_dwh4_commands import CommandSpec, build_audit_command, build_promote_command, command_execution_record
from qsys.workflows.tushare_dwh4_drive_inventory import file_sha256
from qsys.workflows.tushare_dwh4_orchestrator import RunToPreparePlan
from qsys.workflows.tushare_dwh4_reviews import (
    ReviewGateDecision,
    review_compact_artifacts,
    review_ingest_artifacts,
    review_promotion_artifacts,
)
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

RUN_TO_PREPARE_EXECUTABLE_STAGES = ("ingest", "prepare")
SECRET_LIKE_ENV_KEY_PARTS = ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")
VERIFIED_REPLACEMENT_ACTIONS = ("replace_verified_incremental", "replace_verified_latest")
CRITICAL_RESTORE_REQUIRED_STATUS = "CRITICAL_RESTORE_REQUIRED"


class CommandExecutionBlocked(RuntimeError):
    """Raised when a command is not allowed to execute."""


@dataclass(frozen=True)
class RunnerResult:
    """Result returned by an injected command runner."""

    return_code: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class CommandExecutionResult:
    """Token-free execution result plus the non-persisted runner output."""

    stage: str
    command: CommandSpec
    started_at: str
    finished_at: str
    return_code: int
    stdout: str
    stderr: str
    record: dict[str, object]


@dataclass(frozen=True)
class GatedRunToPrepareExecution:
    """Run-to-prepare execution result with mandatory gate decisions."""

    results: tuple[CommandExecutionResult, ...]
    decisions: tuple[ReviewGateDecision, ...]
    blocked_stage: str | None
    blocked_reason: str | None
    run_to_prepare_complete: bool


@dataclass(frozen=True)
class PromotionExecution:
    """Controlled promotion execution result."""

    decision: ReviewGateDecision
    result: CommandExecutionResult | None
    blocked_stage: str | None
    blocked_reason: str | None
    promotion_executed: bool


@dataclass(frozen=True)
class AuditExecution:
    """Controlled read-only audit execution result."""

    result: CommandExecutionResult
    blocked_stage: str | None
    blocked_reason: str | None
    audit_executed: bool


@dataclass(frozen=True)
class FileProfile:
    """Rows, columns, and file digest used by verified replacement checks."""

    rows: int
    columns: tuple[str, ...]
    sha256: str


@dataclass(frozen=True)
class VerifiedReplacementSpec:
    """One Drive replacement that has already passed upstream verification."""

    action: str
    drive_relative_path: str
    candidate_path: Path
    expected_rows: int
    expected_columns: tuple[str, ...]
    expected_sha256: str
    reason: str = ""


@dataclass(frozen=True)
class VerifiedReplacementExecution:
    """Execution result for one verified replacement attempt."""

    action: str
    status: str
    drive_path: Path
    candidate_path: Path
    backup_path: Path
    backup_metadata_path: Path
    critical_restore_path: Path | None
    old_rows: int
    old_columns: tuple[str, ...]
    old_sha256: str
    post_write_rows: int | None
    post_write_columns: tuple[str, ...]
    post_write_sha256: str
    drive_write_executed: bool
    drive_delete_executed: bool
    auto_rollback_executed: bool
    reason: str

    @property
    def passed(self) -> bool:
        """Return whether replacement and post-write verification passed."""
        return self.status == "PASS"

    def payload(self) -> dict[str, object]:
        """Return a JSON-safe execution payload."""
        return {
            "action": self.action,
            "status": self.status,
            "drive_path": str(self.drive_path),
            "candidate_path": str(self.candidate_path),
            "backup_path": str(self.backup_path),
            "backup_metadata_path": str(self.backup_metadata_path),
            "critical_restore_path": str(self.critical_restore_path) if self.critical_restore_path is not None else "",
            "old_rows": self.old_rows,
            "old_columns": list(self.old_columns),
            "old_sha256": self.old_sha256,
            "post_write_rows": self.post_write_rows,
            "post_write_columns": list(self.post_write_columns),
            "post_write_sha256": self.post_write_sha256,
            "drive_write_executed": self.drive_write_executed,
            "drive_delete_executed": self.drive_delete_executed,
            "auto_rollback_executed": self.auto_rollback_executed,
            "reason": self.reason,
        }


class CommandRunner(Protocol):
    """Callable runner interface used by the controlled executor."""

    def __call__(self, command: CommandSpec, *, env: Mapping[str, str]) -> RunnerResult:
        """Run a command with the supplied environment."""


class Clock(Protocol):
    """Clock interface used to make execution records testable."""

    def __call__(self) -> str:
        """Return the current timestamp string."""


class FileProfiler(Protocol):
    """Callable file profile reader used by verified replacement execution."""

    def __call__(self, path: Path) -> FileProfile:
        """Return rows, columns, and sha256 for path."""


class IngestReview(Protocol):
    """Callable read-only ingest review gate."""

    def __call__(self, task: Dwh4TushareTask) -> ReviewGateDecision:
        """Review current ingest artifacts for a task."""


class PackageReview(Protocol):
    """Callable read-only package review gate."""

    def __call__(
        self,
        task: Dwh4TushareTask,
        *,
        package_root: str | Path | None = None,
    ) -> ReviewGateDecision:
        """Review current package artifacts for a task."""


def utc_timestamp() -> str:
    """Return an ISO-8601 UTC timestamp with a Z suffix."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_drive_relative_path(value: str) -> PurePosixPath:
    if not value or "\\" in value or ":" in value:
        raise CommandExecutionBlocked("drive_relative_path must be a safe relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise CommandExecutionBlocked("drive_relative_path must be a safe relative POSIX path")
    return path


def _path_under(root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def parquet_file_profile(path: str | Path) -> FileProfile:
    """Read parquet rows/columns and compute file-level sha256."""
    import pandas as pd

    data_path = Path(path)
    frame = pd.read_parquet(data_path)
    return FileProfile(
        rows=int(len(frame)),
        columns=tuple(str(column) for column in frame.columns),
        sha256=file_sha256(data_path),
    )


def _assert_verified_replacement_policy(
    task: Dwh4TushareTask,
    spec: VerifiedReplacementSpec,
    *,
    confirm_promotion: str,
) -> None:
    if confirm_promotion != task.promotion_name:
        raise CommandExecutionBlocked("--confirm-promotion must exactly match task.promotion_name")
    if spec.action not in VERIFIED_REPLACEMENT_ACTIONS:
        raise CommandExecutionBlocked(f"replacement action must be one of {VERIFIED_REPLACEMENT_ACTIONS!r}")
    if spec.expected_rows < 0:
        raise CommandExecutionBlocked("expected_rows must be non-negative")
    if not spec.expected_columns:
        raise CommandExecutionBlocked("expected_columns must not be empty")
    if not spec.expected_sha256:
        raise CommandExecutionBlocked("expected_sha256 must not be empty")
    policy = task.drive_mutation_policy
    if policy is None or not policy.allow_verified_replace:
        raise CommandExecutionBlocked("task policy does not allow verified replacement")
    if policy.allow_delete:
        raise CommandExecutionBlocked("task policy must not allow Drive delete")
    if not policy.require_final_confirmation_for_replace:
        raise CommandExecutionBlocked("task policy must require final confirmation for replacement")


def _profile_matches(profile: FileProfile, spec: VerifiedReplacementSpec) -> tuple[str, ...]:
    issues: list[str] = []
    if profile.rows != spec.expected_rows:
        issues.append("rows")
    if profile.columns != tuple(spec.expected_columns):
        issues.append("columns")
    if profile.sha256 != spec.expected_sha256:
        issues.append("sha256")
    return tuple(issues)


def _write_backup_metadata(
    path: Path,
    *,
    task: Dwh4TushareTask,
    run_id: str,
    spec: VerifiedReplacementSpec,
    drive_path: Path,
    backup_path: Path,
    old_profile: FileProfile,
    created_at: str,
) -> None:
    payload = {
        "workflow_name": task.workflow_name,
        "run_id": run_id,
        "action": spec.action,
        "drive_path": str(drive_path),
        "backup_path": str(backup_path),
        "old_sha256": old_profile.sha256,
        "old_rows": old_profile.rows,
        "old_columns": list(old_profile.columns),
        "created_at": created_at,
        "backup_local_only": True,
        "drive_delete_executed": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_critical_restore_required(
    path: Path,
    *,
    task: Dwh4TushareTask,
    run_id: str,
    execution: VerifiedReplacementExecution,
    issues: tuple[str, ...],
) -> None:
    lines = [
        "# CRITICAL_RESTORE_REQUIRED",
        "",
        "A post-write verification failed after a verified replacement attempt.",
        "No automatic rollback was performed.",
        "Human must review local backup and Drive state.",
        "",
        "## Context",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {run_id}",
        f"- action: {execution.action}",
        f"- drive_path: {execution.drive_path}",
        f"- candidate_path: {execution.candidate_path}",
        f"- backup_path: {execution.backup_path}",
        f"- failed_checks: {','.join(issues)}",
        "",
        "## Safety",
        "",
        "- auto_rollback_executed: false",
        "- drive_delete_executed: false",
        "- local_backup_created: true",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_subprocess_env(
    command: CommandSpec,
    *,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a subprocess environment for a command without mutating input."""
    env = dict(os.environ if base_env is None else base_env)
    env.update(command.env_update_mapping())
    return env


def run_command_subprocess(command: CommandSpec, *, env: Mapping[str, str]) -> RunnerResult:
    """Run one command through subprocess.run and return captured output."""
    completed = subprocess.run(
        list(command.argv),
        cwd=str(command.cwd),
        env=dict(env),
        capture_output=True,
        text=True,
        check=False,
    )
    return RunnerResult(
        return_code=completed.returncode,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def _secret_like_env_update_keys(command: CommandSpec) -> list[str]:
    keys: list[str] = []
    for key, _value in command.env_updates:
        upper_key = key.upper()
        if any(part in upper_key for part in SECRET_LIKE_ENV_KEY_PARTS):
            keys.append(key)
    return keys


def _assert_allowed_command(command: CommandSpec, allowed_stages: Sequence[str]) -> None:
    allowed = tuple(dict.fromkeys(allowed_stages))
    if not allowed:
        raise CommandExecutionBlocked("allowed_stages must not be empty")
    if command.stage not in set(allowed):
        raise CommandExecutionBlocked(f"stage {command.stage!r} is not in allowed_stages={allowed!r}")
    secret_keys = _secret_like_env_update_keys(command)
    if secret_keys:
        raise CommandExecutionBlocked(f"command env_updates contain secret-like keys: {','.join(secret_keys)}")


def _coerce_runner_result(value: RunnerResult) -> RunnerResult:
    if not isinstance(value.return_code, int):
        raise TypeError("runner return_code must be int")
    return value


def execute_command_with_runner(
    command: CommandSpec,
    *,
    runner: CommandRunner,
    allowed_stages: Sequence[str],
    token_present: bool,
    base_env: Mapping[str, str] | None = None,
    clock: Clock | None = None,
) -> CommandExecutionResult:
    """Execute one allowed command through an injected runner.

    The persisted record intentionally excludes environment values and runner
    stdout/stderr because those streams may contain operational secrets.
    """
    _assert_allowed_command(command, allowed_stages)
    now = utc_timestamp if clock is None else clock
    env = build_subprocess_env(command, base_env=base_env)
    started_at = now()
    runner_result = _coerce_runner_result(runner(command, env=env))
    finished_at = now()
    record = command_execution_record(
        command,
        started_at=started_at,
        finished_at=finished_at,
        return_code=runner_result.return_code,
        token_present=token_present,
    )
    return CommandExecutionResult(
        stage=command.stage,
        command=command,
        started_at=started_at,
        finished_at=finished_at,
        return_code=runner_result.return_code,
        stdout=runner_result.stdout,
        stderr=runner_result.stderr,
        record=record,
    )


def execute_plan_with_runner(
    plan: RunToPreparePlan,
    *,
    runner: CommandRunner,
    allowed_stages: Sequence[str] = RUN_TO_PREPARE_EXECUTABLE_STAGES,
    base_env: Mapping[str, str] | None = None,
    clock: Clock | None = None,
    stop_on_failure: bool = True,
) -> tuple[CommandExecutionResult, ...]:
    """Execute a ready run-to-prepare plan through an injected runner."""
    if not plan.ready:
        raise CommandExecutionBlocked(f"plan status {plan.status!r} is not executable")
    if not plan.token_present:
        raise CommandExecutionBlocked("plan does not record runtime token presence")

    results: list[CommandExecutionResult] = []
    for command in plan.commands:
        result = execute_command_with_runner(
            command,
            runner=runner,
            allowed_stages=allowed_stages,
            token_present=plan.token_present,
            base_env=base_env,
            clock=clock,
        )
        results.append(result)
        if stop_on_failure and result.return_code != 0:
            break
    return tuple(results)


def _command_for_stage(plan: RunToPreparePlan, stage: str) -> CommandSpec:
    for command in plan.commands:
        if command.stage == stage:
            return command
    raise CommandExecutionBlocked(f"run-to-prepare plan does not contain {stage!r} command")


def execute_run_to_prepare_with_ingest_gate(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    *,
    runner: CommandRunner,
    base_env: Mapping[str, str] | None = None,
    clock: Clock | None = None,
    package_root: str | Path | None = None,
    ingest_review: IngestReview = review_ingest_artifacts,
    compact_review: PackageReview = review_compact_artifacts,
    promotion_review: PackageReview = review_promotion_artifacts,
) -> GatedRunToPrepareExecution:
    """Execute run-to-prepare with mandatory read-only gates.

    Execution stops before prepare unless review-ingest passes. After prepare,
    review-compact and review-promotion must also pass. This function never
    promotes, audits, or writes Drive.
    """
    if not plan.ready:
        raise CommandExecutionBlocked(f"plan status {plan.status!r} is not executable")
    if not plan.token_present:
        raise CommandExecutionBlocked("plan does not record runtime token presence")

    ingest_command = _command_for_stage(plan, "ingest")
    prepare_command = _command_for_stage(plan, "prepare")
    results: list[CommandExecutionResult] = []
    decisions: list[ReviewGateDecision] = []

    ingest_result = execute_command_with_runner(
        ingest_command,
        runner=runner,
        allowed_stages=("ingest",),
        token_present=plan.token_present,
        base_env=base_env,
        clock=clock,
    )
    results.append(ingest_result)
    if ingest_result.return_code != 0:
        return GatedRunToPrepareExecution(
            results=tuple(results),
            decisions=tuple(decisions),
            blocked_stage="review-ingest",
            blocked_reason="ingest_return_code",
            run_to_prepare_complete=False,
        )

    ingest_decision = ingest_review(task)
    decisions.append(ingest_decision)
    if not ingest_decision.passed:
        return GatedRunToPrepareExecution(
            results=tuple(results),
            decisions=tuple(decisions),
            blocked_stage="prepare",
            blocked_reason="review-ingest",
            run_to_prepare_complete=False,
        )

    prepare_result = execute_command_with_runner(
        prepare_command,
        runner=runner,
        allowed_stages=("prepare",),
        token_present=plan.token_present,
        base_env=base_env,
        clock=clock,
    )
    results.append(prepare_result)
    if prepare_result.return_code != 0:
        return GatedRunToPrepareExecution(
            results=tuple(results),
            decisions=tuple(decisions),
            blocked_stage="review-compact",
            blocked_reason="prepare_return_code",
            run_to_prepare_complete=False,
        )

    compact_decision = compact_review(task, package_root=package_root)
    decisions.append(compact_decision)
    if not compact_decision.passed:
        return GatedRunToPrepareExecution(
            results=tuple(results),
            decisions=tuple(decisions),
            blocked_stage="review-compact",
            blocked_reason="review-compact",
            run_to_prepare_complete=False,
        )

    promotion_decision = promotion_review(task, package_root=package_root)
    decisions.append(promotion_decision)
    if not promotion_decision.passed:
        return GatedRunToPrepareExecution(
            results=tuple(results),
            decisions=tuple(decisions),
            blocked_stage="review-promotion",
            blocked_reason="review-promotion",
            run_to_prepare_complete=False,
        )

    return GatedRunToPrepareExecution(
        results=tuple(results),
        decisions=tuple(decisions),
        blocked_stage=None,
        blocked_reason=None,
        run_to_prepare_complete=True,
    )


def _required_reviewed_bucket_kinds(decision: ReviewGateDecision) -> tuple[str, ...]:
    metadata = decision.metadata or {}
    raw = metadata.get("review_required_bucket_kinds", [])
    if not isinstance(raw, list):
        return ()
    return tuple(str(kind) for kind in raw if kind)


def _metadata_action_count(decision: ReviewGateDecision, action: str) -> int:
    metadata = decision.metadata or {}
    counts = metadata.get("promotion_action_counts", {})
    if isinstance(counts, dict):
        value = counts.get(action)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    return 0


def _promotion_review_block_reason(decision: ReviewGateDecision) -> str | None:
    metadata = decision.metadata or {}
    delete_requested = bool(metadata.get("delete_request_generated")) or _metadata_action_count(decision, "delete_request_only") > 0
    if delete_requested:
        return "delete_request_only"
    if _metadata_action_count(decision, "block_non_identical") > 0:
        return "block_non_identical"
    return None


def execute_promotion_with_review_gate(
    task: Dwh4TushareTask,
    *,
    runner: CommandRunner,
    confirm_promotion: str,
    package_root: str | Path | None = None,
    python_executable: str = "python",
    base_env: Mapping[str, str] | None = None,
    clock: Clock | None = None,
    promotion_review: PackageReview = review_promotion_artifacts,
    token_present: bool = False,
) -> PromotionExecution:
    """Execute promote only after an explicit confirmation and PASS review.

    This function can perform a Drive-writing promotion when called with a real
    subprocess runner. Callers must keep it behind an explicit human gate.
    """
    if confirm_promotion != task.promotion_name:
        raise CommandExecutionBlocked("--confirm-promotion must exactly match task.promotion_name")

    decision = promotion_review(task, package_root=package_root)
    if not decision.passed:
        return PromotionExecution(
            decision=decision,
            result=None,
            blocked_stage="promote",
            blocked_reason="review-promotion",
            promotion_executed=False,
        )
    block_reason = _promotion_review_block_reason(decision)
    if block_reason is not None:
        return PromotionExecution(
            decision=decision,
            result=None,
            blocked_stage="promote",
            blocked_reason=block_reason,
            promotion_executed=False,
        )

    try:
        command = build_promote_command(
            task,
            confirm_promotion=confirm_promotion,
            required_reviewed_bucket_kinds=_required_reviewed_bucket_kinds(decision),
            package_root=package_root,
            python_executable=python_executable,
        )
    except ValueError as exc:
        raise CommandExecutionBlocked(str(exc)) from exc
    result = execute_command_with_runner(
        command,
        runner=runner,
        allowed_stages=("promote",),
        token_present=token_present,
        base_env=base_env,
        clock=clock,
    )
    return PromotionExecution(
        decision=decision,
        result=result,
        blocked_stage="audit" if result.return_code != 0 else None,
        blocked_reason="promotion_return_code" if result.return_code != 0 else None,
        promotion_executed=True,
    )


def execute_audit_with_runner(
    task: Dwh4TushareTask,
    *,
    runner: CommandRunner,
    python_executable: str = "python",
    base_env: Mapping[str, str] | None = None,
    clock: Clock | None = None,
    token_present: bool = False,
) -> AuditExecution:
    """Execute the read-only compact audit command through an injected runner.

    The audit command is allowed only for stage ``audit`` and is kept separate
    from ingest, prepare, and promote execution modes.
    """
    command = build_audit_command(task, python_executable=python_executable)
    result = execute_command_with_runner(
        command,
        runner=runner,
        allowed_stages=("audit",),
        token_present=token_present,
        base_env=base_env,
        clock=clock,
    )
    return AuditExecution(
        result=result,
        blocked_stage="audit" if result.return_code != 0 else None,
        blocked_reason="audit_return_code" if result.return_code != 0 else None,
        audit_executed=True,
    )


def execute_verified_replacement(
    task: Dwh4TushareTask,
    spec: VerifiedReplacementSpec,
    *,
    run_id: str,
    confirm_promotion: str,
    file_profiler: FileProfiler = parquet_file_profile,
    clock: Clock | None = None,
) -> VerifiedReplacementExecution:
    """Execute one explicit DWH4.1 verified replacement.

    This is a Drive-writing primitive and must remain behind final human
    confirmation. It creates a local backup before replacing the existing
    Drive file, verifies rows/columns/sha after the write, and never performs
    automatic rollback.
    """
    _assert_verified_replacement_policy(task, spec, confirm_promotion=confirm_promotion)
    if not run_id or "/" in run_id or "\\" in run_id:
        raise CommandExecutionBlocked("run_id must be a non-empty path-safe leaf")

    relative = _safe_drive_relative_path(spec.drive_relative_path)
    drive_root = task.drive_dwh_root.resolve()
    drive_path = (drive_root / Path(*relative.parts)).resolve()
    candidate_path = Path(spec.candidate_path).resolve()
    if not _path_under(drive_root, drive_path):
        raise CommandExecutionBlocked("drive path must stay under drive_dwh_root")
    if _path_under(drive_root, candidate_path):
        raise CommandExecutionBlocked("candidate_path must be a local artifact outside drive_dwh_root")
    if not drive_path.exists() or not drive_path.is_file():
        raise CommandExecutionBlocked("drive replacement target must be an existing file")
    if not candidate_path.exists() or not candidate_path.is_file():
        raise CommandExecutionBlocked("candidate_path must be an existing file")

    candidate_profile = file_profiler(candidate_path)
    candidate_issues = _profile_matches(candidate_profile, spec)
    if candidate_issues:
        raise CommandExecutionBlocked(f"candidate verification failed before Drive write: {','.join(candidate_issues)}")

    now = utc_timestamp if clock is None else clock
    created_at = now()
    old_profile = file_profiler(drive_path)
    backup_path = task.ops_workspace / "runs" / task.workflow_name / run_id / "drive_backups" / Path(*relative.parts)
    backup_metadata_path = backup_path.with_name(f"{backup_path.name}.backup.json")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(drive_path, backup_path)
    _write_backup_metadata(
        backup_metadata_path,
        task=task,
        run_id=run_id,
        spec=spec,
        drive_path=drive_path,
        backup_path=backup_path,
        old_profile=old_profile,
        created_at=created_at,
    )

    critical_restore_path = task.ops_workspace / "runs" / task.workflow_name / run_id / "CRITICAL_RESTORE_REQUIRED.md"
    shutil.copy2(candidate_path, drive_path)
    post_profile = file_profiler(drive_path)
    post_issues = _profile_matches(post_profile, spec)
    if post_issues:
        execution = VerifiedReplacementExecution(
            action=spec.action,
            status=CRITICAL_RESTORE_REQUIRED_STATUS,
            drive_path=drive_path,
            candidate_path=candidate_path,
            backup_path=backup_path,
            backup_metadata_path=backup_metadata_path,
            critical_restore_path=critical_restore_path,
            old_rows=old_profile.rows,
            old_columns=old_profile.columns,
            old_sha256=old_profile.sha256,
            post_write_rows=post_profile.rows,
            post_write_columns=post_profile.columns,
            post_write_sha256=post_profile.sha256,
            drive_write_executed=True,
            drive_delete_executed=False,
            auto_rollback_executed=False,
            reason=f"post_write_verification_failed:{','.join(post_issues)}",
        )
        _write_critical_restore_required(
            critical_restore_path,
            task=task,
            run_id=run_id,
            execution=execution,
            issues=post_issues,
        )
        return execution

    return VerifiedReplacementExecution(
        action=spec.action,
        status="PASS",
        drive_path=drive_path,
        candidate_path=candidate_path,
        backup_path=backup_path,
        backup_metadata_path=backup_metadata_path,
        critical_restore_path=None,
        old_rows=old_profile.rows,
        old_columns=old_profile.columns,
        old_sha256=old_profile.sha256,
        post_write_rows=post_profile.rows,
        post_write_columns=post_profile.columns,
        post_write_sha256=post_profile.sha256,
        drive_write_executed=True,
        drive_delete_executed=False,
        auto_rollback_executed=False,
        reason=spec.reason or "verified replacement passed post-write verification",
    )


def append_command_execution_record(path: str | Path, record: Mapping[str, object]) -> Path:
    """Append one token-free command execution record to commands_executed.jsonl."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(dict(record), ensure_ascii=False, sort_keys=True))
        fh.write("\n")
    return output_path
