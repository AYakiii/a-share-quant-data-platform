"""Artifact writers for DWH4 Tushare plan-only agent runs."""
from __future__ import annotations

import json
from pathlib import Path

from qsys.workflows.tushare_dwh4_commands import CommandSpec, build_promote_command, run_artifact_paths
from qsys.workflows.tushare_dwh4_executor import AuditExecution, CommandExecutionResult, PromotionExecution, append_command_execution_record
from qsys.workflows.tushare_dwh4_orchestrator import (
    RunToPreparePlan,
    gate_decisions_payload,
    planned_commands_payload,
    workflow_state_payload,
)
from qsys.workflows.tushare_dwh4_reviews import ReviewGateDecision
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask

FINAL_DECISION_READY = "READY FOR PROMOTION"
FINAL_DECISION_BLOCKED = "BLOCKED"
FINAL_DECISION_DELETE_REVIEW = "NEEDS HUMAN DELETE REVIEW"
PROMOTION_ACTIONS = (
    "copy_new",
    "skip_identical",
    "replace_verified_incremental",
    "replace_verified_latest",
    "active_manifest_update",
    "block_non_identical",
    "superseded_legacy_keep",
    "delete_request_only",
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _decision_summary_lines(decisions: tuple[ReviewGateDecision, ...]) -> list[str]:
    if not decisions:
        return ["- Review artifacts collected: no"]
    lines = ["- Review artifacts collected: yes"]
    for decision in decisions:
        lines.append(f"- {decision.stage}: {decision.status} ({len(decision.issues)} issue(s))")
    return lines


def render_plan_only_report(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    decisions: tuple[ReviewGateDecision, ...],
) -> str:
    """Render a token-free Markdown report for a plan-only run."""
    command_stages = ", ".join(command.stage for command in plan.commands) or "none"
    lines = [
        "# DWH4 Agent Plan-Only Report",
        "",
        "## Status",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {plan.run_id}",
        f"- status: {plan.status}",
        f"- token_present: {str(plan.token_present).lower()}",
        f"- planned_command_stages: {command_stages}",
        "",
        "## Task",
        "",
        f"- provider: {task.provider}",
        f"- dataset_version: {task.dataset_version}",
        f"- date_range: {task.start_date}..{task.end_date}",
        f"- api_names: {','.join(task.api_names)}",
        f"- output_root: {task.output_root}",
        f"- drive_dwh_root: {task.drive_dwh_root}",
        f"- promotion_name: {task.promotion_name}",
        "",
        "## Stage Plan",
        "",
    ]
    for stage in plan.stages:
        lines.append(f"- {stage.stage}: {stage.status} ({stage.kind})")
    lines.extend(["", "## Review Decisions", ""])
    lines.extend(_decision_summary_lines(decisions))
    lines.extend(
        [
            "",
            "## Safety Boundaries",
            "",
            "- Subprocess executed: no",
            "- Tushare API called: no",
            "- Prepare executed: no",
            "- Promotion executed: no",
            "- Drive write executed: no",
            "- Drive delete executed: no",
            "- Token value stored: no",
            "",
        ]
    )
    return "\n".join(lines)


def _run_to_prepare_complete(
    plan: RunToPreparePlan,
    results: tuple[CommandExecutionResult, ...],
    blocked_stage: str | None = None,
) -> bool:
    return blocked_stage is None and len(results) == len(plan.commands) and all(result.return_code == 0 for result in results)


def render_run_to_prepare_execution_report(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    decisions: tuple[ReviewGateDecision, ...],
    results: tuple[CommandExecutionResult, ...],
    blocked_stage: str | None = None,
    blocked_reason: str | None = None,
) -> str:
    """Render a token-free Markdown report for an explicit execution run."""
    command_stages = ", ".join(command.stage for command in plan.commands) or "none"
    executed_stages = ", ".join(result.stage for result in results) or "none"
    lines = [
        "# DWH4 Agent Run-To-Prepare Execution Report",
        "",
        "## Status",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {plan.run_id}",
        f"- status: {plan.status}",
        f"- token_present: {str(plan.token_present).lower()}",
        f"- planned_command_stages: {command_stages}",
        f"- executed_command_stages: {executed_stages}",
        f"- run_to_prepare_complete: {str(_run_to_prepare_complete(plan, results, blocked_stage)).lower()}",
        f"- blocked_stage: {blocked_stage or 'none'}",
        f"- blocked_reason: {blocked_reason or 'none'}",
        "",
        "## Task",
        "",
        f"- provider: {task.provider}",
        f"- dataset_version: {task.dataset_version}",
        f"- date_range: {task.start_date}..{task.end_date}",
        f"- api_names: {','.join(task.api_names)}",
        f"- output_root: {task.output_root}",
        f"- drive_dwh_root: {task.drive_dwh_root}",
        f"- promotion_name: {task.promotion_name}",
        "",
        "## Command Results",
        "",
    ]
    if results:
        for result in results:
            lines.append(f"- {result.stage}: return_code={result.return_code}")
    else:
        lines.append("- none")
    lines.extend(["", "## Review Decisions", ""])
    lines.extend(_decision_summary_lines(decisions))
    lines.extend(
        [
            "",
            "## Safety Boundaries",
            "",
            f"- Subprocess executed: {'yes' if results else 'no'}",
            "- Promotion executed: no",
            "- Drive write executed: no",
            "- Drive delete executed: no",
            "- Token value stored: no",
            "",
        ]
    )
    return "\n".join(lines)


def _promotion_decision(decisions: tuple[ReviewGateDecision, ...]) -> ReviewGateDecision | None:
    for decision in decisions:
        if decision.stage == "review-promotion":
            return decision
    return None


def _required_reviewed_bucket_kinds(decision: ReviewGateDecision | None) -> tuple[str, ...]:
    if decision is None or not isinstance(decision.metadata, dict):
        return ()
    raw = decision.metadata.get("review_required_bucket_kinds", [])
    if not isinstance(raw, list):
        return ()
    return tuple(str(kind) for kind in raw if kind)


def _coerce_int(value: object) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except ValueError:
            return None


def _metadata_bool(metadata: dict[str, object], *keys: str) -> bool:
    for key in keys:
        if key not in metadata:
            continue
        value = metadata[key]
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return False
    return False


def _promotion_action_counts(metadata: dict[str, object]) -> dict[str, int]:
    raw = metadata.get("promotion_action_counts", {})
    counts: dict[str, int] = {}
    if isinstance(raw, dict):
        for action in PROMOTION_ACTIONS:
            value = _coerce_int(raw.get(action))
            if value is not None and value > 0:
                counts[action] = value
    legacy = {
        "copy_new": metadata.get("planned_copy_new_count"),
        "skip_identical": metadata.get("planned_skip_identical_count"),
        "block_non_identical": metadata.get("planned_block_non_identical_count"),
        "replace_verified_incremental": metadata.get("planned_replace_verified_incremental_count"),
        "replace_verified_latest": metadata.get("planned_replace_verified_latest_count"),
        "active_manifest_update": metadata.get("planned_active_manifest_update_count"),
        "superseded_legacy_keep": metadata.get("planned_superseded_legacy_keep_count"),
        "delete_request_only": metadata.get("planned_delete_request_only_count"),
    }
    for action, value in legacy.items():
        if counts.get(action, 0):
            continue
        coerced = _coerce_int(value)
        if coerced is not None and coerced > 0:
            counts[action] = coerced
    return counts


def _promotion_actions_present(metadata: dict[str, object]) -> list[str]:
    counts = _promotion_action_counts(metadata)
    return [action for action in PROMOTION_ACTIONS if counts.get(action, 0) > 0]


def _final_promotion_decision_payload(
    task: Dwh4TushareTask,
    promotion_decision: ReviewGateDecision,
) -> dict[str, object]:
    metadata = promotion_decision.metadata or {}
    action_counts = _promotion_action_counts(metadata)
    blocking_reasons: list[str] = []
    planned_block = action_counts.get("block_non_identical", 0)
    delete_requested = _metadata_bool(metadata, "delete_request_generated", "drive_delete_requested", "delete_requested") or action_counts.get("delete_request_only", 0) > 0

    if promotion_decision.status != "PASS":
        blocking_reasons.append("review-promotion gate did not pass")
    if planned_block > 0:
        blocking_reasons.append("non-identical Drive collisions remain blocked")
    if task.drive_mutation_policy is not None and task.drive_mutation_policy.allow_delete:
        blocking_reasons.append("task policy allows Drive delete")
    if not task.promotion_policy.require_final_human_confirmation:
        blocking_reasons.append("final human confirmation is not required by task policy")

    if blocking_reasons:
        return {
            "decision": FINAL_DECISION_BLOCKED,
            "promotion_ready": False,
            "human_action_required": "Resolve blocking review findings before promotion.",
            "blocking_reasons": blocking_reasons,
            "drive_delete_requested": delete_requested,
        }
    if delete_requested:
        return {
            "decision": FINAL_DECISION_DELETE_REVIEW,
            "promotion_ready": False,
            "human_action_required": "Review delete request separately; promotion confirmation is not delete confirmation.",
            "blocking_reasons": ["delete request generated"],
            "drive_delete_requested": True,
        }
    return {
        "decision": FINAL_DECISION_READY,
        "promotion_ready": True,
        "human_action_required": "Type exact promotion name before promotion.",
        "blocking_reasons": [],
        "drive_delete_requested": False,
    }


def _command_payload(command: CommandSpec) -> dict[str, object]:
    return {
        "stage": command.stage,
        "cwd": str(command.cwd),
        "argv": list(command.argv),
        "env_update_keys": [key for key, _value in command.env_updates],
    }


def _final_promotion_command(
    task: Dwh4TushareTask,
    promotion_decision: ReviewGateDecision | None,
    *,
    package_root: str | Path | None = None,
    python_executable: str = "python",
) -> CommandSpec | None:
    if promotion_decision is None or not promotion_decision.passed:
        return None
    if _final_promotion_decision_payload(task, promotion_decision)["decision"] != FINAL_DECISION_READY:
        return None
    return build_promote_command(
        task,
        confirm_promotion=task.promotion_name,
        required_reviewed_bucket_kinds=_required_reviewed_bucket_kinds(promotion_decision),
        package_root=package_root,
        python_executable=python_executable,
    )


def _table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _header in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return lines


def _yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def _csv_value(values: tuple[str, ...]) -> str:
    return ",".join(values) if values else "none"


def render_final_promotion_review(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    promotion_decision: ReviewGateDecision,
    command: CommandSpec | None,
) -> str:
    """Render the human-only final promotion review artifact."""
    metadata = promotion_decision.metadata or {}
    action_counts = _promotion_action_counts(metadata)
    actions_present = _promotion_actions_present(metadata)
    decision_payload = _final_promotion_decision_payload(task, promotion_decision)
    command_payload = _command_payload(command) if command is not None else None
    required_buckets = _required_reviewed_bucket_kinds(promotion_decision)
    delete_requested = bool(decision_payload["drive_delete_requested"])
    blocking_reasons = tuple(str(reason) for reason in decision_payload["blocking_reasons"])
    active_manifest_path = "not configured"
    if task.incremental_policy is not None:
        active_manifest_path = task.incremental_policy.active_manifest_policy.active_manifest_path

    lines: list[str] = [
        "# DWH4 Final Promotion Review",
        "",
        "## 0. Decision Summary",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {plan.run_id}",
        f"- promotion_name: {task.promotion_name}",
        f"**Decision:** {decision_payload['decision']}",
        f"**Human action required:** {decision_payload['human_action_required']}",
        f"**Promotion name:** `{task.promotion_name}`",
        "",
    ]
    lines.extend(
        _table(
            ("Check", "Status"),
            [
                ("Review-promotion gate", promotion_decision.status),
                ("Drive inventory read", "NOT_IMPLEMENTED_IN_I1"),
                ("Incremental plan", "NOT_IMPLEMENTED_IN_I1"),
                ("Row-level duplicate check", "NOT_IMPLEMENTED_IN_I1"),
                ("Non-identical key conflicts", action_counts.get("block_non_identical", metadata.get("planned_block_non_identical_count", ""))),
                ("Verified replacement buckets", "NOT_IMPLEMENTED_IN_I1"),
                ("Stable latest replacements", "NOT_IMPLEMENTED_IN_I1"),
                ("Active manifest update", "NOT_IMPLEMENTED_IN_I1"),
                ("Drive delete executed", "NO"),
                ("Drive delete requested", _yes_no(delete_requested)),
                ("Drive collision block", action_counts.get("block_non_identical", metadata.get("planned_block_non_identical_count", ""))),
                ("Token persisted", "NO"),
            ],
        )
    )
    lines.extend(["", "## 0.1 Promotion Action Summary", ""])
    lines.extend(
        _table(
            ("Action", "Count", "Meaning"),
            [
                ("copy_new", action_counts.get("copy_new", 0), "Drive target missing; copy candidate after confirmation"),
                ("skip_identical", action_counts.get("skip_identical", 0), "Drive target identical; no mutation needed"),
                ("replace_verified_incremental", action_counts.get("replace_verified_incremental", 0), "Verified open-year same-path replacement"),
                ("replace_verified_latest", action_counts.get("replace_verified_latest", 0), "Verified window/snapshot latest replacement"),
                ("active_manifest_update", action_counts.get("active_manifest_update", 0), "Write/update active manifest after confirmation"),
                ("block_non_identical", action_counts.get("block_non_identical", 0), "Blocks promotion"),
                ("superseded_legacy_keep", action_counts.get("superseded_legacy_keep", 0), "Legacy file kept, excluded from active manifest"),
                ("delete_request_only", action_counts.get("delete_request_only", 0), "No delete; requires separate delete review"),
            ],
        )
    )
    lines.extend(["", "## 1. Task Summary", ""])
    lines.extend(
        _table(
            ("Field", "Value"),
            [
                ("provider", task.provider),
                ("dataset_version", task.dataset_version),
                ("date_range", f"{task.start_date}..{task.end_date}"),
                ("api_names", ",".join(task.api_names)),
                ("output_root", task.output_root),
                ("drive_dwh_root", task.drive_dwh_root),
                ("promotion_name", task.promotion_name),
            ],
        )
    )
    lines.extend(["", "## 2. Execution Environment", ""])
    lines.extend(
        _table(
            ("Item", "Value"),
            [
                ("run_id", plan.run_id),
                ("package_root", metadata.get("package_root", "")),
                ("subprocess_executed_before_promotion", "YES"),
                ("promotion_executed", "NO"),
                ("drive_write_executed", "NO"),
                ("drive_delete_executed", "NO"),
                ("token_value_stored", "NO"),
            ],
        )
    )
    lines.extend(["", "## 3. Drive Inventory Summary", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("Drive inventory reader", "NOT_IMPLEMENTED_IN_I1"),
                ("Drive inventory execution", "NOT_EXECUTED"),
                ("Existing Drive assets mutated", "NO"),
            ],
        )
    )
    lines.extend(["", "## 4. Incremental Plan Summary", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("Incremental planner", "NOT_IMPLEMENTED_IN_I1"),
                ("Open-year replacement plan", "NOT_IMPLEMENTED_IN_I1"),
                ("Closed-year freeze policy", "TASK_CONTRACT_ONLY"),
            ],
        )
    )
    lines.extend(["", "## 5. Ingest Summary", ""])
    lines.extend(
        _table(
            ("Check", "Status"),
            [
                ("review-ingest gate", "PASS in gate_decisions.json"),
                ("operator summary", "checked before final review"),
                ("raw ingest executed by final review", "NO"),
            ],
        )
    )
    lines.extend(["", "## 6. By-API Summary", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("operator_summary_by_api.csv", "checked by review-ingest"),
                ("api-level detail in this I1 report", "DEFERRED"),
            ],
        )
    )
    lines.extend(["", "## 7. Compact Summary", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("review-compact gate", "PASS in gate_decisions.json"),
                ("package_root", metadata.get("package_root", "")),
                ("compact package mutated by final review", "NO"),
            ],
        )
    )
    lines.extend(["", "## 8. Replacement Summary", ""])
    lines.extend(
        _table(
            ("API", "Bucket", "Old Rows", "Candidate Rows", "Added Rows", "Identical Overlap", "Conflicts", "Action"),
            [("not available in I1", "not available", "", "", "", "", metadata.get("planned_block_non_identical_count", ""), "not implemented")],
        )
    )
    lines.extend(["", "## 9. Stable Latest Summary", ""])
    lines.extend(
        _table(
            ("API", "Stable Path", "Old Coverage", "New Coverage", "Action"),
            [("not available in I1", "not available", "", "", "not implemented")],
        )
    )
    lines.extend(["", "## 10. Drive Collision Summary", ""])
    lines.extend(
        _table(
            ("Item", "Value"),
            [
                ("collision_rows", metadata.get("collision_rows", "")),
                ("planned_copy_new_count", action_counts.get("copy_new", metadata.get("planned_copy_new_count", ""))),
                ("planned_skip_identical_count", action_counts.get("skip_identical", metadata.get("planned_skip_identical_count", ""))),
                ("planned_block_non_identical_count", action_counts.get("block_non_identical", metadata.get("planned_block_non_identical_count", ""))),
                ("promotion_actions_present", _csv_value(tuple(actions_present))),
            ],
        )
    )
    lines.extend(["", "## 11. Bucket Review", ""])
    lines.extend(
        _table(
            ("Item", "Value"),
            [
                ("review_required_bucket_kinds", _csv_value(required_buckets)),
                ("authorized_bucket_kinds", _csv_value(tuple(task.promotion_policy.allow_reviewed_bucket_kinds))),
            ],
        )
    )
    lines.extend(["", "## 12. Active Manifest Summary", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("active_manifest_path", active_manifest_path),
                ("active manifest generated", "NOT_IMPLEMENTED_IN_I1"),
                ("active manifest updated", "NO"),
            ],
        )
    )
    lines.extend(["", "## 13. No-Delete Guard", ""])
    lines.extend(
        _table(
            ("Item", "Status"),
            [
                ("Drive delete executed", "NO"),
                ("Drive delete requested", _yes_no(delete_requested)),
                ("autonomous Drive delete allowed", "NO"),
                ("promotion confirmation deletes files", "NO"),
            ],
        )
    )
    lines.extend(["", "## 14. Known Gaps / Warnings", ""])
    warning_rows = [
        ("Drive inventory reader", "not implemented in I1"),
        ("Incremental planner", "not implemented in I1"),
        ("Verified replacement", "not implemented in I1"),
        ("Stable latest replacement", "not implemented in I1"),
        ("Active manifest write", "not implemented in I1"),
    ]
    if blocking_reasons:
        warning_rows.extend(("blocking_reason", reason) for reason in blocking_reasons)
    lines.extend(_table(("Item", "Warning"), warning_rows))
    lines.extend(["", "## 15. Final Promotion Command", ""])
    if command_payload is None:
        lines.extend(
            [
                f"Promotion command is not planned because final decision is {decision_payload['decision']}.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "```json",
                json.dumps(command_payload, ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## 16. Confirmation Required",
            "",
            f"- final_human_confirmation_required: {str(task.promotion_policy.require_final_human_confirmation).lower()}",
            f"- exact_confirmation: {task.promotion_name}",
            "",
            "## Safety Boundaries",
            "",
            "- Promotion executed: no",
            "- Drive write executed: no",
            "- Token value stored: no",
            "",
        ]
    )
    return "\n".join(lines)


def write_plan_only_artifacts(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    decisions: tuple[ReviewGateDecision, ...] = (),
) -> dict[str, Path]:
    """Write plan-only artifacts and return the paths written.

    This function intentionally does not create ``commands_executed.jsonl``
    because no external command has executed.
    """
    paths = plan.artifact_paths
    written: dict[str, Path] = {}
    workflow_state = workflow_state_payload(plan)
    planned_commands = planned_commands_payload(plan)
    gate_decisions = gate_decisions_payload(decisions)
    report = render_plan_only_report(task, plan, decisions)

    _write_json(paths["workflow_state"], workflow_state)
    written["workflow_state"] = paths["workflow_state"]
    _write_json(paths["planned_commands"], planned_commands)
    written["planned_commands"] = paths["planned_commands"]
    _write_json(paths["gate_decisions"], gate_decisions)
    written["gate_decisions"] = paths["gate_decisions"]
    _write_text(paths["agent_report"], report)
    written["agent_report"] = paths["agent_report"]
    return written


def write_run_to_prepare_execution_artifacts(
    task: Dwh4TushareTask,
    plan: RunToPreparePlan,
    decisions: tuple[ReviewGateDecision, ...],
    results: tuple[CommandExecutionResult, ...],
    blocked_stage: str | None = None,
    blocked_reason: str | None = None,
    package_root: str | Path | None = None,
    python_executable: str = "python",
) -> dict[str, Path]:
    """Write artifacts for an explicit run-to-prepare execution pass."""
    paths = plan.artifact_paths
    written: dict[str, Path] = {}
    promotion_decision = _promotion_decision(decisions)
    run_complete = _run_to_prepare_complete(plan, results, blocked_stage)
    final_decision = _final_promotion_decision_payload(task, promotion_decision) if run_complete and promotion_decision is not None else None
    promote_command = _final_promotion_command(task, promotion_decision, package_root=package_root, python_executable=python_executable) if run_complete else None
    workflow_state = workflow_state_payload(plan)
    workflow_state.update(
        {
            "subprocess_executed": bool(results),
            "executed_command_stages": [result.stage for result in results],
            "command_return_codes": {result.stage: result.return_code for result in results},
            "run_to_prepare_complete": run_complete,
            "gate_decision_stages": [decision.stage for decision in decisions],
            "blocked_stage": blocked_stage,
            "blocked_reason": blocked_reason,
            "final_promotion_review_ready": run_complete and promotion_decision is not None,
            "final_promotion_decision": final_decision["decision"] if final_decision is not None else None,
            "final_promotion_human_action_required": final_decision["human_action_required"] if final_decision is not None else None,
            "final_promotion_blocking_reasons": final_decision["blocking_reasons"] if final_decision is not None else [],
            "promotion_action_counts": _promotion_action_counts(promotion_decision.metadata or {}) if promotion_decision is not None else {},
            "promotion_actions_present": _promotion_actions_present(promotion_decision.metadata or {}) if promotion_decision is not None else [],
            "promotion_command_planned": promote_command is not None,
            "promotion_command": _command_payload(promote_command) if promote_command is not None else None,
        }
    )
    planned_commands = planned_commands_payload(plan)
    gate_decisions = gate_decisions_payload(decisions)
    report = render_run_to_prepare_execution_report(task, plan, decisions, results, blocked_stage, blocked_reason)

    _write_json(paths["workflow_state"], workflow_state)
    written["workflow_state"] = paths["workflow_state"]
    _write_json(paths["planned_commands"], planned_commands)
    written["planned_commands"] = paths["planned_commands"]
    _write_json(paths["gate_decisions"], gate_decisions)
    written["gate_decisions"] = paths["gate_decisions"]
    if results:
        for result in results:
            append_command_execution_record(paths["commands_executed"], result.record)
        written["commands_executed"] = paths["commands_executed"]
    _write_text(paths["agent_report"], report)
    written["agent_report"] = paths["agent_report"]
    if run_complete and promotion_decision is not None:
        _write_text(paths["final_promotion_review"], render_final_promotion_review(task, plan, promotion_decision, promote_command))
        written["final_promotion_review"] = paths["final_promotion_review"]
    return written


def _promotion_status(execution: PromotionExecution) -> str:
    if execution.result is None:
        return "PROMOTION_BLOCKED"
    if execution.result.return_code == 0:
        return "PROMOTION_EXECUTED"
    return "PROMOTION_FAILED"


def render_promotion_execution_report(
    task: Dwh4TushareTask,
    run_id: str,
    execution: PromotionExecution,
) -> str:
    """Render a token-free promotion execution report."""
    metadata = execution.decision.metadata or {}
    lines = [
        "# DWH4 Promotion Execution Report",
        "",
        "## Status",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {run_id}",
        f"- promotion_name: {task.promotion_name}",
        f"- status: {_promotion_status(execution)}",
        f"- review_promotion_status: {execution.decision.status}",
        f"- promotion_subprocess_executed: {str(execution.result is not None).lower()}",
        f"- promotion_return_code: {execution.result.return_code if execution.result is not None else 'none'}",
        f"- blocked_stage: {execution.blocked_stage or 'none'}",
        f"- blocked_reason: {execution.blocked_reason or 'none'}",
        "",
        "## Promotion Readiness",
        "",
        f"- package_root: {metadata.get('package_root', '')}",
        f"- ready_for_promotion: {metadata.get('ready_for_promotion', '')}",
        f"- planned_copy_new_count: {metadata.get('planned_copy_new_count', '')}",
        f"- planned_skip_identical_count: {metadata.get('planned_skip_identical_count', '')}",
        f"- planned_block_non_identical_count: {metadata.get('planned_block_non_identical_count', '')}",
        f"- promotion_action_counts: {json.dumps(_promotion_action_counts(metadata), ensure_ascii=False, sort_keys=True)}",
        "",
        "## Safety Boundaries",
        "",
        f"- Promotion subprocess executed: {str(execution.result is not None).lower()}",
        f"- Drive write may have executed: {str(execution.result is not None).lower()}",
        "- Token value stored: no",
        "",
    ]
    return "\n".join(lines)


def write_promotion_execution_artifacts(
    task: Dwh4TushareTask,
    *,
    run_id: str,
    execution: PromotionExecution,
) -> dict[str, Path]:
    """Write artifacts for an explicit promotion execution pass."""
    paths = run_artifact_paths(task, run_id)
    written: dict[str, Path] = {}
    state = {
        "workflow_name": task.workflow_name,
        "run_id": run_id,
        "status": _promotion_status(execution),
        "promotion_name": task.promotion_name,
        "review_promotion_status": execution.decision.status,
        "promotion_executed": execution.result is not None,
        "drive_write_executed": execution.result is not None,
        "promotion_return_code": execution.result.return_code if execution.result is not None else None,
        "blocked_stage": execution.blocked_stage,
        "blocked_reason": execution.blocked_reason,
        "gate_decisions": gate_decisions_payload((execution.decision,)),
        "token_value_stored": False,
    }
    _write_json(paths["promotion_execution_state"], state)
    written["promotion_execution_state"] = paths["promotion_execution_state"]
    if execution.result is not None:
        append_command_execution_record(paths["commands_executed"], execution.result.record)
        written["commands_executed"] = paths["commands_executed"]
    _write_text(paths["promotion_execution_report"], render_promotion_execution_report(task, run_id, execution))
    written["promotion_execution_report"] = paths["promotion_execution_report"]
    return written


def _audit_status(execution: AuditExecution) -> str:
    if execution.result.return_code == 0:
        return "AUDIT_EXECUTED"
    return "AUDIT_FAILED"


def render_audit_execution_report(
    task: Dwh4TushareTask,
    run_id: str,
    execution: AuditExecution,
) -> str:
    """Render a token-free read-only audit execution report."""
    lines = [
        "# DWH4 Audit Execution Report",
        "",
        "## Status",
        "",
        f"- workflow_name: {task.workflow_name}",
        f"- run_id: {run_id}",
        f"- promotion_name: {task.promotion_name}",
        f"- status: {_audit_status(execution)}",
        f"- audit_subprocess_executed: {str(execution.audit_executed).lower()}",
        f"- audit_return_code: {execution.result.return_code}",
        f"- blocked_stage: {execution.blocked_stage or 'none'}",
        f"- blocked_reason: {execution.blocked_reason or 'none'}",
        "",
        "## Safety Boundaries",
        "",
        f"- Audit subprocess executed: {str(execution.audit_executed).lower()}",
        "- Drive read may have executed: true",
        "- Drive write executed: false",
        "- Promotion executed: no",
        "- Token value stored: no",
        "",
    ]
    return "\n".join(lines)


def write_audit_execution_artifacts(
    task: Dwh4TushareTask,
    *,
    run_id: str,
    execution: AuditExecution,
) -> dict[str, Path]:
    """Write artifacts for an explicit read-only audit execution pass."""
    paths = run_artifact_paths(task, run_id)
    written: dict[str, Path] = {}
    state = {
        "workflow_name": task.workflow_name,
        "run_id": run_id,
        "status": _audit_status(execution),
        "promotion_name": task.promotion_name,
        "audit_executed": execution.audit_executed,
        "drive_read_executed": execution.audit_executed,
        "drive_write_executed": False,
        "audit_return_code": execution.result.return_code,
        "blocked_stage": execution.blocked_stage,
        "blocked_reason": execution.blocked_reason,
        "token_value_stored": False,
    }
    _write_json(paths["audit_execution_state"], state)
    written["audit_execution_state"] = paths["audit_execution_state"]
    append_command_execution_record(paths["commands_executed"], execution.result.record)
    written["commands_executed"] = paths["commands_executed"]
    _write_text(paths["audit_execution_report"], render_audit_execution_report(task, run_id, execution))
    written["audit_execution_report"] = paths["audit_execution_report"]
    return written
