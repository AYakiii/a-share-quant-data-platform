"""Task-sheet contract for the DWH4 dual-entry Tushare workflow."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Mapping

from qsys.data.sources.tushare_source_registry import source_specs_by_api

SECRET_LIKE_KEY_FRAGMENTS = ("token", "secret", "password", "credential", "api_key")
SECRET_KEY_ALLOWLIST = {("human_intervention_policy", "only_token_and_final_promotion")}
SUPPORTED_WORKFLOW_MODE = "dwh4_dual_entry_single_core"
SUPPORTED_PROVIDER = "tushare"
SUPPORTED_INCREMENTAL_MODE = "drive_aware_incremental"
SUPPORTED_TARGET_END_DATE_POLICY = "latest_open_trading_day"

_DATE_FORMAT = "%Y%m%d"
_PATH_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DRIVE_MARKERS = (
    "/content/gdrive",
    "\\content\\gdrive",
    "google drive",
    "googledrive",
    "mydrive",
    "g:/",
)


@dataclass(frozen=True)
class Dwh4ExecutionSettings:
    """Execution tuning for the Tushare raw ingest runner."""

    max_workers: int
    request_sleep: float
    request_jitter: float
    retry: int
    heartbeat_sec: float
    resume: bool


@dataclass(frozen=True)
class Dwh4RepoPolicy:
    """Repository sync and cleanliness policy recorded by the task sheet."""

    sync_policy: str
    require_clean_worktree: bool
    record_git_commit: bool


@dataclass(frozen=True)
class Dwh4AutoReviewPolicy:
    """Automatic gate requirements after ingest, compact, and prepare."""

    require_ingest_rough_check_pass: bool
    require_all_api_rough_check_pass: bool
    require_zero_bad_status_partitions: bool
    require_zero_failed_partitions: bool
    require_zero_disallowed_empty_partitions: bool
    require_zero_duplicate_partitions: bool
    require_zero_missing_data_files: bool
    require_zero_missing_metadata_files: bool
    require_zero_missing_required_fields: bool
    require_compact_qa_all_ok: bool
    require_zero_failed_backlog: bool
    require_ready_for_promotion: bool
    block_non_identical_drive_collision: bool


@dataclass(frozen=True)
class Dwh4PromotionPolicy:
    """Promotion policy for the human-gated final Drive write."""

    auto_prepare: bool
    auto_promote: bool
    require_final_human_confirmation: bool
    allow_reviewed_bucket_kinds: tuple[str, ...]


@dataclass(frozen=True)
class Dwh4HumanInterventionPolicy:
    """Policy for where the agent may pause for human input."""

    only_token_and_final_promotion: bool
    do_not_pause_for_scope_review: bool
    do_not_pause_for_ingest_review: bool
    do_not_pause_for_compact_review: bool
    on_any_review_failure: str


@dataclass(frozen=True)
class Dwh4DriveInventoryPolicy:
    """Read-only Drive inventory policy for DWH4.1."""

    enabled: bool
    scan_raw_tushare: bool
    read_parquet_metadata: bool
    compute_sha256: bool
    fail_on_unreadable_existing_asset: bool


@dataclass(frozen=True)
class Dwh4OpenYearPolicy:
    """Open-year replacement policy for DWH4.1 incremental updates."""

    enabled: bool
    replace_current_year_bucket: bool
    freeze_closed_years: bool
    overlap_trading_days: int
    clip_overlap_to_open_year: bool
    block_on_non_identical_key_conflict: bool
    allow_identical_overlap_collapse: bool


@dataclass(frozen=True)
class Dwh4StableLatestPolicy:
    """Stable latest bucket policy for range and snapshot assets."""

    enabled: bool
    range_apis: tuple[str, ...]
    snapshot_apis: tuple[str, ...]
    range_bucket: str
    snapshot_bucket: str


@dataclass(frozen=True)
class Dwh4ActiveManifestPolicy:
    """Active manifest policy for current canonical Drive assets."""

    enabled: bool
    write_active_manifest: bool
    active_manifest_path: str


@dataclass(frozen=True)
class Dwh4IncrementalPolicy:
    """DWH4.1 Drive-aware incremental policy."""

    enabled: bool
    mode: str
    target_end_date_policy: str
    as_of_date: str
    data_lag_trading_days: int
    open_year_policy: Dwh4OpenYearPolicy
    stable_latest_policy: Dwh4StableLatestPolicy
    active_manifest_policy: Dwh4ActiveManifestPolicy


@dataclass(frozen=True)
class Dwh4DriveMutationPolicy:
    """Drive mutation guardrails for DWH4.1."""

    allow_delete: bool
    allow_verified_replace: bool
    require_final_confirmation_for_replace: bool
    generate_delete_request_only: bool
    backup_old_drive_assets_locally_before_replace: bool


@dataclass(frozen=True)
class Dwh4TushareTask:
    """Validated task-sheet shape shared by notebook and agent routes."""

    workflow_name: str
    workflow_mode: str
    execution_repo: Path
    ops_workspace: Path
    provider: str
    symbols_file: Path
    universe_name: str
    expected_symbol_count: int | None
    dataset_version: str
    start_date: str
    end_date: str
    api_names: tuple[str, ...]
    allow_candidate_sources: bool
    work_name: str
    output_root: Path
    drive_dwh_root: Path
    promotion_name: str
    execution: Dwh4ExecutionSettings
    repo_policy: Dwh4RepoPolicy
    auto_review_policy: Dwh4AutoReviewPolicy
    promotion_policy: Dwh4PromotionPolicy
    human_intervention_policy: Dwh4HumanInterventionPolicy
    drive_inventory_policy: Dwh4DriveInventoryPolicy | None = None
    incremental_policy: Dwh4IncrementalPolicy | None = None
    drive_mutation_policy: Dwh4DriveMutationPolicy | None = None
    secret_like_key_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class TaskValidationIssue:
    """Machine-readable task validation issue."""

    severity: str
    code: str
    field: str
    message: str


def _require_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"task field {key!r} must be an object")
    return value


def _optional_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"task field {key!r} must be an object")
    return value

def _require_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"task field {key!r} must be a non-empty string")
    return value


def _as_bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"task field {key!r} must be a boolean")
    return value


def _as_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"task field {key!r} must be an integer")
    return value


def _as_non_negative_int(payload: Mapping[str, Any], key: str) -> int:
    value = _as_int(payload, key)
    if value < 0:
        raise ValueError(f"task field {key!r} must be non-negative")
    return value


def _as_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"task field {key!r} must be numeric")
    return float(value)


def _as_str_tuple(payload: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"task field {key!r} must be a list of non-empty strings")
    return tuple(value)


def _parse_drive_inventory_policy(mapping: Mapping[str, Any] | None) -> Dwh4DriveInventoryPolicy | None:
    if mapping is None:
        return None
    return Dwh4DriveInventoryPolicy(
        enabled=_as_bool(mapping, "enabled"),
        scan_raw_tushare=_as_bool(mapping, "scan_raw_tushare"),
        read_parquet_metadata=_as_bool(mapping, "read_parquet_metadata"),
        compute_sha256=_as_bool(mapping, "compute_sha256"),
        fail_on_unreadable_existing_asset=_as_bool(mapping, "fail_on_unreadable_existing_asset"),
    )


def _parse_open_year_policy(mapping: Mapping[str, Any]) -> Dwh4OpenYearPolicy:
    return Dwh4OpenYearPolicy(
        enabled=_as_bool(mapping, "enabled"),
        replace_current_year_bucket=_as_bool(mapping, "replace_current_year_bucket"),
        freeze_closed_years=_as_bool(mapping, "freeze_closed_years"),
        overlap_trading_days=_as_non_negative_int(mapping, "overlap_trading_days"),
        clip_overlap_to_open_year=_as_bool(mapping, "clip_overlap_to_open_year"),
        block_on_non_identical_key_conflict=_as_bool(mapping, "block_on_non_identical_key_conflict"),
        allow_identical_overlap_collapse=_as_bool(mapping, "allow_identical_overlap_collapse"),
    )


def _parse_stable_latest_policy(mapping: Mapping[str, Any]) -> Dwh4StableLatestPolicy:
    return Dwh4StableLatestPolicy(
        enabled=_as_bool(mapping, "enabled"),
        range_apis=_as_str_tuple(mapping, "range_apis"),
        snapshot_apis=_as_str_tuple(mapping, "snapshot_apis"),
        range_bucket=_require_str(mapping, "range_bucket"),
        snapshot_bucket=_require_str(mapping, "snapshot_bucket"),
    )


def _parse_active_manifest_policy(mapping: Mapping[str, Any]) -> Dwh4ActiveManifestPolicy:
    return Dwh4ActiveManifestPolicy(
        enabled=_as_bool(mapping, "enabled"),
        write_active_manifest=_as_bool(mapping, "write_active_manifest"),
        active_manifest_path=_require_str(mapping, "active_manifest_path"),
    )


def _parse_incremental_policy(mapping: Mapping[str, Any] | None) -> Dwh4IncrementalPolicy | None:
    if mapping is None:
        return None
    open_year_policy = _require_mapping(mapping, "open_year_policy")
    stable_latest_policy = _require_mapping(mapping, "stable_latest_policy")
    active_manifest_policy = _require_mapping(mapping, "active_manifest_policy")
    return Dwh4IncrementalPolicy(
        enabled=_as_bool(mapping, "enabled"),
        mode=_require_str(mapping, "mode"),
        target_end_date_policy=_require_str(mapping, "target_end_date_policy"),
        as_of_date=_require_str(mapping, "as_of_date"),
        data_lag_trading_days=_as_non_negative_int(mapping, "data_lag_trading_days"),
        open_year_policy=_parse_open_year_policy(open_year_policy),
        stable_latest_policy=_parse_stable_latest_policy(stable_latest_policy),
        active_manifest_policy=_parse_active_manifest_policy(active_manifest_policy),
    )


def _parse_drive_mutation_policy(mapping: Mapping[str, Any] | None) -> Dwh4DriveMutationPolicy | None:
    if mapping is None:
        return None
    return Dwh4DriveMutationPolicy(
        allow_delete=_as_bool(mapping, "allow_delete"),
        allow_verified_replace=_as_bool(mapping, "allow_verified_replace"),
        require_final_confirmation_for_replace=_as_bool(mapping, "require_final_confirmation_for_replace"),
        generate_delete_request_only=_as_bool(mapping, "generate_delete_request_only"),
        backup_old_drive_assets_locally_before_replace=_as_bool(mapping, "backup_old_drive_assets_locally_before_replace"),
    )


def _secret_like_key_paths(payload: Any, path: tuple[str, ...] = ()) -> tuple[str, ...]:
    hits: list[str] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if not isinstance(key, str):
                continue
            key_path = path + (key,)
            lower = key.lower()
            if key_path not in SECRET_KEY_ALLOWLIST and any(fragment in lower for fragment in SECRET_LIKE_KEY_FRAGMENTS):
                hits.append(".".join(key_path))
            hits.extend(_secret_like_key_paths(value, key_path))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            hits.extend(_secret_like_key_paths(item, path + (f"[{index}]",)))
    return tuple(hits)


def task_from_dict(payload: Mapping[str, Any]) -> Dwh4TushareTask:
    """Build a typed task object from a decoded JSON mapping."""
    execution = _require_mapping(payload, "execution")
    repo_policy = _require_mapping(payload, "repo_policy")
    auto_review_policy = _require_mapping(payload, "auto_review_policy")
    promotion_policy = _require_mapping(payload, "promotion_policy")
    human_policy = _require_mapping(payload, "human_intervention_policy")
    drive_inventory_policy = _optional_mapping(payload, "drive_inventory_policy")
    incremental_policy = _optional_mapping(payload, "incremental_policy")
    drive_mutation_policy = _optional_mapping(payload, "drive_mutation_policy")
    expected_count = payload.get("expected_symbol_count")
    if expected_count is not None and (not isinstance(expected_count, int) or isinstance(expected_count, bool)):
        raise ValueError("task field 'expected_symbol_count' must be an integer or null")
    return Dwh4TushareTask(
        workflow_name=_require_str(payload, "workflow_name"),
        workflow_mode=_require_str(payload, "workflow_mode"),
        execution_repo=Path(_require_str(payload, "execution_repo")),
        ops_workspace=Path(_require_str(payload, "ops_workspace")),
        provider=_require_str(payload, "provider"),
        symbols_file=Path(_require_str(payload, "symbols_file")),
        universe_name=_require_str(payload, "universe_name"),
        expected_symbol_count=expected_count,
        dataset_version=_require_str(payload, "dataset_version"),
        start_date=_require_str(payload, "start_date"),
        end_date=_require_str(payload, "end_date"),
        api_names=_as_str_tuple(payload, "api_names"),
        allow_candidate_sources=_as_bool(payload, "allow_candidate_sources"),
        work_name=_require_str(payload, "work_name"),
        output_root=Path(_require_str(payload, "output_root")),
        drive_dwh_root=Path(_require_str(payload, "drive_dwh_root")),
        promotion_name=_require_str(payload, "promotion_name"),
        execution=Dwh4ExecutionSettings(
            max_workers=_as_int(execution, "max_workers"),
            request_sleep=_as_float(execution, "request_sleep"),
            request_jitter=_as_float(execution, "request_jitter"),
            retry=_as_int(execution, "retry"),
            heartbeat_sec=_as_float(execution, "heartbeat_sec"),
            resume=_as_bool(execution, "resume"),
        ),
        repo_policy=Dwh4RepoPolicy(
            sync_policy=_require_str(repo_policy, "sync_policy"),
            require_clean_worktree=_as_bool(repo_policy, "require_clean_worktree"),
            record_git_commit=_as_bool(repo_policy, "record_git_commit"),
        ),
        auto_review_policy=Dwh4AutoReviewPolicy(
            require_ingest_rough_check_pass=_as_bool(auto_review_policy, "require_ingest_rough_check_pass"),
            require_all_api_rough_check_pass=_as_bool(auto_review_policy, "require_all_api_rough_check_pass"),
            require_zero_bad_status_partitions=_as_bool(auto_review_policy, "require_zero_bad_status_partitions"),
            require_zero_failed_partitions=_as_bool(auto_review_policy, "require_zero_failed_partitions"),
            require_zero_disallowed_empty_partitions=_as_bool(auto_review_policy, "require_zero_disallowed_empty_partitions"),
            require_zero_duplicate_partitions=_as_bool(auto_review_policy, "require_zero_duplicate_partitions"),
            require_zero_missing_data_files=_as_bool(auto_review_policy, "require_zero_missing_data_files"),
            require_zero_missing_metadata_files=_as_bool(auto_review_policy, "require_zero_missing_metadata_files"),
            require_zero_missing_required_fields=_as_bool(auto_review_policy, "require_zero_missing_required_fields"),
            require_compact_qa_all_ok=_as_bool(auto_review_policy, "require_compact_qa_all_ok"),
            require_zero_failed_backlog=_as_bool(auto_review_policy, "require_zero_failed_backlog"),
            require_ready_for_promotion=_as_bool(auto_review_policy, "require_ready_for_promotion"),
            block_non_identical_drive_collision=_as_bool(auto_review_policy, "block_non_identical_drive_collision"),
        ),
        promotion_policy=Dwh4PromotionPolicy(
            auto_prepare=_as_bool(promotion_policy, "auto_prepare"),
            auto_promote=_as_bool(promotion_policy, "auto_promote"),
            require_final_human_confirmation=_as_bool(promotion_policy, "require_final_human_confirmation"),
            allow_reviewed_bucket_kinds=_as_str_tuple(promotion_policy, "allow_reviewed_bucket_kinds"),
        ),
        human_intervention_policy=Dwh4HumanInterventionPolicy(
            only_token_and_final_promotion=_as_bool(human_policy, "only_token_and_final_promotion"),
            do_not_pause_for_scope_review=_as_bool(human_policy, "do_not_pause_for_scope_review"),
            do_not_pause_for_ingest_review=_as_bool(human_policy, "do_not_pause_for_ingest_review"),
            do_not_pause_for_compact_review=_as_bool(human_policy, "do_not_pause_for_compact_review"),
            on_any_review_failure=_require_str(human_policy, "on_any_review_failure"),
        ),
        drive_inventory_policy=_parse_drive_inventory_policy(drive_inventory_policy),
        incremental_policy=_parse_incremental_policy(incremental_policy),
        drive_mutation_policy=_parse_drive_mutation_policy(drive_mutation_policy),
        secret_like_key_paths=_secret_like_key_paths(payload),
    )


def load_dwh4_tushare_task(path: str | Path) -> Dwh4TushareTask:
    """Load a DWH4 Tushare task JSON from disk."""
    task_path = Path(path)
    try:
        payload = json.loads(task_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid DWH4 task JSON at {task_path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("DWH4 task JSON must be a top-level object")
    return task_from_dict(payload)


def runtime_token_present(env: Mapping[str, str] | None = None) -> bool:
    """Return whether TUSHARE_TOKEN is present without exposing its value."""
    source = os.environ if env is None else env
    return bool(source.get("TUSHARE_TOKEN"))


def _issue(code: str, field: str, message: str, severity: str = "ERROR") -> TaskValidationIssue:
    return TaskValidationIssue(severity=severity, code=code, field=field, message=message)


def _valid_yyyymmdd(value: str) -> bool:
    if not re.fullmatch(r"\d{8}", value):
        return False
    try:
        return datetime.strptime(value, _DATE_FORMAT).strftime(_DATE_FORMAT) == value
    except ValueError:
        return False


def _path_safe_segment(value: str) -> bool:
    if value in {"", ".", ".."}:
        return False
    if "/" in value or "\\" in value or ":" in value:
        return False
    if ".." in value.split("."):
        return False
    return bool(_PATH_SAFE_SEGMENT_RE.fullmatch(value))


def _safe_relative_manifest_path(value: str, dataset_version: str) -> bool:
    if not value or "\\" in value or ":" in value:
        return False
    path = PurePosixPath(value)
    if path.is_absolute():
        return False
    parts = path.parts
    if any(part in {"", ".", ".."} for part in parts):
        return False
    if len(parts) < 5:
        return False
    return parts[:4] == ("catalog", "active", "tushare", dataset_version)


def _validate_dwh41_policy(task: Dwh4TushareTask) -> list[TaskValidationIssue]:
    issues: list[TaskValidationIssue] = []
    incremental = task.incremental_policy
    mutation = task.drive_mutation_policy
    inventory = task.drive_inventory_policy
    if incremental is not None and incremental.enabled:
        if inventory is None or not inventory.enabled:
            issues.append(_issue("DWH41_DRIVE_INVENTORY_REQUIRED", "drive_inventory_policy.enabled", "DWH4.1 incremental mode requires enabled drive inventory policy"))
        if incremental.mode != SUPPORTED_INCREMENTAL_MODE:
            issues.append(_issue("DWH41_INCREMENTAL_MODE_UNSUPPORTED", "incremental_policy.mode", f"expected {SUPPORTED_INCREMENTAL_MODE!r}"))
        if incremental.target_end_date_policy != SUPPORTED_TARGET_END_DATE_POLICY:
            issues.append(_issue("DWH41_TARGET_END_DATE_POLICY_UNSUPPORTED", "incremental_policy.target_end_date_policy", f"expected {SUPPORTED_TARGET_END_DATE_POLICY!r}"))
        if incremental.as_of_date != "today" and not _valid_yyyymmdd(incremental.as_of_date):
            issues.append(_issue("DWH41_AS_OF_DATE_INVALID", "incremental_policy.as_of_date", "as_of_date must be 'today' or YYYYMMDD"))
        stable = incremental.stable_latest_policy
        unknown_stable_apis = sorted((set(stable.range_apis) | set(stable.snapshot_apis)) - set(task.api_names))
        for api_name in unknown_stable_apis:
            issues.append(_issue("DWH41_STABLE_LATEST_API_NOT_IN_TASK", "incremental_policy.stable_latest_policy", f"{api_name} is not listed in api_names"))
        if stable.enabled and stable.range_bucket != "window=latest":
            issues.append(_issue("DWH41_RANGE_BUCKET_UNSUPPORTED", "incremental_policy.stable_latest_policy.range_bucket", "range_bucket must be window=latest"))
        if stable.enabled and stable.snapshot_bucket != "snapshot=latest":
            issues.append(_issue("DWH41_SNAPSHOT_BUCKET_UNSUPPORTED", "incremental_policy.stable_latest_policy.snapshot_bucket", "snapshot_bucket must be snapshot=latest"))
        active = incremental.active_manifest_policy
        if active.enabled and not _safe_relative_manifest_path(active.active_manifest_path, task.dataset_version):
            issues.append(_issue("DWH41_ACTIVE_MANIFEST_PATH_INVALID", "incremental_policy.active_manifest_policy.active_manifest_path", "active manifest path must be relative under catalog/active/tushare/<dataset_version>/"))
        if mutation is None:
            issues.append(_issue("DWH41_DRIVE_MUTATION_POLICY_REQUIRED", "drive_mutation_policy", "DWH4.1 incremental mode requires drive mutation policy"))
    if mutation is not None:
        if mutation.allow_delete:
            issues.append(_issue("DWH41_DELETE_NOT_ALLOWED", "drive_mutation_policy.allow_delete", "DWH4.1 normal promotion must not allow Drive delete"))
        if mutation.allow_verified_replace and not mutation.require_final_confirmation_for_replace:
            issues.append(_issue("DWH41_REPLACE_CONFIRMATION_REQUIRED", "drive_mutation_policy.require_final_confirmation_for_replace", "verified replacement requires final human confirmation"))
        if not mutation.generate_delete_request_only:
            issues.append(_issue("DWH41_DELETE_REQUEST_ONLY_REQUIRED", "drive_mutation_policy.generate_delete_request_only", "delete handling must generate request artifacts only"))
    return issues


def _is_drive_like(path: Path) -> bool:
    text = str(path).replace("\\", "/").lower()
    return any(marker in text for marker in _DRIVE_MARKERS)


def _symbol_count(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def validate_dwh4_tushare_task(
    task: Dwh4TushareTask,
    *,
    registry_path: str | Path | None = None,
    check_drive_root: bool = False,
    check_runtime_token: bool = False,
    env: Mapping[str, str] | None = None,
) -> list[TaskValidationIssue]:
    """Validate a task sheet without running acquisition, compact, or promotion."""
    issues: list[TaskValidationIssue] = []
    for path in task.secret_like_key_paths:
        issues.append(_issue("SECRET_LIKE_KEY_REJECTED", path, "task keys must not contain secret-like names"))
    if task.workflow_mode != SUPPORTED_WORKFLOW_MODE:
        issues.append(_issue("WORKFLOW_MODE_UNSUPPORTED", "workflow_mode", f"expected {SUPPORTED_WORKFLOW_MODE!r}"))
    if task.provider != SUPPORTED_PROVIDER:
        issues.append(_issue("PROVIDER_UNSUPPORTED", "provider", "DWH4 task route currently supports only tushare"))
    if not task.execution_repo.exists():
        issues.append(_issue("EXECUTION_REPO_MISSING", "execution_repo", f"execution_repo not found: {task.execution_repo}"))
    elif not (task.execution_repo / "src").exists():
        issues.append(_issue("EXECUTION_REPO_SRC_MISSING", "execution_repo", f"src not found under execution_repo: {task.execution_repo}"))
    if not task.symbols_file.exists():
        issues.append(_issue("SYMBOLS_FILE_MISSING", "symbols_file", f"symbols_file not found: {task.symbols_file}"))
    elif task.expected_symbol_count is not None:
        if task.expected_symbol_count <= 0:
            issues.append(_issue("EXPECTED_SYMBOL_COUNT_INVALID", "expected_symbol_count", "expected_symbol_count must be positive"))
        else:
            actual = _symbol_count(task.symbols_file)
            if actual != task.expected_symbol_count:
                issues.append(_issue("EXPECTED_SYMBOL_COUNT_MISMATCH", "expected_symbol_count", f"expected {task.expected_symbol_count} symbols, found {actual}"))
    if not _valid_yyyymmdd(task.start_date):
        issues.append(_issue("DATE_FORMAT_INVALID", "start_date", "start_date must be YYYYMMDD"))
    if not _valid_yyyymmdd(task.end_date):
        issues.append(_issue("DATE_FORMAT_INVALID", "end_date", "end_date must be YYYYMMDD"))
    if _valid_yyyymmdd(task.start_date) and _valid_yyyymmdd(task.end_date) and task.start_date > task.end_date:
        issues.append(_issue("DATE_RANGE_INVALID", "start_date", "start_date must be <= end_date"))
    if not _path_safe_segment(task.dataset_version):
        issues.append(_issue("DATASET_VERSION_INVALID", "dataset_version", "dataset_version must be a path-safe segment"))
    if not _path_safe_segment(task.promotion_name):
        issues.append(_issue("PROMOTION_NAME_INVALID", "promotion_name", "promotion_name must be a path-safe segment"))
    if _is_drive_like(task.output_root):
        issues.append(_issue("OUTPUT_ROOT_DRIVE_LIKE", "output_root", "output_root must be local staging, not Google Drive"))
    if check_drive_root and not task.drive_dwh_root.exists():
        issues.append(_issue("DRIVE_DWH_ROOT_MISSING", "drive_dwh_root", f"drive_dwh_root not found: {task.drive_dwh_root}"))
    if not task.api_names:
        issues.append(_issue("API_NAMES_EMPTY", "api_names", "api_names must not be empty"))
    registry_issues = [issue for issue in issues if issue.code in {"EXECUTION_REPO_MISSING", "EXECUTION_REPO_SRC_MISSING"}]
    if not registry_issues:
        effective_registry_path = Path(registry_path) if registry_path is not None else task.execution_repo / "configs" / "tushare" / "source_registry.yaml"
        try:
            specs = source_specs_by_api(effective_registry_path)
        except Exception as exc:  # noqa: BLE001 - validation should return structured issues instead of raising.
            issues.append(_issue("REGISTRY_LOAD_FAILED", "configs/tushare/source_registry.yaml", str(exc)))
        else:
            for api_name in task.api_names:
                spec = specs.get(api_name)
                if spec is None:
                    issues.append(_issue("API_NAME_UNKNOWN", "api_names", f"unknown Tushare API: {api_name}"))
                elif not spec.production_enabled and not task.allow_candidate_sources:
                    issues.append(_issue("API_NOT_PRODUCTION_ENABLED", "api_names", f"{api_name} is not production-enabled"))
    issues.extend(_validate_dwh41_policy(task))
    if check_runtime_token and not runtime_token_present(env):
        issues.append(_issue("TOKEN_NOT_PRESENT", "runtime", "TUSHARE_TOKEN is not present in the runtime environment", severity="WARNING"))
    return issues


def assert_dwh4_tushare_task_valid(task: Dwh4TushareTask, **kwargs: Any) -> None:
    """Raise ValueError when validation returns any ERROR issues."""
    issues = validate_dwh4_tushare_task(task, **kwargs)
    errors = [issue for issue in issues if issue.severity == "ERROR"]
    if errors:
        detail = "; ".join(f"{issue.code}:{issue.field}" for issue in errors)
        raise ValueError(f"DWH4 Tushare task validation failed: {detail}")
