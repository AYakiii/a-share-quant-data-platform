"""Notebook-facing adapter for DWH4 Tushare task JSON parameters.

The notebook remains a manual console. This module only maps the shared task
sheet into the variable names used by the notebook cells; it does not call
Tushare, compact data, promote, audit, or persist runtime secrets.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from qsys.workflows.tushare_dwh4_commands import default_package_root
from qsys.workflows.tushare_dwh4_task import Dwh4TushareTask, load_dwh4_tushare_task, runtime_token_present


@dataclass(frozen=True)
class Dwh4NotebookParameters:
    """Notebook globals derived from the shared DWH4 task sheet."""

    task: Dwh4TushareTask
    runtime_token_present: bool = False

    @property
    def compact_parent(self) -> Path:
        """Return the absolute compact package parent used by notebook cells."""
        return self.task.execution_repo / default_package_root(self.task).parent

    @property
    def package_root(self) -> Path:
        """Return the absolute compact package root for this task promotion."""
        return self.task.execution_repo / default_package_root(self.task)

    def incremental_policy_summary(self) -> dict[str, object]:
        """Return token-free DWH4.1 incremental policy fields for display."""
        policy = self.task.incremental_policy
        if policy is None:
            return {
                "enabled": False,
                "mode": "",
                "target_end_date_policy": "",
                "as_of_date": "",
                "data_lag_trading_days": None,
                "open_year_replace_current_year_bucket": False,
                "open_year_freeze_closed_years": False,
                "open_year_overlap_trading_days": None,
                "stable_latest_range_apis": [],
                "stable_latest_snapshot_apis": [],
                "active_manifest_path": "",
            }
        return {
            "enabled": policy.enabled,
            "mode": policy.mode,
            "target_end_date_policy": policy.target_end_date_policy,
            "as_of_date": policy.as_of_date,
            "data_lag_trading_days": policy.data_lag_trading_days,
            "open_year_replace_current_year_bucket": policy.open_year_policy.replace_current_year_bucket,
            "open_year_freeze_closed_years": policy.open_year_policy.freeze_closed_years,
            "open_year_overlap_trading_days": policy.open_year_policy.overlap_trading_days,
            "stable_latest_range_apis": list(policy.stable_latest_policy.range_apis),
            "stable_latest_snapshot_apis": list(policy.stable_latest_policy.snapshot_apis),
            "active_manifest_path": policy.active_manifest_policy.active_manifest_path,
        }

    def drive_policy_summary(self) -> dict[str, object]:
        """Return token-free DWH4.1 Drive inventory and mutation policy fields."""
        inventory = self.task.drive_inventory_policy
        mutation = self.task.drive_mutation_policy
        return {
            "drive_inventory_enabled": bool(inventory.enabled) if inventory is not None else False,
            "drive_inventory_compute_sha256": bool(inventory.compute_sha256) if inventory is not None else False,
            "drive_inventory_read_parquet_metadata": bool(inventory.read_parquet_metadata) if inventory is not None else False,
            "allow_delete": bool(mutation.allow_delete) if mutation is not None else False,
            "allow_verified_replace": bool(mutation.allow_verified_replace) if mutation is not None else False,
            "require_final_confirmation_for_replace": bool(mutation.require_final_confirmation_for_replace) if mutation is not None else False,
            "generate_delete_request_only": bool(mutation.generate_delete_request_only) if mutation is not None else False,
        }

    def as_notebook_globals(self) -> dict[str, object]:
        """Return notebook variable names matching the existing DWH4 console.

        ``API_NAMES`` intentionally remains a comma-separated string because
        existing notebook command cells interpolate it directly into CLI args.
        ``API_NAME_LIST`` is also provided for inspection and widgets.
        """
        task = self.task
        incremental_summary = self.incremental_policy_summary()
        drive_summary = self.drive_policy_summary()
        return {
            "REPO_ROOT": task.execution_repo,
            "EXECUTION_REPO": task.execution_repo,
            "OPS_WORKSPACE": task.ops_workspace,
            "PROVIDER": task.provider,
            "SYMBOLS_FILE": task.symbols_file,
            "UNIVERSE_NAME": task.universe_name,
            "EXPECTED_SYMBOL_COUNT": task.expected_symbol_count,
            "DATASET_VERSION": task.dataset_version,
            "START_DATE": task.start_date,
            "END_DATE": task.end_date,
            "API_NAMES": ",".join(task.api_names),
            "API_NAME_LIST": list(task.api_names),
            "ALLOW_CANDIDATE_SOURCES": task.allow_candidate_sources,
            "WORK_NAME": task.work_name,
            "OUTPUT_ROOT": task.output_root,
            "DRIVE_DWH_ROOT": task.drive_dwh_root,
            "PROMOTION_NAME": task.promotion_name,
            "COMPACT_PARENT": self.compact_parent,
            "PACKAGE_ROOT": self.package_root,
            "MAX_WORKERS": task.execution.max_workers,
            "REQUEST_SLEEP": task.execution.request_sleep,
            "REQUEST_JITTER": task.execution.request_jitter,
            "RETRY": task.execution.retry,
            "HEARTBEAT_SEC": task.execution.heartbeat_sec,
            "RESUME": task.execution.resume,
            "RUNTIME_TOKEN_PRESENT": self.runtime_token_present,
            "DWH41_INCREMENTAL_POLICY": incremental_summary,
            "DWH41_DRIVE_POLICY": drive_summary,
            "DWH41_INCREMENTAL_ENABLED": incremental_summary["enabled"],
            "DWH41_INCREMENTAL_MODE": incremental_summary["mode"],
            "DWH41_TARGET_END_DATE_POLICY": incremental_summary["target_end_date_policy"],
            "DWH41_ACTIVE_MANIFEST_PATH": incremental_summary["active_manifest_path"],
            "DWH41_ALLOW_DELETE": drive_summary["allow_delete"],
            "DWH41_ALLOW_VERIFIED_REPLACE": drive_summary["allow_verified_replace"],
        }

    def as_summary(self) -> dict[str, object]:
        """Return a small token-free summary suitable for notebook display."""
        task = self.task
        incremental_summary = self.incremental_policy_summary()
        drive_summary = self.drive_policy_summary()
        return {
            "workflow_name": task.workflow_name,
            "provider": task.provider,
            "dataset_version": task.dataset_version,
            "date_range": f"{task.start_date}..{task.end_date}",
            "api_count": len(task.api_names),
            "output_root": str(task.output_root),
            "drive_dwh_root": str(task.drive_dwh_root),
            "promotion_name": task.promotion_name,
            "runtime_token_present": self.runtime_token_present,
            "dwh41_incremental_enabled": incremental_summary["enabled"],
            "dwh41_incremental_mode": incremental_summary["mode"],
            "dwh41_active_manifest_path": incremental_summary["active_manifest_path"],
            "dwh41_allow_delete": drive_summary["allow_delete"],
            "dwh41_allow_verified_replace": drive_summary["allow_verified_replace"],
        }


def notebook_parameters_from_task(task: Dwh4TushareTask, *, env: Mapping[str, str] | None = None) -> Dwh4NotebookParameters:
    """Build notebook parameters from an already-loaded task."""
    return Dwh4NotebookParameters(task=task, runtime_token_present=runtime_token_present(env))


def load_dwh4_tushare_notebook_parameters(path: str | Path, *, env: Mapping[str, str] | None = None) -> Dwh4NotebookParameters:
    """Load shared task JSON and expose notebook-facing parameters."""
    return notebook_parameters_from_task(load_dwh4_tushare_task(path), env=env)


def load_dwh4_tushare_notebook_globals(path: str | Path, *, env: Mapping[str, str] | None = None) -> dict[str, object]:
    """Load shared task JSON and return a ``globals().update(...)`` mapping."""
    return load_dwh4_tushare_notebook_parameters(path, env=env).as_notebook_globals()
