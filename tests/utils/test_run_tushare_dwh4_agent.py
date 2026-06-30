from __future__ import annotations

import csv
import json
from pathlib import Path

from qsys.utils import run_tushare_dwh4_agent as agent_cli
from qsys.utils.run_tushare_dwh4_agent import main
from qsys.workflows.tushare_dwh4_executor import RunnerResult
from qsys.workflows.tushare_dwh4_orchestrator import DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE


def _registry_row(api_name: str) -> dict[str, object]:
    return {
        "source_family": "fam",
        "api_name": api_name,
        "fields": ["ts_code", "trade_date"],
        "query_mode": "by_trade_date",
        "calendar_mode": "trading_days",
        "partition_key": "trade_date",
        "primary_key": ["ts_code", "trade_date"],
        "universe_filter_mode": "ts_code",
        "empty_result_allowed": False,
        "compact_bucket": "year_from_trade_date",
        "status": "approved",
        "production_enabled": True,
    }


def _write_registry(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"sources": [_registry_row(api) for api in ("daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange")]}),
        encoding="utf-8",
    )


def _add_dwh41_policies(payload: dict[str, object]) -> dict[str, object]:
    dataset_version = str(payload["dataset_version"])
    payload["drive_inventory_policy"] = {
        "enabled": True,
        "scan_raw_tushare": True,
        "read_parquet_metadata": True,
        "compute_sha256": True,
        "fail_on_unreadable_existing_asset": True,
    }
    payload["incremental_policy"] = {
        "enabled": True,
        "mode": "drive_aware_incremental",
        "target_end_date_policy": "latest_open_trading_day",
        "as_of_date": "today",
        "data_lag_trading_days": 1,
        "open_year_policy": {
            "enabled": True,
            "replace_current_year_bucket": True,
            "freeze_closed_years": True,
            "overlap_trading_days": 3,
            "clip_overlap_to_open_year": True,
            "block_on_non_identical_key_conflict": True,
            "allow_identical_overlap_collapse": True,
        },
        "stable_latest_policy": {
            "enabled": True,
            "range_apis": ["trade_cal", "namechange"],
            "snapshot_apis": ["stock_basic"],
            "range_bucket": "window=latest",
            "snapshot_bucket": "snapshot=latest",
        },
        "active_manifest_policy": {
            "enabled": True,
            "write_active_manifest": True,
            "active_manifest_path": f"catalog/active/tushare/{dataset_version}/dwh4_tushare_active_manifest.json",
        },
    }
    payload["drive_mutation_policy"] = {
        "allow_delete": False,
        "allow_verified_replace": True,
        "require_final_confirmation_for_replace": True,
        "generate_delete_request_only": True,
        "backup_old_drive_assets_locally_before_replace": True,
    }
    return payload


def _task_json(tmp_path: Path, *, dwh41: bool = False) -> Path:
    execution_repo = tmp_path / "execution_repo"
    (execution_repo / "src").mkdir(parents=True, exist_ok=True)
    _write_registry(execution_repo / "configs" / "tushare" / "source_registry.yaml")
    symbols_file = execution_repo / "stock_universe_v1_symbols.txt"
    symbols_file.write_text("000001\n000002\n", encoding="utf-8")
    payload = {
        "workflow_name": "tushare_test",
        "workflow_mode": "dwh4_dual_entry_single_core",
        "execution_repo": str(execution_repo),
        "ops_workspace": str(tmp_path / "ops"),
        "provider": "tushare",
        "symbols_file": str(symbols_file),
        "universe_name": "stock_universe_v1",
        "expected_symbol_count": 2,
        "dataset_version": "v1_csi500_2021_2025_union",
        "start_date": "20220101",
        "end_date": "20260601",
        "api_names": ["daily_basic", "stk_limit", "suspend_d", "trade_cal", "stock_basic", "namechange"],
        "allow_candidate_sources": False,
        "work_name": "tushare_test",
        "output_root": str(tmp_path / "outputs" / "tushare_test"),
        "drive_dwh_root": str(tmp_path / "drive" / "a_share_quant_data"),
        "promotion_name": "tushare_test_compact",
        "execution": {
            "max_workers": 16,
            "request_sleep": 0.2,
            "request_jitter": 0.08,
            "retry": 4,
            "heartbeat_sec": 10,
            "resume": True,
        },
        "repo_policy": {
            "sync_policy": "manual_or_current",
            "require_clean_worktree": False,
            "record_git_commit": True,
        },
        "auto_review_policy": {
            "require_ingest_rough_check_pass": True,
            "require_all_api_rough_check_pass": True,
            "require_zero_bad_status_partitions": True,
            "require_zero_failed_partitions": True,
            "require_zero_disallowed_empty_partitions": True,
            "require_zero_duplicate_partitions": True,
            "require_zero_missing_data_files": True,
            "require_zero_missing_metadata_files": True,
            "require_zero_missing_required_fields": True,
            "require_compact_qa_all_ok": True,
            "require_zero_failed_backlog": True,
            "require_ready_for_promotion": True,
            "block_non_identical_drive_collision": True,
        },
        "promotion_policy": {
            "auto_prepare": True,
            "auto_promote": False,
            "require_final_human_confirmation": True,
            "allow_reviewed_bucket_kinds": ["snapshot"],
        },
        "human_intervention_policy": {
            "only_token_and_final_promotion": True,
            "do_not_pause_for_scope_review": True,
            "do_not_pause_for_ingest_review": True,
            "do_not_pause_for_compact_review": True,
            "on_any_review_failure": "stop_and_write_report",
        },
    }
    if dwh41:
        payload = _add_dwh41_policies(payload)
    task_path = tmp_path / "task.json"
    task_path.write_text(json.dumps(payload), encoding="utf-8")
    return task_path


def _run_root(tmp_path: Path) -> Path:
    return tmp_path / "ops" / "runs" / "tushare_test" / "run_001"


def _package_root(tmp_path: Path) -> Path:
    return tmp_path / "pkg"


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fieldnames or list(rows[0].keys() if rows else [])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)


def _write_ingest_artifacts_from_task_json(task_path: Path) -> None:
    payload = json.loads(task_path.read_text(encoding="utf-8"))
    output_root = Path(str(payload["output_root"]))
    art = output_root / "artifacts" / "tushare_raw_acquisition"
    art.mkdir(parents=True, exist_ok=True)
    summary = {
        "rough_check": "PASS",
        "planned_partitions": 12,
        "status_counts": {"ok": 12},
        "abnormal_counts": {
            "bad_status_partitions": 0,
            "failed_partitions": 0,
            "disallowed_empty_partitions": 0,
            "duplicate_partitions": 0,
            "missing_data_files": 0,
            "missing_metadata_files": 0,
            "required_contract_fields_missing": 0,
        },
    }
    (art / "operator_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    _write_csv(
        art / "operator_summary_by_api.csv",
        [{"api_name": api, "rough_check": "PASS"} for api in payload["api_names"]],
    )
    (art / "tushare_acquisition_manifest.json").write_text(json.dumps({"request_date_count": 2}), encoding="utf-8")


def _write_compact_artifacts_from_task_json(task_path: Path, pkg: Path, *, ready: bool = True, qa_ok: bool = True) -> None:
    payload = json.loads(task_path.read_text(encoding="utf-8"))
    pkg.mkdir(parents=True, exist_ok=True)
    asset = "raw/tushare/fam/api/v1/snapshot=20260601/data.parquet"
    manifest = {
        "promotion_name": payload["promotion_name"],
        "provider": payload["provider"],
        "dataset_version": payload["dataset_version"],
        "total_rows": 10,
        "failed_backlog_task_count": 0,
        "compact_assets": [
            {
                "relative_path": asset,
                "source_family": "fam",
                "api_name": "api",
                "bucket_kind": "snapshot",
                "bucket_value": "20260601",
                "rows": 10,
            }
        ],
    }
    (pkg / "compact_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    _write_csv(pkg / "compact_qa_report.csv", [{"relative_path": asset, "ok": qa_ok}])
    _write_csv(pkg / "raw_asset_inventory.csv", [{"relative_path": "source.parquet"}])
    _write_csv(pkg / "compact_source_lineage.csv", [{"compact_relative_path": asset}])
    (pkg / "_LOCAL_COMPACT_READY.txt").write_text("ready\n", encoding="utf-8")
    if ready:
        ready_payload = {
            "ready_for_promotion": True,
            "promotion_name": payload["promotion_name"],
            "provider": payload["provider"],
            "dataset_version": payload["dataset_version"],
            "planned_copy_new_count": 1,
            "planned_skip_identical_count": 0,
            "planned_block_non_identical_count": 0,
            "review_required_bucket_kinds": ["snapshot"],
        }
        (pkg / "READY_FOR_PROMOTION.json").write_text(json.dumps(ready_payload), encoding="utf-8")
        _write_csv(
            pkg / "drive_collision_plan.csv",
            [
                {
                    "source_family": "fam",
                    "api_name": "api",
                    "bucket_kind": "snapshot",
                    "bucket_value": "20260601",
                    "rows": 10,
                    "relative_path": asset,
                    "action": "copy_new",
                }
            ],
        )


def test_cli_writes_plan_only_artifacts_without_executing_commands(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path)
    assert main(["--task", str(task_path), "--stage", "run-to-prepare", "--run-id", "run_001"]) == 0
    root = _run_root(tmp_path)
    assert (root / "workflow_state.json").exists()
    assert (root / "planned_commands.json").exists()
    assert (root / "gate_decisions.json").exists()
    assert (root / "dwh4_agent_report.md").exists()
    assert not (root / "commands_executed.jsonl").exists()
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    planned = json.loads((root / "planned_commands.json").read_text(encoding="utf-8"))
    assert state["status"] == "READY_TO_RUN_TO_PREPARE"
    assert [row["stage"] for row in planned] == ["ingest", "prepare"]
    dumped = json.dumps({"state": state, "planned": planned})
    assert "secret-value" not in dumped


def test_cli_plan_only_records_dwh41_v2_stage_sequence_without_promote(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path, dwh41=True)
    assert main(["--task", str(task_path), "--stage", "run-to-prepare", "--run-id", "run_001", "--skip-review-artifact-scan"]) == 0
    root = _run_root(tmp_path)
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    planned = json.loads((root / "planned_commands.json").read_text(encoding="utf-8"))
    report = (root / "dwh4_agent_report.md").read_text(encoding="utf-8")
    assert state["run_to_prepare_v2"] is True
    assert state["stage_sequence"] == list(DWH41_RUN_TO_PREPARE_STAGE_SEQUENCE)
    assert state["planned_command_stages"] == ["ingest", "prepare"]
    assert state["promotion_executed"] is False
    assert state["drive_write_executed"] is False
    assert state["drive_delete_executed"] is False
    assert [row["stage"] for row in planned] == ["ingest", "prepare"]
    assert all("promote" not in row["argv"] for row in planned)
    assert "drive-inventory: PENDING" in report
    assert "final-review: PENDING" in report
    assert "Drive delete executed: no" in report


def test_cli_can_write_blocked_token_plan(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    assert main(["--task", str(task_path), "--stage", "run-to-prepare", "--run-id", "run_001"]) == 0
    state = json.loads((_run_root(tmp_path) / "workflow_state.json").read_text(encoding="utf-8"))
    planned = json.loads((_run_root(tmp_path) / "planned_commands.json").read_text(encoding="utf-8"))
    assert state["status"] == "BLOCKED_TOKEN"
    assert planned == []


def test_cli_allow_missing_token_for_plan_records_commands_without_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    assert main(["--task", str(task_path), "--stage", "run-to-prepare", "--run-id", "run_001", "--allow-missing-token-for-plan", "--skip-review-artifact-scan"]) == 0
    root = _run_root(tmp_path)
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    gates = json.loads((root / "gate_decisions.json").read_text(encoding="utf-8"))
    planned = json.loads((root / "planned_commands.json").read_text(encoding="utf-8"))
    assert state["status"] == "READY_TO_RUN_TO_PREPARE"
    assert state["token_present"] is False
    assert gates == {}
    assert [row["stage"] for row in planned] == ["ingest", "prepare"]


def test_cli_execute_requires_exact_confirmation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-run-to-prepare",
        ]
    )
    assert rc == 2
    assert not (_run_root(tmp_path) / "commands_executed.jsonl").exists()


def test_cli_execute_missing_token_writes_blocked_plan_without_commands(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-run-to-prepare",
            "--confirm-execute-run-to-prepare",
            "run_001",
            "--skip-review-artifact-scan",
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 2
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    planned = json.loads((root / "planned_commands.json").read_text(encoding="utf-8"))
    assert state["status"] == "BLOCKED_TOKEN"
    assert planned == []
    assert not (root / "commands_executed.jsonl").exists()


def test_cli_execute_run_to_prepare_uses_injected_runner_and_writes_records(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_runner(command, *, env):
        calls.append({"stage": command.stage, "argv": command.argv, "env": dict(env)})
        if command.stage == "ingest":
            _write_ingest_artifacts_from_task_json(task_path)
        if command.stage == "prepare":
            _write_compact_artifacts_from_task_json(task_path, _package_root(tmp_path))
        return RunnerResult(return_code=0, stdout=f"stdout:{command.stage}", stderr="")

    monkeypatch.setattr(agent_cli, "run_command_subprocess", fake_runner)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-run-to-prepare",
            "--confirm-execute-run-to-prepare",
            "run_001",
            "--skip-review-artifact-scan",
            "--package-root",
            str(_package_root(tmp_path)),
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 0
    assert [call["stage"] for call in calls] == ["ingest", "prepare"]
    lines = (root / "commands_executed.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    records = [json.loads(line) for line in lines]
    assert [record["stage"] for record in records] == ["ingest", "prepare"]
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    gates = json.loads((root / "gate_decisions.json").read_text(encoding="utf-8"))
    report = (root / "dwh4_agent_report.md").read_text(encoding="utf-8")
    final_review = (root / "final_promotion_review.md").read_text(encoding="utf-8")
    assert state["subprocess_executed"] is True
    assert state["run_to_prepare_complete"] is True
    assert state["gate_decision_stages"] == ["review-ingest", "review-compact", "review-promotion"]
    assert [gates[stage]["status"] for stage in state["gate_decision_stages"]] == ["PASS", "PASS", "PASS"]
    assert state["final_promotion_review_ready"] is True
    assert state["promotion_command"]["stage"] == "promote"
    assert "--confirm-promotion" in state["promotion_command"]["argv"]
    assert "Subprocess executed: yes" in report
    assert "DWH4 Final Promotion Review" in final_review
    assert "Promotion executed: no" in final_review
    dumped = "\n".join(lines) + json.dumps(state) + report + final_review
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_cli_execute_stops_prepare_when_ingest_gate_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_runner(command, *, env):
        calls.append({"stage": command.stage, "argv": command.argv, "env": dict(env)})
        return RunnerResult(return_code=0, stdout=f"stdout:{command.stage}", stderr="")

    monkeypatch.setattr(agent_cli, "run_command_subprocess", fake_runner)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-run-to-prepare",
            "--confirm-execute-run-to-prepare",
            "run_001",
            "--skip-review-artifact-scan",
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 1
    assert [call["stage"] for call in calls] == ["ingest"]
    lines = (root / "commands_executed.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    gates = json.loads((root / "gate_decisions.json").read_text(encoding="utf-8"))
    report = (root / "dwh4_agent_report.md").read_text(encoding="utf-8")
    assert state["executed_command_stages"] == ["ingest"]
    assert state["run_to_prepare_complete"] is False
    assert state["blocked_stage"] == "prepare"
    assert state["blocked_reason"] == "review-ingest"
    assert gates["review-ingest"]["status"] == "FAIL"
    assert "ARTIFACT_MISSING" in json.dumps(gates)
    assert "blocked_stage: prepare" in report
    dumped = "\n".join(lines) + json.dumps(state) + json.dumps(gates) + report
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_cli_execute_stops_when_compact_gate_fails_after_prepare(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-value")
    task_path = _task_json(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_runner(command, *, env):
        calls.append({"stage": command.stage, "argv": command.argv, "env": dict(env)})
        if command.stage == "ingest":
            _write_ingest_artifacts_from_task_json(task_path)
        return RunnerResult(return_code=0, stdout=f"stdout:{command.stage}", stderr="")

    monkeypatch.setattr(agent_cli, "run_command_subprocess", fake_runner)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-run-to-prepare",
            "--confirm-execute-run-to-prepare",
            "run_001",
            "--skip-review-artifact-scan",
            "--package-root",
            str(_package_root(tmp_path)),
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 1
    assert [call["stage"] for call in calls] == ["ingest", "prepare"]
    lines = (root / "commands_executed.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    state = json.loads((root / "workflow_state.json").read_text(encoding="utf-8"))
    gates = json.loads((root / "gate_decisions.json").read_text(encoding="utf-8"))
    report = (root / "dwh4_agent_report.md").read_text(encoding="utf-8")
    assert state["executed_command_stages"] == ["ingest", "prepare"]
    assert state["run_to_prepare_complete"] is False
    assert state["blocked_stage"] == "review-compact"
    assert state["blocked_reason"] == "review-compact"
    assert state["gate_decision_stages"] == ["review-ingest", "review-compact"]
    assert gates["review-ingest"]["status"] == "PASS"
    assert gates["review-compact"]["status"] == "FAIL"
    assert state["final_promotion_review_ready"] is False
    assert not (root / "final_promotion_review.md").exists()
    assert "ARTIFACT_MISSING" in json.dumps(gates)
    assert "blocked_stage: review-compact" in report
    dumped = "\n".join(lines) + json.dumps(state) + json.dumps(gates) + report
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_cli_execute_promotion_requires_confirmation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-promotion",
        ]
    )
    assert rc == 2
    assert not (_run_root(tmp_path) / "commands_executed.jsonl").exists()


def test_cli_execute_promotion_uses_injected_runner_when_review_passes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    pkg = _package_root(tmp_path)
    _write_compact_artifacts_from_task_json(task_path, pkg)
    calls: list[dict[str, object]] = []

    def fake_runner(command, *, env):
        calls.append({"stage": command.stage, "argv": command.argv, "env": dict(env)})
        return RunnerResult(return_code=0, stdout="secret-value", stderr="")

    monkeypatch.setattr(agent_cli, "run_command_subprocess", fake_runner)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-promotion",
            "--confirm-promotion",
            "tushare_test_compact",
            "--package-root",
            str(pkg),
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 0
    assert [call["stage"] for call in calls] == ["promote"]
    state = json.loads((root / "promotion_execution_state.json").read_text(encoding="utf-8"))
    report = (root / "promotion_execution_report.md").read_text(encoding="utf-8")
    lines = (root / "commands_executed.jsonl").read_text(encoding="utf-8").splitlines()
    assert state["status"] == "PROMOTION_EXECUTED"
    assert state["promotion_executed"] is True
    assert state["drive_write_executed"] is True
    assert json.loads(lines[0])["stage"] == "promote"
    assert json.loads(lines[0])["token_present"] is False
    assert "DWH4 Promotion Execution Report" in report
    dumped = json.dumps(state) + report + "\n".join(lines)
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_cli_execute_promotion_blocks_when_review_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(agent_cli, "run_command_subprocess", lambda command, *, env: calls.append({"stage": command.stage}) or RunnerResult(return_code=0))
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-promotion",
            "--confirm-promotion",
            "tushare_test_compact",
            "--package-root",
            str(_package_root(tmp_path)),
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 1
    assert calls == []
    state = json.loads((root / "promotion_execution_state.json").read_text(encoding="utf-8"))
    assert state["status"] == "PROMOTION_BLOCKED"
    assert state["blocked_reason"] == "review-promotion"
    assert not (root / "commands_executed.jsonl").exists()


def test_cli_execute_audit_uses_injected_runner_without_token_or_drive_write(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_runner(command, *, env):
        calls.append({"stage": command.stage, "argv": command.argv, "env": dict(env)})
        return RunnerResult(return_code=0, stdout="secret-value", stderr="")

    monkeypatch.setattr(agent_cli, "run_command_subprocess", fake_runner)
    rc = main(
        [
            "--task",
            str(task_path),
            "--stage",
            "run-to-prepare",
            "--run-id",
            "run_001",
            "--execute-audit",
        ]
    )
    root = _run_root(tmp_path)
    assert rc == 0
    assert [call["stage"] for call in calls] == ["audit"]
    state = json.loads((root / "audit_execution_state.json").read_text(encoding="utf-8"))
    report = (root / "audit_execution_report.md").read_text(encoding="utf-8")
    lines = (root / "commands_executed.jsonl").read_text(encoding="utf-8").splitlines()
    assert state["status"] == "AUDIT_EXECUTED"
    assert state["audit_executed"] is True
    assert state["drive_read_executed"] is True
    assert state["drive_write_executed"] is False
    assert json.loads(lines[0])["stage"] == "audit"
    assert json.loads(lines[0])["token_present"] is False
    assert "DWH4 Audit Execution Report" in report
    dumped = json.dumps(state) + report + "\n".join(lines)
    assert "secret-value" not in dumped
    assert "TUSHARE_TOKEN" not in dumped


def test_cli_execute_audit_cannot_combine_with_promotion(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    task_path = _task_json(tmp_path)
    rc = main(
        [
            "--task",
            str(task_path),
            "--run-id",
            "run_001",
            "--execute-audit",
            "--execute-promotion",
            "--confirm-promotion",
            "tushare_test_compact",
        ]
    )
    assert rc == 2
    assert not (_run_root(tmp_path) / "commands_executed.jsonl").exists()
