from __future__ import annotations

import csv
from pathlib import Path


PLAN_PATH = Path("config/factor_sources/p15p2_recovered_source_registration_plan.csv")

REQUIRED_COLUMNS = {
    "source_family",
    "api_name",
    "priority_tier",
    "data_theme",
    "status_after_probe",
    "last_probe_rows",
    "replaces_api",
    "replacement_status",
    "importance",
    "default_enabled",
    "manual_review_required",
    "acquisition_mode",
    "recommended_action",
    "notes",
}

RECOVERED_CANDIDATES = {
    "stock_market_fund_flow",
    "stock_fund_flow_concept",
    "stock_fund_flow_industry",
    "stock_hsgt_fund_flow_summary_em",
    "futures_inventory_em",
    "futures_comex_inventory",
    "futures_gfex_warehouse_receipt",
    "futures_shfe_warehouse_receipt",
    "futures_warehouse_receipt_czce",
}

FAILED_ORIGINALS = {
    "stock_individual_fund_flow_rank",
    "stock_sector_fund_flow_rank",
    "stock_sector_fund_flow_summary",
    "futures_inventory_99",
}


def _read_plan_rows() -> list[dict[str, str]]:
    with PLAN_PATH.open(newline="", encoding="utf-8") as file_obj:
        return list(csv.DictReader(file_obj))


def test_p15p2_recovered_source_registration_plan_has_required_columns() -> None:
    with PLAN_PATH.open(newline="", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        assert reader.fieldnames is not None
        assert REQUIRED_COLUMNS <= set(reader.fieldnames)


def test_p15p2_recovered_candidates_are_present_disabled_and_pending_review() -> None:
    rows_by_api = {row["api_name"]: row for row in _read_plan_rows()}

    assert RECOVERED_CANDIDATES <= set(rows_by_api)
    for api_name in RECOVERED_CANDIDATES:
        row = rows_by_api[api_name]
        assert row["status_after_probe"] == "success"
        assert row["replacement_status"] == "candidate_replacement"
        assert row["default_enabled"] == "false"
        assert row["manual_review_required"] == "true"
        assert row["recommended_action"] == "schema_review_then_adapter_integration"


def test_original_failed_apis_are_disabled_deferred_and_not_faked_as_success() -> None:
    rows_by_api = {row["api_name"]: row for row in _read_plan_rows()}

    assert FAILED_ORIGINALS <= set(rows_by_api)
    for api_name in FAILED_ORIGINALS:
        row = rows_by_api[api_name]
        assert row["status_after_probe"] == "failed"
        assert row["replacement_status"] == "replaced_or_deferred"
        assert row["default_enabled"] == "false"
        assert row["manual_review_required"] == "true"
        assert row["recommended_action"] == "do_not_integrate_original_api_until_upstream_behavior_is_revalidated"


def test_stock_gdfx_holding_analyse_is_manual_long_recovery_only() -> None:
    rows_by_api = {row["api_name"]: row for row in _read_plan_rows()}
    row = rows_by_api["stock_gdfx_holding_analyse_em"]

    assert row["priority_tier"] == "P1"
    assert row["data_theme"] == "shareholder_holding_analysis"
    assert row["status_after_probe"] == "success"
    assert row["last_probe_rows"] == "123880"
    assert row["default_enabled"] == "false"
    assert row["manual_review_required"] == "true"
    assert row["acquisition_mode"] == "long_recovery_run"
    assert row["recommended_action"] == "manual_long_run_only"
