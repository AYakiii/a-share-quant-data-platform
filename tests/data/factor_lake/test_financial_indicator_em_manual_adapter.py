from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.factor_lake.raw_ingest import (
    API_POLICY_METADATA,
    COVERAGE_API_SPECS,
    _build_raw_partition,
    _financial_indicator_em_symbol,
    _params_for_mode,
    run_raw_coverage_ingest,
)
from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY, SOURCE_CAPABILITY_REGISTRY


def test_financial_indicator_em_registered_manual_only_and_legacy_preserved() -> None:
    financial_specs = COVERAGE_API_SPECS["financial_fundamental"]
    api_names = {spec["api_name"] for spec in financial_specs}
    capability_by_api = {spec.api_name: spec for spec in SOURCE_CAPABILITY_REGISTRY}
    source_case_names = {case.api_name: case for case in FACTOR_SOURCE_REGISTRY}

    assert "stock_financial_analysis_indicator_em" in api_names
    assert "stock_financial_analysis_indicator_em" in capability_by_api
    assert "stock_financial_analysis_indicator_em" in source_case_names
    capability = capability_by_api["stock_financial_analysis_indicator_em"]
    assert capability.adapter_function == "stock_financial_analysis_indicator_em"
    assert capability.date_field == "REPORT_DATE"
    assert capability.symbol_field == "SECUCODE/SECURITY_CODE"
    assert capability.report_period_field == "REPORT_DATE"
    assert capability.announcement_date_field == "NOTICE_DATE"
    assert capability.normalized_target == ""
    assert capability.factor_family_target == ""
    assert capability.lookahead_risk_fields == "REPORT_DATE must not be used as PIT availability time; use NOTICE_DATE"
    policy = API_POLICY_METADATA[("financial_fundamental", "stock_financial_analysis_indicator_em")]
    assert policy["enabled"] is False
    assert policy["default_enabled"] is False
    assert policy["manual_review_required"] is True
    assert policy["acquisition_mode"] == "manual_selected_only"
    assert policy["priority_tier"] == "P1"
    assert policy["data_theme"] == "financial_analysis_indicator"
    assert policy["disabled_category"] == "recovered_financial_fundamental_source"
    assert source_case_names["stock_financial_analysis_indicator_em"].enabled is False

    assert "stock_financial_analysis_indicator" in api_names
    legacy_policy = API_POLICY_METADATA[("financial_fundamental", "stock_financial_analysis_indicator")]
    assert legacy_policy["enabled"] is False
    assert legacy_policy["default_enabled"] is False
    assert legacy_policy["manual_review_required"] is True
    assert legacy_policy["disabled_category"] == "empty_review_source"
    assert legacy_policy["review_category"] == "parameter_schema_review"
    assert legacy_policy["legacy_policy"] == "legacy_start_year_required"
    assert legacy_policy["acquisition_mode"] == "legacy_direct_manual_only"
    assert "start_year=1900" in str(legacy_policy["disabled_reason"])
    assert "valid start_year such as 2020" in str(legacy_policy["disabled_reason"])


def test_financial_indicator_em_task_planning_produces_two_indicator_modes() -> None:
    tasks = _params_for_mode(
        "financial_indicator_em",
        symbols=["000001"],
        index_symbols=[],
        report_dates=[],
        trade_dates=[],
        industry_names=[],
        concept_names=[],
        start_date="20200101",
        end_date="20200101",
    )

    assert tasks == [
        {"symbol": "000001", "indicator": "按报告期"},
        {"symbol": "000001", "indicator": "按单季度"},
    ]


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("000001", "000001.SZ"),
        ("300750", "300750.SZ"),
        ("600000", "600000.SH"),
        ("000001.SZ", "000001.SZ"),
        ("600000.SH", "600000.SH"),
    ],
)
def test_financial_indicator_em_symbol_conversion(symbol: str, expected: str) -> None:
    assert _financial_indicator_em_symbol(symbol) == expected


def test_financial_indicator_em_symbol_conversion_rejects_unverified_prefixes() -> None:
    with pytest.raises(ValueError, match="unsupported EM financial indicator symbol prefix"):
        _financial_indicator_em_symbol("830000")


def test_financial_indicator_em_symbol_conversion_rejects_malformed_unsuffixed_input() -> None:
    with pytest.raises(ValueError, match="expected exactly six digits"):
        _financial_indicator_em_symbol("abc000001")


def test_financial_indicator_em_partitions_keep_logical_symbol_and_stable_indicator_labels() -> None:
    report = _build_raw_partition(
        "financial_fundamental",
        "stock_financial_analysis_indicator_em",
        {"symbol": "000001", "indicator": "按报告期"},
        {"symbol": "000001.SZ", "indicator": "按报告期"},
    )
    quarter = _build_raw_partition(
        "financial_fundamental",
        "stock_financial_analysis_indicator_em",
        {"symbol": "000001.SZ", "indicator": "按单季度"},
        {"symbol": "000001.SZ", "indicator": "按单季度"},
    )

    assert report == {"symbol": "000001", "indicator": "report_period"}
    assert quarter == {"symbol": "000001", "indicator": "single_quarter"}
    assert report != quarter


def test_financial_indicator_em_default_skips_but_explicit_selection_runs_without_unrelated_disabled(tmp_path: Path) -> None:
    calls: list[dict[str, str]] = []

    def stock_financial_analysis_indicator_em(symbol: str, indicator: str) -> pd.DataFrame:
        calls.append({"symbol": symbol, "indicator": indicator})
        return pd.DataFrame({"symbol": [symbol], "indicator": [indicator]})

    def stock_financial_analysis_indicator(symbol: str) -> pd.DataFrame:  # noqa: ARG001
        raise AssertionError("legacy disabled source should not run merely because it is selected")

    default_out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "default"),
        families=["financial_fundamental"],
        symbols=["000001"],
        start_date="20100101",
        end_date="20100101",
        adapter_map={"stock_financial_analysis_indicator_em": stock_financial_analysis_indicator_em},
        max_workers=1,
    )

    em_default_rows = [row for row in default_out["rows"] if row["api_name"] == "stock_financial_analysis_indicator_em"]
    assert len(em_default_rows) == 2
    assert {row["status"] for row in em_default_rows} == {"skipped"}
    assert {row["error_type"] for row in em_default_rows} == {"default_disabled"}
    assert calls == []

    selected_out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "selected"),
        families=["financial_fundamental"],
        symbols=["000001"],
        start_date="20100101",
        end_date="20100101",
        selected_api_names=["stock_financial_analysis_indicator_em", "stock_financial_analysis_indicator"],
        adapter_map={
            "stock_financial_analysis_indicator_em": stock_financial_analysis_indicator_em,
            "stock_financial_analysis_indicator": stock_financial_analysis_indicator,
        },
        max_workers=1,
    )

    em_rows = [row for row in selected_out["rows"] if row["api_name"] == "stock_financial_analysis_indicator_em"]
    legacy_rows = [row for row in selected_out["rows"] if row["api_name"] == "stock_financial_analysis_indicator"]
    assert len(em_rows) == 2
    assert {row["status"] for row in em_rows} == {"success"}
    assert len(legacy_rows) == 1
    assert legacy_rows[0]["status"] == "skipped"
    assert calls == [
        {"symbol": "000001.SZ", "indicator": "按报告期"},
        {"symbol": "000001.SZ", "indicator": "按单季度"},
    ]


def test_financial_indicator_em_synthetic_ingest_writes_raw_and_metadata_for_both_modes(tmp_path: Path) -> None:
    def stock_financial_analysis_indicator_em(symbol: str, indicator: str) -> pd.DataFrame:
        return pd.DataFrame({"actual_symbol": [symbol], "actual_indicator": [indicator], "value": [1.0]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001"],
        selected_api_names=["stock_financial_analysis_indicator_em"],
        adapter_map={"stock_financial_analysis_indicator_em": stock_financial_analysis_indicator_em},
        max_workers=1,
    )

    rows = sorted(out["rows"], key=lambda row: json.loads(row["partition_json"])["indicator"])
    assert len(rows) == 2
    assert {row["status"] for row in rows} == {"success"}
    assert {row["original_symbol"] for row in rows} == {"000001"}
    assert {row["akshare_symbol"] for row in rows} == {"000001.SZ"}
    partitions = [json.loads(row["partition_json"]) for row in rows]
    assert partitions == [
        {"indicator": "report_period", "symbol": "000001"},
        {"indicator": "single_quarter", "symbol": "000001"},
    ]
    assert rows[0]["output_path"] != rows[1]["output_path"]
    assert all("symbol=000001" in row["output_path"] for row in rows)
    assert {"indicator=report_period", "indicator=single_quarter"} == {
        "indicator=report_period" if "indicator=report_period" in row["output_path"] else "indicator=single_quarter"
        for row in rows
    }

    for row in rows:
        raw = pd.read_parquet(row["output_path"])
        assert raw.shape == (1, 3)
        metadata = json.loads(Path(row["metadata_path"]).read_text(encoding="utf-8"))
        assert metadata["original_symbol"] == "000001"
        assert metadata["akshare_symbol"] == "000001.SZ"
        assert metadata["params"]["symbol"] == "000001.SZ"
        assert metadata["params"]["indicator"] in {"按报告期", "按单季度"}
        assert metadata["status"] == "success"
