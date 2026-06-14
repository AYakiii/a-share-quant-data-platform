from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.akshare_raw_ingest import run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_stock_jgdy_detail_none_like_page_failure_is_not_downgraded_to_empty(tmp_path):
    class _FakeResponse:
        def json(self) -> dict:
            return {"result": None}

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        return _FakeResponse()

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        report_dates=["20241211"],
        selected_api_names=["stock_jgdy_detail_em"],
        adapter_map={
            "__stock_jgdy_detail_em_request_get__": fake_get,
            "__stock_jgdy_detail_em_config__": {"retry_attempts": 1, "retry_sleep_sec": 0.0, "request_sleep_sec": 0.0},
        },
        include_disabled=True,
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "failed"
    assert row["error_type"] == "JgdyDetailPageFailure"
    assert "failed_pages" in row["error_message"]
    assert "checkpoint_dir" in row["error_message"]


def test_stock_jgdy_detail_default_policy_is_high_importance_manual_review(tmp_path):
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        symbols=["000001"],
        report_dates=["20241211"],
        max_workers=1,
    )

    row = [r for r in out["rows"] if r["api_name"] == "stock_jgdy_detail_em"][0]
    assert row["status"] == "skipped"
    assert row["error_type"] == "default_disabled"
    assert row["importance"] == "high"
    assert row["default_enabled"] is False
    assert row["manual_review_required"] is True
    assert row["acquisition_mode"] == "long_detail_run"
    assert row["disabled_category"] == "heavy_detail_source"


def test_stock_gdfx_holding_analyse_policy_is_recovered_heavy_manual_review(tmp_path):
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["event_ownership"],
        symbols=["000001"],
        report_dates=["20241231"],
        max_workers=1,
    )

    row = [r for r in out["rows"] if r["api_name"] == "stock_gdfx_holding_analyse_em"][0]
    assert row["status"] == "skipped"
    assert row["error_type"] == "default_disabled"
    assert row["default_enabled"] is False
    assert row["manual_review_required"] is True
    assert row["disabled_category"] == "recovered_heavy_source"
    assert row["importance"] == "high"
    assert row["acquisition_mode"] == "long_recovery_run"


def test_stock_zh_a_disclosure_relation_schema_mismatch_is_schema_drift_empty(tmp_path):
    def stock_zh_a_disclosure_relation_cninfo(symbol: str, start_date: str, end_date: str) -> _Result:  # noqa: ARG001
        raise KeyError("['公告日期'] are in the [columns]")

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        symbols=["000001"],
        start_date="20240101",
        end_date="20240131",
        selected_api_names=["stock_zh_a_disclosure_relation_cninfo"],
        adapter_map={"stock_zh_a_disclosure_relation_cninfo": stock_zh_a_disclosure_relation_cninfo},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["error_type"] == "downgraded_to_empty"
    assert "defensive_shape_guard" in row["error_message"]
    assert row["review_category"] == "schema_drift"
    assert row["disabled_category"] == "empty_review"
    assert row["manual_review_required"] is True


def test_stock_financial_analysis_indicator_empty_response_is_empty_review(tmp_path):
    def stock_financial_analysis_indicator(symbol: str) -> _Result:  # noqa: ARG001
        return _Result(pd.DataFrame())

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["000001"],
        selected_api_names=["stock_financial_analysis_indicator"],
        adapter_map={"stock_financial_analysis_indicator": stock_financial_analysis_indicator},
        include_disabled=True,
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["rows"] == 0
    assert row["disabled_category"] == "empty_review_source"
    assert row["review_category"] == "parameter_schema_review"
    assert row["manual_review_required"] is True
