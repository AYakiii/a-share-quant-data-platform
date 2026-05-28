from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_p2_discovery_probe, run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_stock_jgdy_detail_none_like_response_downgrades_to_auditable_empty(tmp_path):
    def stock_jgdy_detail_em(date: str) -> _Result:  # noqa: ARG001
        raise TypeError("'NoneType' object is not subscriptable")

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        report_dates=["20241211"],
        selected_api_names=["stock_jgdy_detail_em"],
        adapter_map={"stock_jgdy_detail_em": stock_jgdy_detail_em},
        include_disabled=True,
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["error_type"] == "downgraded_to_empty"
    assert "defensive_shape_guard" in row["error_message"]
    assert "parser_empty_response" in row["error_message"]


def test_disclosure_relation_schema_mismatch_stays_auditable_empty(tmp_path):
    def stock_zh_a_disclosure_relation_cninfo(symbol: str, start_date: str, end_date: str) -> _Result:  # noqa: ARG001
        raise KeyError("['关联关系'] are in the [columns]")

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        symbols=["000001"],
        start_date="20230618",
        end_date="20231219",
        selected_api_names=["stock_zh_a_disclosure_relation_cninfo"],
        adapter_map={"stock_zh_a_disclosure_relation_cninfo": stock_zh_a_disclosure_relation_cninfo},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["error_type"] == "downgraded_to_empty"
    assert "schema_mismatch_empty_response" in row["error_message"]
    assert "defensive_shape_guard" in row["error_message"]


def test_financial_analysis_indicator_empty_response_is_manual_review_empty(tmp_path):
    def stock_financial_analysis_indicator(symbol: str) -> _Result:
        return _Result(pd.DataFrame(columns=["symbol", "指标"]))

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["financial_fundamental"],
        symbols=["600004"],
        selected_api_names=["stock_financial_analysis_indicator"],
        adapter_map={"stock_financial_analysis_indicator": stock_financial_analysis_indicator},
        include_disabled=True,
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["rows"] == 0
    assert row["error_type"] == "empty_review_source"
    assert "manual review" in row["error_message"]


def test_p2_discovery_failures_are_classified_without_crashing():
    def stock_market_fund_flow() -> _Result:
        raise ConnectionError("read timed out")

    def stock_individual_fund_flow_rank(indicator: str) -> _Result:  # noqa: ARG001
        raise RuntimeError("502 Bad Gateway")

    def stock_sector_fund_flow_rank(indicator: str, sector_type: str) -> _Result:  # noqa: ARG001
        raise ValueError("JSONDecodeError: Expecting value")

    def stock_sector_fund_flow_summary(symbol: str, indicator: str) -> _Result:  # noqa: ARG001
        raise TypeError("missing required positional argument: symbol")

    def futures_inventory_99(symbol: str) -> None:  # noqa: ARG001
        return None

    out = run_p2_discovery_probe(
        adapter_map={
            "stock_market_fund_flow": stock_market_fund_flow,
            "stock_individual_fund_flow_rank": stock_individual_fund_flow_rank,
            "stock_sector_fund_flow_rank": stock_sector_fund_flow_rank,
            "stock_sector_fund_flow_summary": stock_sector_fund_flow_summary,
            "futures_inventory_99": futures_inventory_99,
        }
    )

    by_api = {row["api_name"]: row["failure_class"] for row in out["rows"]}
    assert by_api["stock_market_fund_flow"] == "network_unstable_retry"
    assert by_api["stock_individual_fund_flow_rank"] == "bad_gateway"
    assert by_api["stock_sector_fund_flow_rank"] == "json_empty_response"
    assert by_api["stock_sector_fund_flow_summary"] == "missing_required_param"
    assert by_api["futures_inventory_99"] == "parser_empty_response"
    assert all(row["status"] == "failed" for row in out["rows"])
    assert all(row["params_json"] for row in out["rows"])


def test_heavy_unstable_sources_remain_deferred_by_default(tmp_path):
    called = {"n": 0}

    def should_not_call(**kwargs):  # noqa: ANN003, ARG001
        called["n"] += 1
        return _Result(pd.DataFrame({"x": [1]}))

    heavy_apis = [
        "stock_gdfx_free_holding_analyse_em",
        "stock_gdfx_holding_analyse_em",
        "stock_gpzy_pledge_ratio_detail_em",
    ]
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["event_ownership"],
        report_dates=["20221231"],
        selected_api_names=heavy_apis,
        adapter_map={api: should_not_call for api in heavy_apis},
        include_disabled=False,
        max_workers=1,
    )

    rows = out["rows"]
    assert called["n"] == 0
    assert {row["api_name"] for row in rows} == set(heavy_apis)
    assert {row["status"] for row in rows} == {"skipped"}
    assert {row["error_type"] for row in rows} == {"deferred_manual_review"}
    assert {row["disabled_category"] for row in rows} == {"heavy_unstable_source"}
    assert all(row["manual_review_required"] is True for row in rows)
