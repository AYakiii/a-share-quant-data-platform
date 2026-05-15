from __future__ import annotations

import time
import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_broad_coverage_ingest_statuses_and_paths(tmp_path):
    adapters = {
        "stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
        "stock_zh_index_hist_csindex": lambda **kwargs: _Result(pd.DataFrame()),
        "stock_margin_detail_sse": lambda **kwargs: (_ for _ in ()).throw(ValueError("fail")),
    }
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "index_market", "margin_leverage", "financial_fundamental"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=True,
        include_disabled=True,
    )
    df = pd.read_csv(out["catalog_path"])
    assert {"success", "empty", "failed", "pending_adapter"}.issubset(set(df["status"]))
    assert (tmp_path / "raw_ingest_catalog.csv").exists()
    assert (tmp_path / "raw_ingest_summary.csv").exists()


def test_continue_on_error_false_does_not_crash(tmp_path):
    adapters = {"stock_zh_a_daily": lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))}
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=False,
        include_disabled=True,
    )
    assert len(out["rows"]) >= 1


def test_parameter_filtering_avoids_unexpected_kwargs(tmp_path):
    seen = {"pledge_called": False, "lhb_called": False, "mrtj_called": False, "mrmx_called": False}

    def noarg_pledge():
        seen["pledge_called"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    def noarg_lhb():
        seen["lhb_called"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    def noarg_mrtj():
        seen["mrtj_called"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    def noarg_mrmx():
        seen["mrmx_called"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    adapters = {
        "stock_gpzy_pledge_ratio_detail_em": noarg_pledge,
        "stock_lhb_stock_statistic_em": noarg_lhb,
        "stock_dzjy_mrtj": noarg_mrtj,
        "stock_dzjy_mrmx": noarg_mrmx,
    }

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["event_ownership", "trading_attention"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=True,
        include_disabled=True,
    )

    assert all(seen.values())
    df = pd.read_csv(out["catalog_path"])
    assert set(df.loc[df["api_name"].isin(["stock_gpzy_pledge_ratio_detail_em", "stock_lhb_stock_statistic_em", "stock_dzjy_mrtj", "stock_dzjy_mrmx"]), "status"]) == {"success"}




def test_phase18a12_viable_whitelist_coverage_exactness():
    from qsys.data.factor_lake.raw_ingest import COVERAGE_API_SPECS

    whitelist = {
        "stock_zh_a_hist","stock_individual_info_em","stock_zh_index_hist_csindex","index_stock_cons_csindex","index_stock_cons_weight_csindex",
        "stock_financial_analysis_indicator","stock_yjyg_em","stock_yysj_em","stock_margin_sse","stock_margin_detail_sse","stock_margin_szse",
        "stock_margin_detail_szse","stock_margin_underlying_info_szse","stock_industry_category_cninfo","stock_industry_change_cninfo",
        "stock_industry_clf_hist_sw","sw_index_first_info","sw_index_second_info","sw_index_third_info","index_component_sw","index_hist_sw",
        "index_realtime_sw","stock_board_industry_name_ths","stock_board_industry_index_ths","stock_board_industry_info_ths",
        "stock_board_industry_summary_ths","stock_board_concept_name_ths","stock_board_concept_index_ths","stock_board_concept_info_ths",
        "stock_board_concept_summary_ths","stock_zh_a_gdhs","stock_zh_a_gdhs_detail_em","stock_gdfx_free_holding_analyse_em",
        "stock_gdfx_holding_analyse_em","stock_gpzy_pledge_ratio_em","stock_gpzy_pledge_ratio_detail_em","stock_gpzy_industry_data_em",
        "stock_gpzy_profile_em","stock_fhps_em","stock_history_dividend","stock_history_dividend_detail","stock_restricted_release_queue_em",
        "stock_restricted_release_summary_em","stock_restricted_release_detail_em","stock_dzjy_sctj","stock_dzjy_mrmx","stock_dzjy_mrtj",
        "stock_dzjy_hyyybtj","stock_lhb_detail_em","stock_lhb_stock_statistic_em","stock_lhb_jgmmtj_em","stock_lhb_hyyyb_em",
        "stock_lhb_yybph_em","stock_jgdy_tj_em",
    }
    assert len(whitelist) == 54

    phase_families = [
        "market_price",
        "index_market",
        "financial_fundamental",
        "margin_leverage",
        "industry_concept",
        "event_ownership",
        "corporate_action",
        "trading_attention",
    ]
    phase_set = {
        row["api_name"]
        for family in phase_families
        for row in COVERAGE_API_SPECS.get(family, [])
    }

    missing = whitelist - phase_set
    assert not missing, f"missing viable APIs: {sorted(missing)}"

    assert "stock_industry_clf_hist_sw" in phase_set
    assert "stock_jgdy_tj_em" in phase_set

    extra = phase_set - whitelist
    assert not extra, f"unexpected non-whitelist APIs in phase set: {sorted(extra)}"
    assert "stock_zh_a_daily" not in phase_set


def test_recovery_seed_params_and_error_normalization(tmp_path):
    from qsys.data.factor_lake.raw_ingest import _params_for_mode, _normalize_error_message

    hist = _params_for_mode(
        "daily_symbol_range_hist", ["000001"], ["000300"], ["20240331"], ["20240329"], ["半导体"], ["AI PC"], "20240101", "20240331"
    )[0]
    assert hist["period"] == "daily"
    assert hist["adjust"] == "qfq"

    assert "network_unstable_retry" in _normalize_error_message("stock_zh_a_hist", "Read timed out")
    assert "defensive_shape_guard" in _normalize_error_message("stock_yjyg_em", "NoneType object")


def test_stock_individual_info_em_csv_fallback_on_write_error(tmp_path):
    from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest

    class _Result:
        def __init__(self, raw):
            self.raw = raw

    def ok_adapter(**kwargs):
        return _Result(pd.DataFrame({"symbol": [kwargs.get("symbol", "000001")], "mixed": [{"k": 1}]}))

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={"stock_individual_info_em": ok_adapter},
        continue_on_error=True,
        include_disabled=True,
    )
    df = pd.read_csv(out["catalog_path"])
    row = df.loc[df["api_name"] == "stock_individual_info_em"].iloc[0]
    assert row["status"] == "success"
    assert str(row["output_path"]).endswith(".csv") or str(row["output_path"]).endswith(".parquet")


def test_phase18a13b_wave3_defensive_downgrade_to_empty(tmp_path):
    from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest

    adapters = {
        "stock_individual_info_em": lambda **kwargs: (_ for _ in ()).throw(ValueError("Expecting value: line 1")),
        "stock_yjyg_em": lambda **kwargs: (_ for _ in ()).throw(TypeError("'NoneType' object is not subscriptable")),
        "stock_yysj_em": lambda **kwargs: (_ for _ in ()).throw(TypeError("'NoneType' object is not subscriptable")),
        "stock_industry_change_cninfo": lambda **kwargs: (_ for _ in ()).throw(KeyError("变更日期")),
    }

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "financial_fundamental", "industry_concept"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=True,
        include_disabled=True,
    )

    df = pd.read_csv(out["catalog_path"])
    for api in ["stock_individual_info_em", "stock_yjyg_em", "stock_yysj_em", "stock_industry_change_cninfo"]:
        row = df.loc[df["api_name"] == api].iloc[0]
        assert row["status"] == "empty"


def test_acquisition_checklist_outputs_and_rules(tmp_path):
    from qsys.data.factor_lake.raw_ingest import build_acquisition_checklist

    catalog_df = pd.DataFrame(
        [
            {"source_family": "index_market", "api_name": "stock_zh_index_hist_csindex", "status": "success"},
            {"source_family": "industry_concept", "api_name": "index_hist_sw", "status": "failed"},
            {"source_family": "market_price", "api_name": "stock_zh_a_hist", "status": "success"},
        ]
    )
    checklist_df, summary_df = build_acquisition_checklist(catalog_df)
    assert list(checklist_df.columns) == ["api_name", "source_family", "acquisition_status"]
    assert list(summary_df.columns) == ["acquisition_status", "count"]

    # temp disabled must stay paused even if success
    row_hist = checklist_df.loc[
        (checklist_df["source_family"] == "market_price")
        & (checklist_df["api_name"] == "stock_zh_a_hist")
    ].iloc[0]
    assert row_hist["acquisition_status"] == "暂停获取"

    # explicit excluded api
    row_daily = checklist_df.loc[
        (checklist_df["source_family"] == "market_price")
        & (checklist_df["api_name"] == "stock_zh_a_daily")
    ].iloc[0]
    assert row_daily["acquisition_status"] == "排除"

    # success non-disabled api
    row_index = checklist_df.loc[
        (checklist_df["source_family"] == "index_market")
        & (checklist_df["api_name"] == "stock_zh_index_hist_csindex")
    ].iloc[0]
    assert row_index["acquisition_status"] == "获取"


def test_disabled_sources_skipped_by_default_and_timing_fields(tmp_path):
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={},
        continue_on_error=True,
    )
    df = pd.read_csv(out["catalog_path"])
    row = df.loc[df["api_name"] == "stock_zh_a_hist"].iloc[0]
    assert row["status"] == "skipped"
    assert int(row["rows"]) == 0
    assert "disabled_reason" in str(row["error_message"])
    assert "started_at" in df.columns and "finished_at" in df.columns and "elapsed_sec" in df.columns
    row_free = df.loc[df["api_name"] == "stock_gdfx_free_holding_analyse_em"].iloc[0]
    row_hold = df.loc[df["api_name"] == "stock_gdfx_holding_analyse_em"].iloc[0]
    assert row_free["status"] == "skipped"
    assert row_hold["status"] == "skipped"
    assert "expensive and unstable in 10d recovery run" in str(row_free["error_message"])
    assert "expensive and unstable in 10d recovery run" in str(row_hold["error_message"])


def test_include_disabled_runs_disabled_sources(tmp_path):
    seen = {"called": False}

    class _Result:
        def __init__(self, raw):
            self.raw = raw

    def adapter(**kwargs):
        seen["called"] = True
        return _Result(pd.DataFrame({"x": [1]}))

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={"stock_zh_a_hist": adapter},
        continue_on_error=True,
        include_disabled=True,
    )
    df = pd.read_csv(out["catalog_path"])
    row = df.loc[df["api_name"] == "stock_zh_a_hist"].iloc[0]
    assert row["status"] in {"success", "empty", "failed"}
    assert seen["called"]


def test_max_workers_preserves_row_count_and_skipped_rows(tmp_path):
    out1 = run_raw_coverage_ingest(
        output_root=str(tmp_path / "mw1"),
        families=["market_price", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={},
        continue_on_error=True,
        max_workers=1,
    )
    out2 = run_raw_coverage_ingest(
        output_root=str(tmp_path / "mw2"),
        families=["market_price", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={},
        continue_on_error=True,
        max_workers=2,
    )
    df1 = pd.read_csv(out1["catalog_path"])
    df2 = pd.read_csv(out2["catalog_path"])
    assert len(df1) == len(df2)
    assert "skipped" in set(df1["status"])
    assert "skipped" in set(df2["status"])
    for col in ["started_at", "finished_at", "elapsed_sec"]:
        assert col in df1.columns and col in df2.columns
    assert (tmp_path / "mw1" / "raw_source_acquisition_checklist.csv").exists()
    assert (tmp_path / "mw2" / "raw_source_acquisition_summary.csv").exists()


def test_parallel_max_workers_2_faster_than_1_for_slow_adapters(tmp_path):
    class _Result:
        def __init__(self, raw):
            self.raw = raw

    def slow_hist(**kwargs):
        time.sleep(0.2)
        return _Result(pd.DataFrame({"x": [1]}))

    def slow_info(**kwargs):
        time.sleep(0.2)
        return _Result(pd.DataFrame({"x": [1]}))

    kwargs = dict(
        families=["market_price"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={"stock_zh_a_hist": slow_hist, "stock_individual_info_em": slow_info},
        continue_on_error=True,
        include_disabled=True,
    )

    t1 = time.perf_counter()
    run_raw_coverage_ingest(output_root=str(tmp_path / "seq"), max_workers=1, **kwargs)
    d1 = time.perf_counter() - t1
    t2 = time.perf_counter()
    run_raw_coverage_ingest(output_root=str(tmp_path / "par"), max_workers=2, **kwargs)
    d2 = time.perf_counter() - t2
    assert d2 < d1


def test_checklist_marks_new_disabled_event_ownership_as_paused():
    from qsys.data.factor_lake.raw_ingest import build_acquisition_checklist

    df = pd.DataFrame(
        [
            {"source_family": "event_ownership", "api_name": "stock_gdfx_free_holding_analyse_em", "status": "success"},
            {"source_family": "event_ownership", "api_name": "stock_gdfx_holding_analyse_em", "status": "success"},
        ]
    )
    checklist_df, _ = build_acquisition_checklist(df)
    for api in ["stock_gdfx_free_holding_analyse_em", "stock_gdfx_holding_analyse_em"]:
        row = checklist_df.loc[
            (checklist_df["source_family"] == "event_ownership")
            & (checklist_df["api_name"] == api)
        ].iloc[0]
        assert row["acquisition_status"] == "暂停获取"

def test_selected_api_names_limits_recovery_scope(tmp_path):
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "margin_leverage", "financial_fundamental", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map={},
        continue_on_error=True,
        include_disabled=False,
        selected_api_names=[
            "stock_zh_a_hist",
            "stock_individual_info_em",
            "stock_margin_detail_szse",
            "stock_financial_analysis_indicator",
            "stock_gpzy_pledge_ratio_detail_em",
        ],
    )
    df = pd.read_csv(out["catalog_path"])
    assert set(df["api_name"]) == {
        "stock_zh_a_hist",
        "stock_individual_info_em",
        "stock_margin_detail_szse",
        "stock_financial_analysis_indicator",
        "stock_gpzy_pledge_ratio_detail_em",
    }
    assert set(df["status"]) == {"skipped"}


def test_selected_api_names_with_include_disabled_runs_controlled_recovery(tmp_path):
    seen = set()

    class _Result:
        def __init__(self, raw):
            self.raw = raw

    def ok(api_name):
        def _fn(**kwargs):
            seen.add(api_name)
            return _Result(pd.DataFrame({"x": [1]}))

        return _fn

    adapters = {
        "stock_zh_a_hist": ok("stock_zh_a_hist"),
        "stock_individual_info_em": ok("stock_individual_info_em"),
        "stock_margin_detail_szse": ok("stock_margin_detail_szse"),
        "stock_financial_analysis_indicator": ok("stock_financial_analysis_indicator"),
        "stock_gpzy_pledge_ratio_detail_em": ok("stock_gpzy_pledge_ratio_detail_em"),
    }
    targets = list(adapters.keys())
    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_price", "margin_leverage", "financial_fundamental", "event_ownership"],
        symbols=["000001"],
        index_symbols=["000300"],
        report_dates=["20240331"],
        trade_dates=["20240329"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20240101",
        end_date="20240331",
        adapter_map=adapters,
        continue_on_error=True,
        include_disabled=True,
        selected_api_names=targets,
    )
    df = pd.read_csv(out["catalog_path"])
    assert set(df["api_name"]) == set(targets)
    assert set(df["status"]) == {"success"}
    assert seen == set(targets)
