from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from qsys.data.factor_lake.acquisition_universe import load_industry_codes
from qsys.data.factor_lake.raw_ingest import _params_for_mode, run_raw_coverage_ingest
from qsys.utils import run_raw_lake_preheat as preheat


class FakeAk:
    def tool_trade_date_hist_sina(self):
        return pd.DataFrame({"trade_date": pd.to_datetime(["2026-05-16", "2026-05-18", "2026-05-20", "2026-05-29", "2026-06-01"])})

    def stock_zh_index_spot_em(self):
        return pd.DataFrame({"代码": ["000300", "000905", "000852", "399001"]})

    def stock_board_industry_name_ths(self):
        return pd.DataFrame({"name": ["半导体", "银行", "半导体"]})

    def stock_board_concept_name_ths(self):
        return pd.DataFrame({"概念名称": ["人工智能", "低空经济"]})

    def sw_index_first_info(self):
        return pd.DataFrame({"行业代码": ["801010", "801020"]})

    def sw_index_second_info(self):
        return pd.DataFrame({"行业代码": ["801011"]})

    def sw_index_third_info(self):
        return pd.DataFrame({"行业代码": ["801012", "801010"]})


def _symbols_file(tmp_path: Path, content: str = "# comment\n000001\n600000\n000001\n\n") -> Path:
    path = tmp_path / "symbols.txt"
    path.write_text(content, encoding="utf-8")
    return path


def _args(tmp_path: Path, **overrides):
    args = SimpleNamespace(
        symbols_file=str(_symbols_file(tmp_path)),
        output_root=str(tmp_path / "out"),
        start_date="20260518",
        end_date="20260529",
        report_dates="20251231,20260331",
        lanes="main",
        only_families=None,
        exclude_families=None,
        only_apis=None,
        exclude_apis=None,
        max_workers=64,
        heavy_max_workers=16,
        long_run_max_workers=1,
        deferred_max_workers=4,
        heartbeat_sec=30,
        task_timeout_sec=120,
        manual_selected_task_timeout_sec=180,
        heavy_task_timeout_sec=300,
        long_run_task_timeout_sec=600,
        deferred_task_timeout_sec=300,
        request_sleep=0.10,
        heavy_request_sleep=0.20,
        long_run_request_sleep=0.50,
        task_retry_attempts=2,
        task_retry_sleep_sec=0.0,
        task_retry_backoff=1.0,
        task_retry_jitter_sec=0.0,
        resume=False,
        refresh_universe=False,
        include_deferred_recovery=False,
        skip_heavy=False,
        skip_long_run=False,
        dry_run=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _universe(tmp_path: Path):
    return preheat.discover_universe(_symbols_file(tmp_path), "20260518", "20260529", "20251231,20260331", ak_module=FakeAk())


def test_symbols_file_loads_six_digit_symbols_and_preserves_order_dedup_comments(tmp_path):
    path = _symbols_file(tmp_path, "# header\n000001\n\n600000\n000001\n")
    assert preheat.load_stock_symbols_from_file(path) == ["000001", "600000"]


def test_symbols_file_missing_or_empty_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError, match="symbols file not found"):
        preheat.load_stock_symbols_from_file(tmp_path / "missing.txt")
    empty = _symbols_file(tmp_path, "# only comments\n\n")
    with pytest.raises(ValueError, match="empty"):
        preheat.load_stock_symbols_from_file(empty)


def test_trading_date_discovery_filters_start_end_range():
    assert preheat.discover_trading_dates(FakeAk(), "20260518", "20260529") == ["20260518", "20260520", "20260529"]


def test_index_industry_concept_discovery_uses_synthetic_akshare_adapter():
    ak = FakeAk()
    assert preheat.discover_index_symbols(ak) == ["000300", "000905", "000852", "399001"]
    assert preheat.discover_industry_names(ak) == ["半导体", "银行"]
    assert preheat.discover_concept_names(ak) == ["人工智能", "低空经济"]
    assert preheat.discover_sw_industry_codes(ak) == ["801010", "801020", "801011", "801012"]


def test_report_date_parsing_preserves_explicit_values():
    assert preheat.parse_report_dates("20251231,20260331,20251231") == ["20251231", "20260331"]


def test_lane_planning_places_financial_statement_apis_in_main(tmp_path):
    plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    by_api = {row["api_name"]: row for row in plan}
    for api in [
        "stock_balance_sheet_by_report_em",
        "stock_profit_sheet_by_report_em",
        "stock_cash_flow_sheet_by_report_em",
        "stock_zcfz_em",
        "stock_lrb_em",
        "stock_xjll_em",
    ]:
        assert by_api[api]["lane"] == "main"
        assert by_api[api]["enabled"] is True


def test_lane_planning_places_index_industry_concept_fanout_in_heavy(tmp_path):
    plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    by_api = {row["api_name"]: row for row in plan}
    assert by_api["stock_zh_index_hist_csindex"]["lane"] == "heavy"
    assert by_api["stock_board_industry_index_ths"]["lane"] == "heavy"
    assert by_api["stock_board_concept_index_ths"]["lane"] == "heavy"


def test_lane_planning_places_known_long_run_apis(tmp_path):
    plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    by_api = {row["api_name"]: row for row in plan}
    assert by_api["stock_jgdy_detail_em"]["lane"] == "long_run"
    assert by_api["stock_gdfx_holding_analyse_em"]["lane"] == "long_run"


def test_deferred_sources_recorded_and_not_executed_by_default(tmp_path):
    plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    by_api = {row["api_name"]: row for row in plan}
    assert by_api["stock_zh_a_hist"]["lane"] == "deferred"
    assert by_api["stock_zh_a_hist"]["enabled"] is False


def test_include_deferred_recovery_adds_separate_lane(tmp_path):
    plan = preheat.build_preheat_plan(_args(tmp_path, include_deferred_recovery=True), _universe(tmp_path))
    by_api = {row["api_name"]: row for row in plan}
    assert by_api["stock_zh_a_hist"]["lane"] == "deferred_recovery"
    assert by_api["stock_zh_a_hist"]["enabled"] is True


def test_duplicate_api_registrations_do_not_cause_duplicate_execution(monkeypatch, tmp_path):
    monkeypatch.setattr(
        preheat,
        "COVERAGE_API_SPECS",
        {"family_a": [{"api_name": "same_api", "param_mode": "none"}], "family_b": [{"api_name": "same_api", "param_mode": "none"}]},
    )
    monkeypatch.setattr(preheat, "PHASE_COVERAGE_FAMILIES", ("family_a", "family_b"))
    plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    assert [row["api_name"] for row in plan] == ["same_api"]


def test_deferred_recovery_only_api_filter_does_not_release_all_deferred(tmp_path):
    args = _args(tmp_path, lanes="deferred_recovery", only_apis="stock_zh_a_hist")
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    selected = [row for row in plan if row["selected"]]
    assert [row["api_name"] for row in selected] == ["stock_zh_a_hist"]
    assert selected[0]["lane"] == "deferred_recovery"


def test_conflicting_duplicate_api_param_modes_fail_clearly(monkeypatch, tmp_path):
    monkeypatch.setattr(
        preheat,
        "COVERAGE_API_SPECS",
        {"family_a": [{"api_name": "same_api", "param_mode": "none"}], "family_b": [{"api_name": "same_api", "param_mode": "symbol_only"}]},
    )
    monkeypatch.setattr(preheat, "PHASE_COVERAGE_FAMILIES", ("family_a", "family_b"))
    with pytest.raises(ValueError, match="Conflicting duplicate API registration.*same_api"):
        preheat.build_preheat_plan(_args(tmp_path), preheat.PreheatUniverse())


def test_dry_run_writes_plan_artifacts_and_executes_no_acquisition_calls(tmp_path):
    args = _args(tmp_path, dry_run=True)
    called = False

    def runner(**kwargs):
        nonlocal called
        called = True
        return {"rows": []}

    rc = preheat.main(
        [
            "--symbols-file",
            args.symbols_file,
            "--output-root",
            args.output_root,
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
            "--report-dates",
            args.report_dates,
            "--dry-run",
        ],
        ak_module=FakeAk(),
        runner=runner,
    )
    assert rc == 0
    assert called is False
    op = Path(args.output_root) / "_operation_review"
    assert (op / "preheat_plan.json").exists()
    assert (op / "preheat_plan_by_api.csv").exists()
    assert (op / "deferred_sources.csv").exists()
    assert (op / "long_run_sources.csv").exists()
    assert (op / "universe_snapshots" / "stock_symbols.csv").exists()


@pytest.mark.parametrize("path", ["/content/drive/MyDrive/out", "/content/gdrive/out", "MyDrive/raw"])
def test_drive_like_output_paths_are_rejected(path):
    with pytest.raises(ValueError, match="Google Drive"):
        preheat.reject_drive_output_root(path)


def test_resume_is_forwarded_to_official_raw_ingest_runner(tmp_path):
    args = _args(tmp_path, resume=True)
    universe = _universe(tmp_path)
    plan = [{"source_family": "financial_fundamental", "api_name": "stock_zcfz_em", "lane": "main", "enabled": True}]
    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
        return {"rows": []}

    preheat.run_lanes(args, universe, plan, runner=runner)
    assert calls[0]["resume"] is True


def test_checklist_artifact_created_from_synthetic_lane_outputs(tmp_path):
    output_root = tmp_path / "out"
    plan = [
        {
            "lane": "main",
            "source_family": "financial_fundamental",
            "api_name": "stock_zcfz_em",
            "priority_tier": "P1",
            "data_theme": "financial_statement_raw",
            "acquisition_mode": "bulk_financial_core",
            "planned_tasks": 1,
            "enabled": True,
        }
    ]
    manifests = [
        {
            "lane": "main",
            "rows": [
                {"source_family": "financial_fundamental", "api_name": "stock_zcfz_em", "status": "success", "rows": 3, "elapsed_sec": 1.5}
            ],
        }
    ]
    preheat.write_runtime_artifacts(output_root, plan, manifests)
    checklist = pd.read_csv(output_root / "_operation_review" / "acquisition_checklist.csv")
    assert checklist.loc[0, "success_tasks"] == 1
    assert checklist.loc[0, "rows"] == 3


def test_default_main_only_execution_excludes_heavy_and_long_run(tmp_path):
    args = _args(tmp_path)
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    assert {row["lane"] for row in plan if row["selected"]} == {"main"}
    assert not any(row["api_name"] == "stock_zh_index_hist_csindex" and row["selected"] for row in plan)


def test_manual_selected_lane_requires_explicit_selection(tmp_path):
    default_plan = preheat.build_preheat_plan(_args(tmp_path), _universe(tmp_path))
    assert not any(row["api_name"] == "stock_financial_analysis_indicator_em" and row["selected"] for row in default_plan)
    manual_plan = preheat.build_preheat_plan(_args(tmp_path, lanes="manual_selected"), _universe(tmp_path))
    assert any(row["api_name"] == "stock_financial_analysis_indicator_em" and row["lane"] == "manual_selected" and row["selected"] for row in manual_plan)


def test_heavy_opt_in_and_only_heavy_execution(tmp_path):
    args = _args(tmp_path, lanes="heavy")
    universe = preheat.PreheatUniverse(index_symbols=["000300"], industry_names=["半导体"], concept_names=["人工智能"], industry_codes=["801010"])
    plan = preheat.build_preheat_plan(args, universe)
    selected = [row for row in plan if row["selected"]]
    assert selected
    assert {row["lane"] for row in selected} == {"heavy"}
    assert any(row["api_name"] == "stock_zh_index_hist_csindex" for row in selected)


def test_family_and_api_include_exclude_filters(tmp_path):
    args = _args(tmp_path, only_families="financial_fundamental", exclude_apis="stock_zcfz_em")
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    selected = [row for row in plan if row["selected"]]
    assert selected
    assert {row["source_family"] for row in selected} == {"financial_fundamental"}
    assert "stock_zcfz_em" not in {row["api_name"] for row in selected}
    args = _args(tmp_path, only_apis="stock_zcfz_em,stock_lrb_em", exclude_families="market_price")
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    assert {row["api_name"] for row in plan if row["selected"]} == {"stock_zcfz_em", "stock_lrb_em"}


def test_only_non_heavy_execution_with_main_lane(tmp_path):
    args = _args(tmp_path, lanes="main")
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    assert all(row["lane"] == "main" for row in plan if row["selected"])


def test_lazy_discovery_and_conditional_symbols_and_report_dates(tmp_path):
    calls = []

    class MinimalAk(FakeAk):
        def stock_zh_index_spot_em(self):
            calls.append("index")
            return super().stock_zh_index_spot_em()

        def stock_board_industry_name_ths(self):
            calls.append("industry")
            return super().stock_board_industry_name_ths()

    args = _args(tmp_path, symbols_file=None, report_dates=None, only_apis="stock_margin_underlying_info_szse")
    skeleton = preheat.build_preheat_plan(args, preheat.PreheatUniverse())
    universe = preheat.discover_universe_for_plan(args, skeleton, ak_module=MinimalAk())
    assert universe == preheat.PreheatUniverse()
    assert calls == []
    args = _args(tmp_path, symbols_file=None, report_dates=None, only_apis="stock_zcfz_em")
    skeleton = preheat.build_preheat_plan(args, preheat.PreheatUniverse())
    with pytest.raises(ValueError, match="report-dates"):
        preheat.discover_universe_for_plan(args, skeleton, ak_module=FakeAk())


def test_catalog_preservation_across_lanes_and_retry_forwarding(tmp_path):
    args = _args(tmp_path, lanes="main,heavy", task_retry_sleep_sec=1.5, task_retry_backoff=2.0, task_retry_jitter_sec=0.25)
    universe = preheat.PreheatUniverse(stock_symbols=["000001"], report_dates=["20251231"], index_symbols=["000300"], industry_names=["半导体"], concept_names=["人工智能"], industry_codes=["801010"])
    plan = preheat.build_preheat_plan(args, universe)
    calls = []

    def runner(**kwargs):
        calls.append(kwargs)
        return {"rows": []}

    manifests = preheat.run_lanes(args, universe, plan, runner=runner)
    assert len(calls) >= 2
    assert calls[0]["resume"] is False
    assert calls[1]["resume"] is True
    assert manifests[0]["requested_resume"] is False
    assert manifests[1]["effective_resume"] is True
    assert calls[0]["task_retry_sleep_sec"] == 1.5
    assert calls[0]["task_retry_backoff"] == 2.0
    assert calls[0]["task_retry_jitter_sec"] == 0.25


def test_resume_reuses_universe_snapshots_by_default(tmp_path):
    output_root = tmp_path / "out"
    preheat.write_universe_snapshots(output_root, preheat.PreheatUniverse(stock_symbols=["000001"], report_dates=["20251231"]))
    args = _args(tmp_path, output_root=str(output_root), symbols_file=None, report_dates=None, only_apis="stock_zcfz_em", resume=True)
    skeleton = preheat.build_preheat_plan(args, preheat.PreheatUniverse())
    universe = preheat.discover_universe_for_plan(args, skeleton, ak_module=FakeAk())
    assert universe.stock_symbols == ["000001"]
    assert universe.report_dates == ["20251231"]


def test_fresh_non_resume_run_rediscoveres_instead_of_reusing_stale_snapshots(tmp_path):
    output_root = tmp_path / "out"
    preheat.write_universe_snapshots(output_root, preheat.PreheatUniverse(report_dates=["19991231"]))
    args = _args(tmp_path, output_root=str(output_root), report_dates="20251231", only_apis="stock_zcfz_em", resume=False)
    skeleton = preheat.build_preheat_plan(args, preheat.PreheatUniverse())
    universe = preheat.discover_universe_for_plan(args, skeleton, ak_module=FakeAk())
    assert universe.report_dates == ["20251231"]


def test_industry_codes_loader_and_raw_ingest_fallback_use_industry_codes_file(tmp_path):
    universe_root = tmp_path / "universe"
    universe_root.mkdir()
    pd.DataFrame({"industry_code": ["801010", "801020"]}).to_csv(universe_root / "industry_codes.csv", index=False)
    assert load_industry_codes(universe_root=universe_root) == ["801010", "801020"]
    assert _params_for_mode("industry_code", [], [], [], [], [], [], "20200101", "20200101", industry_codes=["801010", "801020"]) == [
        {"symbol": "801010"},
        {"symbol": "801020"},
    ]

    calls = []

    def index_hist_sw(symbol: str):
        calls.append(symbol)
        return pd.DataFrame({"代码": [symbol]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "out"),
        families=["industry_concept"],
        selected_api_names=["index_hist_sw"],
        adapter_map={"index_hist_sw": index_hist_sw},
        universe_root=universe_root,
        max_workers=1,
    )
    assert calls == ["801010", "801020"]
    assert [row["status"] for row in out["rows"]] == ["success", "success"]


def test_dry_run_semantics_do_not_create_false_recovery_rows(tmp_path):
    args = _args(tmp_path, dry_run=True)
    plan = preheat.build_preheat_plan(args, _universe(tmp_path))
    preheat.write_runtime_artifacts(args.output_root, plan, [], dry_run=True)
    checklist = pd.read_csv(Path(args.output_root) / "_operation_review" / "acquisition_checklist.csv")
    selected = checklist[checklist["api_name"] == "stock_zcfz_em"].iloc[0]
    assert selected["execution_status"] == "dry_run_not_executed"
    assert selected["recovery_required"] == False
    assert selected["recommended_action"] == "ready_for_execution"
    recovery = pd.read_csv(Path(args.output_root) / "_operation_review" / "recovery_tasks.csv")
    assert recovery.empty


def test_sw_full_industry_code_fanout_and_heavy_classification(tmp_path):
    args = _args(tmp_path, lanes="heavy")
    skeleton = preheat.build_preheat_plan(args, preheat.PreheatUniverse())
    universe = preheat.discover_universe_for_plan(args, skeleton, ak_module=FakeAk())
    assert universe.industry_codes == ["801010", "801020", "801011", "801012"]
    plan = preheat.build_preheat_plan(args, universe)
    by_api = {row["api_name"]: row for row in plan}
    assert by_api["index_hist_sw"]["lane"] == "heavy"
    assert by_api["index_hist_sw"]["planned_tasks"] == 4


def test_timeout_defaults_and_overrides(tmp_path):
    parser = preheat.build_parser()
    args = parser.parse_args(["--output-root", str(tmp_path), "--start-date", "20260518", "--end-date", "20260529"])
    assert args.task_timeout_sec == 120
    assert args.manual_selected_task_timeout_sec == 180
    assert args.heavy_task_timeout_sec == 300
    assert args.long_run_task_timeout_sec == 600
    assert args.deferred_task_timeout_sec == 300
    args = parser.parse_args(["--output-root", str(tmp_path), "--start-date", "20260518", "--end-date", "20260529", "--task-timeout-sec", "9", "--heavy-task-timeout-sec", "10"])
    assert args.task_timeout_sec == 9
    assert args.heavy_task_timeout_sec == 10


def test_raw_ingest_runner_is_reused_without_parquet_writer_logic():
    source = Path(preheat.__file__).read_text(encoding="utf-8")
    assert "run_raw_ingest_official" in source
    assert "write_raw_partition" not in source
    assert ".to_parquet" not in source
