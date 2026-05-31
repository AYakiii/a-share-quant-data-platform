from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

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
        max_workers=64,
        heavy_max_workers=16,
        long_run_max_workers=1,
        deferred_max_workers=4,
        heartbeat_sec=30,
        task_timeout_sec=600,
        heavy_task_timeout_sec=1200,
        long_run_task_timeout_sec=1800,
        request_sleep=0.10,
        heavy_request_sleep=0.20,
        long_run_request_sleep=0.50,
        task_retry_attempts=2,
        resume=False,
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


def test_raw_ingest_runner_is_reused_without_parquet_writer_logic():
    source = Path(preheat.__file__).read_text(encoding="utf-8")
    assert "run_raw_ingest_official" in source
    assert "write_raw_partition" not in source
    assert ".to_parquet" not in source
