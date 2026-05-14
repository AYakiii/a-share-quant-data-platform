from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_broad_coverage_ingest_statuses_and_paths(tmp_path):
    adapters = {
        "stock_zh_a_daily": lambda **kwargs: _Result(pd.DataFrame({"x": [1]})),
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
    )
    df = pd.read_csv(out["catalog_path"])
    assert {"success", "empty", "failed", "pending_adapter"}.issubset(set(df["status"]))
    assert (tmp_path / "raw_ingest_catalog.csv").exists()
    assert (tmp_path / "raw_ingest_summary.csv").exists()


def test_continue_on_error_false_does_not_crash(tmp_path):
    adapters = {"stock_zh_a_daily": lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))}
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
        adapter_map=adapters,
        continue_on_error=False,
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
    )

    assert all(seen.values())
    df = pd.read_csv(out["catalog_path"])
    assert set(df.loc[df["api_name"].isin(["stock_gpzy_pledge_ratio_detail_em", "stock_lhb_stock_statistic_em", "stock_dzjy_mrtj", "stock_dzjy_mrmx"]), "status"]) == {"success"}


def test_phase18a12_expansion_apis_included_in_coverage_specs():
    from qsys.data.factor_lake.raw_ingest import COVERAGE_API_SPECS

    expected = {
        "corporate_action": {"stock_history_dividend_detail", "stock_restricted_release_queue_em", "stock_restricted_release_summary_em"},
        "event_ownership": {"stock_gpzy_industry_data_em", "stock_gpzy_profile_em"},
        "index_market": {"index_stock_cons_csindex", "index_stock_cons_weight_csindex"},
        "industry_concept": {
            "index_realtime_sw", "stock_board_concept_info_ths", "stock_board_concept_name_ths",
            "stock_board_concept_summary_ths", "stock_board_industry_info_ths", "stock_board_industry_name_ths",
            "stock_board_industry_summary_ths", "stock_industry_category_cninfo", "sw_index_first_info",
            "sw_index_second_info", "sw_index_third_info",
        },
        "margin_leverage": {"stock_margin_sse", "stock_margin_szse", "stock_margin_underlying_info_szse"},
        "market_price": {"stock_individual_info_em"},
        "trading_attention": {"stock_dzjy_hyyybtj", "stock_dzjy_sctj", "stock_lhb_hyyyb_em", "stock_lhb_yybph_em"},
    }

    for family, apis in expected.items():
        configured = {row["api_name"] for row in COVERAGE_API_SPECS[family]}
        assert apis.issubset(configured)
