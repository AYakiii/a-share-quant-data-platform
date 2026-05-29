from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_stock_jgdy_detail_none_like_parser_response_downgrades_to_empty(tmp_path):
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
    assert "NoneType" in row["error_message"]
