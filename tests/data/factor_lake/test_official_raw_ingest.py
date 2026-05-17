from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_ingest_official


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def test_official_catalog_contract(tmp_path):
    out = run_raw_ingest_official(
        output_root=str(tmp_path),
        families=["market_price"],
        symbols=["000001"],
        index_symbols=["000300"],
        trade_dates=["20100104"],
        report_dates=["20100331"],
        industry_names=["半导体"],
        concept_names=["AI PC"],
        start_date="20100101",
        end_date="20100131",
        adapter_map={"stock_zh_a_hist": lambda **kwargs: _Result(pd.DataFrame({"x": [1]}))},
        include_disabled=True,
    )
    df = pd.read_csv(out["catalog_path"])
    required = {"run_id","dataset_name","source_family","api_name","partition_json","params_json","status","rows","error_type","error_message","output_path","metadata_path","started_at","finished_at","elapsed_sec"}
    assert required.issubset(df.columns)
