from __future__ import annotations

import pandas as pd

from qsys.data.factor_lake.io import write_inventory
from qsys.data.factor_lake.schemas import SourceCase


def test_write_inventory_non_empty_sourcecase_list(tmp_path):
    cases = [
        SourceCase(
            case_id="daily_bar_raw__daily__000001__2024q1",
            source_family="market_price",
            api_name="stock_zh_a_daily",
            kwargs={"symbol": "sz000001", "start_date": "20240101", "end_date": "20240331", "adjust": ""},
            description="daily fallback probe",
            enabled=True,
        )
    ]
    out = tmp_path / "source_case_inventory.csv"
    write_inventory(cases, out)

    df = pd.read_csv(out)
    assert len(df) == 1
    assert df.loc[0, "api_name"] == "stock_zh_a_daily"
