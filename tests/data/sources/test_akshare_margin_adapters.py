from __future__ import annotations

import sys
import types

import pandas as pd

from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse


def test_fetch_stock_margin_detail_sse_length_mismatch_returns_empty(monkeypatch) -> None:
    fake = types.SimpleNamespace()

    def bad_call(*_args, **_kwargs):
        raise ValueError("Length mismatch: Expected axis has 0 elements, new values have 13 elements")

    fake.stock_margin_detail_sse = bad_call
    monkeypatch.setitem(sys.modules, "akshare", fake)

    res = fetch_stock_margin_detail_sse("20250104")
    assert isinstance(res.raw, pd.DataFrame)
    assert res.raw.empty
