from __future__ import annotations

import pytest

from qsys.data.factor_lake.acquisition_universe import build_trade_dates, load_stock_symbols


def test_universe_missing_files_fail_loud(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_stock_symbols(None, universe_root=tmp_path)


def test_trade_dates_use_filter_override(tmp_path):
    out = build_trade_dates("20100101", "20100131", trade_dates=["20100104"], universe_root=tmp_path)
    assert out == ["20100104"]
