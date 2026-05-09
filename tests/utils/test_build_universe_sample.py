from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from qsys.utils import build_universe_sample as mod
from qsys.universe import csindex


def _fake_fetch(index_symbol: str) -> pd.DataFrame:
    mapping = {
        "000300": ["000001", "000002", "000003"],
        "000905": ["000003", "000004", "000005"],
        "000852": ["000005", "000006", "000007"],
    }
    return pd.DataFrame({"品种代码": mapping[index_symbol]})


def test_n_is_configurable_and_capped(monkeypatch) -> None:
    monkeypatch.setattr(csindex, "fetch_index_components", _fake_fetch)

    symbols, meta = mod.build_universe_sample(["000300", "000905", "000852"], n=5, seed=1)
    assert len(symbols) == 5
    assert int(meta["actual_n"].iloc[0]) == 5

    symbols2, meta2 = mod.build_universe_sample(["000300"], n=1000, seed=1)
    assert len(symbols2) == 3
    assert int(meta2["actual_n"].iloc[0]) == 3
    assert int(meta2["requested_n"].iloc[0]) == 1000


def test_generated_filenames_include_n_and_seed(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(csindex, "fetch_index_components", _fake_fetch)

    out = tmp_path / "u"
    # emulate CLI behavior by calling helpers in main-like flow
    symbols, meta = mod.build_universe_sample(["000300", "000905"], n=50, seed=42)
    name = "csi_large_mid"
    symbols_path = out / f"{name}_n50_seed42_symbols.txt"
    metadata_path = out / f"{name}_n50_seed42_metadata.csv"
    out.mkdir(parents=True, exist_ok=True)
    symbols_path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    meta.to_csv(metadata_path, index=False)

    assert symbols_path.exists()
    assert metadata_path.exists()
    assert "n50_seed42" in symbols_path.name
    assert "n50_seed42" in metadata_path.name


def test_cli_explicit_output_paths_override_generated_names(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(csindex, "fetch_index_components", _fake_fetch)

    symbols, meta = mod.build_universe_sample(["000300"], n=2, seed=7)
    sym = tmp_path / "custom_symbols.txt"
    mta = tmp_path / "custom_metadata.csv"
    sym.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    meta.to_csv(mta, index=False)

    assert Path(sym).name == "custom_symbols.txt"
    assert Path(mta).name == "custom_metadata.csv"
