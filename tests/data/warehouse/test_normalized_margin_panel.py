from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qsys.data.warehouse.normalized_margin_panel import (
    MarginPanelBuildConfig,
    build_normalized_margin_panel,
    load_margin_panel,
    normalize_margin_raw_frame,
    resolve_output_dataset_path,
    resolve_raw_dataset_path,
)


def _write_raw(root: Path, exchange: str, date: str, df: pd.DataFrame) -> None:
    fp = root / f"exchange={exchange}" / f"trade_date={date}" / "data.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fp, index=False)


def _sse_df() -> pd.DataFrame:
    return pd.DataFrame({"信用交易日期": ["2024-01-02"], "标的证券代码": ["600000"], "标的证券简称": ["浦发银行"], "融资余额": [1.0], "融资买入额": [2.0], "融资偿还额": [3.0], "融券余量": [4.0], "融券卖出量": [5.0], "融券偿还量": [6.0]})


def _szse_df() -> pd.DataFrame:
    return pd.DataFrame({"证券代码": ["000001"], "证券简称": ["平安银行"], "融资买入额": [2.0], "融资余额": [1.0], "融券卖出量": [5.0], "融券余量": [4.0], "融券余额": [7.0], "融资融券余额": [8.0], "trade_date": ["2024-01-02"]})


def test_path_resolution_canonical_and_override(tmp_path: Path) -> None:
    c = MarginPanelBuildConfig(raw_root=tmp_path / "raw", output_root=tmp_path / "norm", start_date="2024-01-01", end_date="2024-01-31")
    assert resolve_raw_dataset_path(c) == tmp_path / "raw" / "margin_detail" / "v1"
    assert resolve_output_dataset_path(c) == tmp_path / "norm" / "margin_panel" / "v1"
    c2 = MarginPanelBuildConfig(raw_root=tmp_path / "r", output_root=tmp_path / "o", raw_dataset_path=tmp_path / "custom_raw", output_dataset_path=tmp_path / "custom_out", start_date="2024-01-01", end_date="2024-01-31")
    assert resolve_raw_dataset_path(c2) == tmp_path / "custom_raw"
    assert resolve_output_dataset_path(c2) == tmp_path / "custom_out"


def test_normalization_sse_szse_and_strict() -> None:
    sse, _ = normalize_margin_raw_frame(_sse_df(), "SSE", "2024-01-02")
    assert sse.loc[0, "asset"] == "600000.SH"
    assert bool(sse.loc[0, "has_short_balance"]) is False
    szse, _ = normalize_margin_raw_frame(_szse_df(), "SZSE", "2024-01-02")
    assert szse.loc[0, "asset"] == "000001.SZ"
    assert bool(szse.loc[0, "has_short_balance"]) is True
    with pytest.raises(ValueError):
        normalize_margin_raw_frame(_sse_df().drop(columns=["融资余额"]), "SSE", "2024-01-02", strict_schema=True)


def test_build_artifacts_overwrite_loader_and_guardrails(tmp_path: Path) -> None:
    raw_dataset = tmp_path / "raw" / "margin_detail" / "v1"
    _write_raw(raw_dataset, "SSE", "2024-01-02", _sse_df())
    sz = _szse_df(); sz["trade_date"] = ["2024-02-01"]; _write_raw(raw_dataset, "SZSE", "2024-02-01", sz)
    out_root = tmp_path / "normalized"
    artifact = tmp_path / "artifacts"
    cfg = MarginPanelBuildConfig(raw_root=tmp_path / "raw", output_root=out_root, artifact_dir=artifact, start_date="2024-01-01", end_date="2024-02-28")
    res = build_normalized_margin_panel(cfg)

    assert (resolve_output_dataset_path(cfg) / "year=2024" / "month=01" / "data.parquet").exists()
    assert (resolve_output_dataset_path(cfg) / "year=2024" / "month=02" / "data.parquet").exists()

    manifest = json.loads((artifact / "manifest.json").read_text(encoding="utf-8"))
    for key in ["source_name", "raw_dataset_path", "output_dataset_path", "n_rows", "warnings_count"]:
        assert key in manifest

    inv = pd.read_csv(artifact / "raw_to_normalized_inventory.csv")
    assert set(["exchange", "trade_date", "raw_path", "raw_exists", "raw_size", "raw_rows", "normalized_rows", "status", "error_type", "error_message"]).issubset(inv.columns)

    cmp = pd.read_csv(artifact / "compact_manifest.csv")
    assert set(["year", "month", "path", "rows", "n_dates", "n_assets", "min_date", "max_date", "size", "size_mb"]).issubset(cmp.columns)

    assert (artifact / "warnings.md").read_text(encoding="utf-8").strip() == "No warnings recorded."

    keep = raw_dataset / "keep.txt"
    keep.write_text("raw", encoding="utf-8")
    (resolve_output_dataset_path(cfg) / "sentinel.txt").write_text("out", encoding="utf-8")
    cfg_over = MarginPanelBuildConfig(raw_root=tmp_path / "raw", output_root=out_root, artifact_dir=artifact, start_date="2024-01-01", end_date="2024-02-28", overwrite=True)
    build_normalized_margin_panel(cfg_over)
    assert keep.exists()
    assert not (resolve_output_dataset_path(cfg_over) / "sentinel.txt").exists()

    with pytest.raises(ValueError):
        build_normalized_margin_panel(MarginPanelBuildConfig(raw_root=tmp_path / "raw", output_root=tmp_path / "raw", raw_dataset_path=raw_dataset, output_dataset_path=raw_dataset, start_date="2024-01-01", end_date="2024-01-31"))

    loaded = load_margin_panel(resolve_output_dataset_path(cfg_over), "2024-01-01", "2024-01-31")
    assert isinstance(loaded.index, pd.MultiIndex)
    assert loaded.index.names == ["date", "asset"]
    loaded_sz = load_margin_panel(resolve_output_dataset_path(cfg_over), "2024-01-01", "2024-02-28", exchanges=["szse"], set_index=False)
    assert set(loaded_sz["exchange"]) == {"SZSE"}
    assert pd.to_datetime(loaded_sz["date"]).min() >= pd.Timestamp("2024-01-01")
