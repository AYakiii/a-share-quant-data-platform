from __future__ import annotations

import json

import numpy as np
import pandas as pd

from qsys.factors.factor_output import summarize_factor_output, validate_factor_output, write_factor_output


def _make_factors() -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=5, freq="D"), ["000001.SZ", "600000.SH"]],
        names=["date", "asset"],
    )
    return pd.DataFrame(
        {
            "ret_5d": np.linspace(-0.05, 0.05, len(idx)),
            "realized_vol_20d": np.linspace(0.1, 0.3, len(idx)),
        },
        index=idx,
    )


def test_valid_factor_output_returns_no_messages() -> None:
    df = _make_factors()
    msgs = validate_factor_output(df)
    assert msgs == []


def test_non_multiindex_input_produces_message() -> None:
    df = pd.DataFrame({"ret_5d": [1.0, 2.0]})
    msgs = validate_factor_output(df)
    assert any("MultiIndex" in m for m in msgs)


def test_missing_date_asset_level_names_produces_message() -> None:
    df = _make_factors().copy()
    df.index = df.index.set_names(["d", "a"])
    msgs = validate_factor_output(df)
    assert any("index must contain levels" in m for m in msgs)


def test_duplicate_index_produces_message() -> None:
    df = _make_factors().copy()
    dup = df.iloc[[0]].copy()
    dup.index = pd.MultiIndex.from_tuples([df.index[0]], names=df.index.names)
    bad = pd.concat([df, dup])
    msgs = validate_factor_output(bad)
    assert any("duplicate" in m for m in msgs)


def test_forbidden_fields_produce_message() -> None:
    df = _make_factors().copy()
    df["fwd_ret_5d"] = 0.0
    msgs = validate_factor_output(df)
    assert any("forbidden field present" in m for m in msgs)


def test_raw_input_columns_produce_message() -> None:
    df = _make_factors().copy()
    df["close"] = 1.0
    msgs = validate_factor_output(df)
    assert any("raw input column" in m for m in msgs)


def test_non_numeric_factor_column_produces_message() -> None:
    df = _make_factors().copy()
    df["note"] = "x"
    msgs = validate_factor_output(df)
    assert any("non-numeric factor column" in m for m in msgs)


def test_inf_values_produce_message() -> None:
    df = _make_factors().copy()
    df.iloc[0, 0] = np.inf
    msgs = validate_factor_output(df)
    assert any("inf/-inf" in m for m in msgs)


def test_all_nan_column_produces_message_unless_allowed() -> None:
    df = _make_factors().copy()
    df["nan_col"] = np.nan
    msgs = validate_factor_output(df)
    assert any("all-NaN" in m for m in msgs)

    msgs_ok = validate_factor_output(df, allow_all_nan_columns=True)
    assert not any("all-NaN" in m for m in msgs_ok)


def test_summarize_factor_output_returns_expected_coverage() -> None:
    df = _make_factors().copy()
    df.loc[df.index[:2], "ret_5d"] = np.nan
    summary = summarize_factor_output(df)
    row = summary[summary["factor_name"] == "ret_5d"].iloc[0]
    assert int(row["n_total"]) == len(df)
    assert int(row["n_non_null"]) == len(df) - 2
    assert abs(float(row["coverage"]) - ((len(df) - 2) / len(df))) < 1e-12


def test_write_factor_output_writes_files(tmp_path) -> None:
    df = _make_factors()
    out = write_factor_output(df, output_root=tmp_path, dataset_name="techliq_v0", metadata={"phase": "17G"})
    assert out["factors"].exists()
    assert out["metadata"].exists()
    assert out["summary"].exists()

    meta = json.loads(out["metadata"].read_text(encoding="utf-8"))
    assert meta["dataset_name"] == "techliq_v0"
    assert meta["phase"] == "17G"
    assert list(meta.keys()) == sorted(meta.keys())
