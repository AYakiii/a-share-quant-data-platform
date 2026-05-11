"""Static Phase 17S/17T contract loaders and validators."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

INDUSTRY_COLUMNS = [
    "source_api",
    "source_family",
    "date_field",
    "symbol_field",
    "industry_code_field",
    "industry_name_field",
    "asof_rule",
    "usable_for_historical_backtest",
    "known_limitations",
    "forbidden_usage",
    "notes",
]

EVENT_COLUMNS = [
    "source_api",
    "source_family",
    "event_type",
    "primary_event_date",
    "announcement_date_field",
    "effective_date_field",
    "suggested_lookback_windows",
    "suggested_active_window",
    "post_event_outcome_fields",
    "pit_requirement",
    "builder_readiness",
    "forbidden_usage",
    "notes",
]


def load_industry_asof_contract(path: str | Path | None = None) -> pd.DataFrame:
    fp = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "industry_asof_contract_v0.csv"
    )
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def load_event_window_contract(path: str | Path | None = None) -> pd.DataFrame:
    fp = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[3] / "config" / "factor_sources" / "event_window_contract_v0.csv"
    )
    return pd.read_csv(fp, dtype=str, keep_default_na=False)


def validate_industry_asof_contract(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []
    missing = [c for c in INDUSTRY_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["source_api", "source_family", "date_field", "symbol_field", "industry_code_field", "asof_rule", "usable_for_historical_backtest"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")
    return msgs


def validate_event_window_contract(df: pd.DataFrame) -> list[str]:
    msgs: list[str] = []
    missing = [c for c in EVENT_COLUMNS if c not in df.columns]
    if missing:
        msgs.append(f"Missing required columns: {missing}")

    for c in ["source_api", "source_family", "event_type", "primary_event_date", "pit_requirement", "builder_readiness"]:
        if c in df.columns:
            bad = df[c].astype(str).str.strip() == ""
            for i in df.index[bad].tolist():
                msgs.append(f"Row {int(i)}: {c} is empty")

    needed = {"post_event_outcome_fields", "forbidden_usage"}
    if needed.issubset(df.columns):
        for i, row in df.iterrows():
            outcome = str(row["post_event_outcome_fields"]).strip()
            forbidden = str(row["forbidden_usage"]).strip()
            if outcome and not forbidden:
                msgs.append(f"Row {int(i)}: forbidden_usage must be non-empty when post_event_outcome_fields is non-empty")

    if {"source_api", "post_event_outcome_fields"}.issubset(df.columns):
        lhb = df[df["source_api"] == "stock_lhb_detail_em"]
        if lhb.empty:
            msgs.append("stock_lhb_detail_em must exist")
        else:
            need = ["上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"]
            got = str(lhb.iloc[0]["post_event_outcome_fields"])
            for field in need:
                if field not in got:
                    msgs.append("stock_lhb_detail_em must list 上榜后1日, 上榜后2日, 上榜后5日, 上榜后10日")
                    break

        unlock = df[df["source_api"] == "stock_restricted_release_detail_em"]
        if unlock.empty:
            msgs.append("stock_restricted_release_detail_em must exist")
        elif "解禁后20日涨跌幅" not in str(unlock.iloc[0]["post_event_outcome_fields"]):
            msgs.append("stock_restricted_release_detail_em must list 解禁后20日涨跌幅")

    return msgs
