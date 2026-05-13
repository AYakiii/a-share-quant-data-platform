from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd


DATE_PAT = re.compile(r"date|日期|时间|day|month|year", re.IGNORECASE)
SYMBOL_PAT = re.compile(r"symbol|代码|证券|股票|ticker|指数", re.IGNORECASE)
ANN_PAT = re.compile(r"公告|披露|ann|notice|meeting|调研", re.IGNORECASE)


def safe_filename(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return re.sub(r"[^\w\-\.\u4e00-\u9fa5]+", "_", normalized).strip("_")


def detect_date_like_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if DATE_PAT.search(c)]


def detect_symbol_like_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if SYMBOL_PAT.search(c)]


def detect_announcement_like_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if ANN_PAT.search(c)]


def summarize_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    columns = [str(c) for c in df.columns]
    date_like = detect_date_like_columns(columns)
    symbol_like = detect_symbol_like_columns(columns)
    ann_like = detect_announcement_like_columns(columns)
    return {
        "rows": int(len(df)),
        "n_cols": int(df.shape[1]),
        "columns": columns,
        "date_like_columns": date_like,
        "symbol_like_columns": symbol_like,
        "announcement_like_columns": ann_like,
        "has_date_like_column": bool(date_like),
        "has_symbol_like_column": bool(symbol_like),
        "has_announcement_like_column": bool(ann_like),
    }
