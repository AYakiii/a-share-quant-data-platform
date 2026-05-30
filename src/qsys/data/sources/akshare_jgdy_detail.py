from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

EASTMONEY_JGDY_DETAIL_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
JGDY_DETAIL_PAGE_SIZE = 50
JGDY_DETAIL_COLUMNS = [
    "序号",
    "代码",
    "名称",
    "最新价",
    "涨跌幅",
    "调研机构",
    "机构类型",
    "调研人员",
    "接待方式",
    "接待人员",
    "接待地点",
    "调研日期",
    "公告日期",
]

RequestGet = Callable[..., Any]


@dataclass
class JgdyDetailPageFailure(RuntimeError):
    """Raised when one or more JGDY detail pages cannot be acquired."""

    message: str
    total_pages: int
    completed_pages: int
    failed_pages: list[int]
    checkpoint_dir: Path

    def __str__(self) -> str:
        return self.message


@dataclass
class JgdyDetailResult:
    """Raw result plus page-level checkpoint metadata for the official runner."""

    raw: pd.DataFrame
    checkpoint_dir: Path
    catalog_path: Path
    total_pages: int
    completed_pages: int


def _since_date_text(date: str) -> str:
    text = "".join(ch for ch in str(date) if ch.isdigit())
    if len(text) != 8:
        raise ValueError(f"stock_jgdy_detail_em requires YYYYMMDD date, got {date!r}")
    return text


def _eastmoney_date(date: str) -> str:
    text = _since_date_text(date)
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def _checkpoint_dir(output_root: str | Path, since_date: str) -> Path:
    return Path(output_root) / "_operation_review" / "stock_jgdy_detail_em_pages" / f"since_date={since_date}"


def _page_path(checkpoint_dir: Path, page: int) -> Path:
    return checkpoint_dir / f"page={page:06d}.parquet"


def _base_params(date: str, page_number: int) -> dict[str, str]:
    return {
        "sortColumns": "NOTICE_DATE,RECEIVE_START_DATE,SECURITY_CODE,NUMBERNEW",
        "sortTypes": "-1,-1,1,-1",
        "pageSize": str(JGDY_DETAIL_PAGE_SIZE),
        "pageNumber": str(page_number),
        "reportName": "RPT_ORG_SURVEY",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NOTICE_DATE,RECEIVE_START_DATE,"
        "RECEIVE_OBJECT,RECEIVE_PLACE,RECEIVE_WAY_EXPLAIN,INVESTIGATORS,RECEPTIONIST,ORG_TYPE",
        "quoteColumns": "f2~01~SECURITY_CODE~CLOSE_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE",
        "quoteType": "0",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(IS_SOURCE="1")(RECEIVE_START_DATE>\'{_eastmoney_date(date)}\')',
    }


def _parse_page_payload(payload: Any) -> tuple[pd.DataFrame, int]:
    if not isinstance(payload, dict):
        raise ValueError(f"malformed_page_payload: response is {type(payload).__name__}, expected dict")
    result = payload.get("result")
    if result is None:
        raise ValueError("malformed_page_payload: result is None or missing")
    if not isinstance(result, dict):
        raise ValueError(f"malformed_page_payload: result is {type(result).__name__}, expected dict")
    if "data" not in result:
        raise ValueError("malformed_page_payload: result.data is missing")
    data = result.get("data")
    if data is None:
        raise ValueError("malformed_page_payload: result.data is None")
    total_pages = int(result.get("pages") or 1)
    return pd.DataFrame(data), total_pages


def _normalize_page_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=JGDY_DETAIL_COLUMNS)
    df = frame.reset_index(drop=True).copy()
    columns = [
        "SECUCODE",
        "SECURITY_CODE",
        "SECURITY_NAME_ABBR",
        "NOTICE_DATE",
        "RECEIVE_START_DATE",
        "RECEIVE_OBJECT",
        "RECEIVE_PLACE",
        "RECEIVE_WAY_EXPLAIN",
        "INVESTIGATORS",
        "RECEPTIONIST",
        "ORG_TYPE",
        "CLOSE_PRICE",
        "CHANGE_RATE",
    ]
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    out = pd.DataFrame(
        {
            "序号": pd.NA,
            "代码": df["SECURITY_CODE"],
            "名称": df["SECURITY_NAME_ABBR"],
            "最新价": pd.to_numeric(df["CLOSE_PRICE"], errors="coerce"),
            "涨跌幅": pd.to_numeric(df["CHANGE_RATE"], errors="coerce"),
            "调研机构": df["RECEIVE_OBJECT"],
            "机构类型": df["ORG_TYPE"],
            "调研人员": df["INVESTIGATORS"],
            "接待方式": df["RECEIVE_WAY_EXPLAIN"],
            "接待人员": df["RECEPTIONIST"],
            "接待地点": df["RECEIVE_PLACE"],
            "调研日期": pd.to_datetime(df["RECEIVE_START_DATE"], errors="coerce").dt.date,
            "公告日期": pd.to_datetime(df["NOTICE_DATE"], errors="coerce").dt.date,
        }
    )
    return out[JGDY_DETAIL_COLUMNS]


def _read_existing_catalog(catalog_path: Path) -> pd.DataFrame:
    if not catalog_path.exists():
        return pd.DataFrame()
    return pd.read_csv(catalog_path)


def _catalog_total_pages(catalog: pd.DataFrame) -> int | None:
    if catalog.empty or "total_pages" not in catalog.columns:
        return None
    values = pd.to_numeric(catalog["total_pages"], errors="coerce").dropna()
    if values.empty:
        return None
    return int(values.max())


def _write_catalog(catalog_path: Path, records: list[dict[str, Any]]) -> None:
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values(["page", "finished_at"], kind="mergesort").drop_duplicates(subset=["page"], keep="last")
    df.to_csv(catalog_path, index=False, encoding="utf-8-sig")


def _request_payload(request_get: RequestGet, page: int, date: str, timeout: float) -> Any:
    response = request_get(EASTMONEY_JGDY_DETAIL_URL, params=_base_params(date, page), timeout=timeout)
    if isinstance(response, dict):
        return response
    json_method = getattr(response, "json", None)
    if not callable(json_method):
        raise ValueError("malformed_page_payload: response has no json() method")
    return json_method()


def _load_checkpoint(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:  # noqa: BLE001
        return None


def fetch_stock_jgdy_detail_em_resilient(
    date: str,
    output_root: str | Path,
    *,
    request_get: RequestGet | None = None,
    retry_attempts: int = 3,
    retry_sleep_sec: float = 0.5,
    retry_backoff: float = 2.0,
    request_sleep_sec: float = 0.1,
    request_timeout_sec: float = 30.0,
) -> JgdyDetailResult:
    """Fetch stock_jgdy_detail_em with page retries and parquet checkpoints.

    Fetching is intentionally sequential and low-rate.  Each successful page is
    stored under `_operation_review/stock_jgdy_detail_em_pages/since_date=YYYYMMDD/`
    and reruns reuse readable page parquet files before issuing network calls.
    """
    since_date = _since_date_text(date)
    checkpoint_dir = _checkpoint_dir(output_root, since_date)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = checkpoint_dir / "page_catalog.csv"
    existing_catalog = _read_existing_catalog(catalog_path)
    records: list[dict[str, Any]] = existing_catalog.to_dict("records") if not existing_catalog.empty else []
    request_get = request_get or requests.get
    total_pages = _catalog_total_pages(existing_catalog)
    page_frames: dict[int, pd.DataFrame] = {}

    def record_page(page: int, status: str, rows: int, retry_count: int, started_at: datetime, error_type: str = "", error_message: str = "") -> None:
        finished_at = datetime.now(UTC)
        records.append(
            {
                "since_date": since_date,
                "page": page,
                "total_pages": total_pages or "",
                "rows": rows,
                "status": status,
                "retry_count": retry_count,
                "error_type": error_type,
                "error_message": error_message,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
            }
        )
        _write_catalog(catalog_path, records)

    page = 1
    max_attempts = max(1, int(retry_attempts))
    while total_pages is None or page <= total_pages:
        started_at = datetime.now(UTC)
        existing_frame = _load_checkpoint(_page_path(checkpoint_dir, page))
        if existing_frame is not None:
            page_frames[page] = existing_frame
            record_page(page, "reused", len(existing_frame), 0, started_at)
            page += 1
            continue

        last_error_type = ""
        last_error_message = ""
        for attempt in range(1, max_attempts + 1):
            try:
                payload = _request_payload(request_get, page, since_date, float(request_timeout_sec))
                raw_page, observed_total_pages = _parse_page_payload(payload)
                if total_pages is None:
                    total_pages = observed_total_pages
                elif observed_total_pages and observed_total_pages != total_pages:
                    total_pages = max(total_pages, observed_total_pages)
                normalized = _normalize_page_frame(raw_page)
                normalized.to_parquet(_page_path(checkpoint_dir, page), index=False)
                page_frames[page] = normalized
                record_page(page, "success", len(normalized), attempt - 1, started_at)
                break
            except Exception as exc:  # noqa: BLE001
                last_error_type = type(exc).__name__
                last_error_message = str(exc)
                if attempt < max_attempts:
                    sleep_sec = float(retry_sleep_sec) * (float(retry_backoff) ** (attempt - 1))
                    if sleep_sec > 0:
                        time.sleep(sleep_sec)
                else:
                    record_page(page, "failed", 0, attempt - 1, started_at, last_error_type, last_error_message)
        if page not in page_frames:
            break
        if request_sleep_sec > 0 and (total_pages is None or page < total_pages):
            time.sleep(request_sleep_sec)
        page += 1

    if total_pages is None:
        total_pages = max(page_frames) if page_frames else 0
    if total_pages <= 0 and not page_frames:
        message = (
            "stock_jgdy_detail_em incomplete page crawl: "
            f"total_pages={total_pages}; completed_pages=0; "
            f"failed_pages={[page]}; checkpoint_dir={checkpoint_dir}"
        )
        raise JgdyDetailPageFailure(message, total_pages, 0, [page], checkpoint_dir)
    expected_pages = set(range(1, total_pages + 1))
    completed_pages = sorted(page for page in expected_pages if _load_checkpoint(_page_path(checkpoint_dir, page)) is not None)
    failed_pages = sorted(expected_pages.difference(completed_pages))
    if failed_pages:
        message = (
            "stock_jgdy_detail_em incomplete page crawl: "
            f"total_pages={total_pages}; completed_pages={len(completed_pages)}; "
            f"failed_pages={failed_pages}; checkpoint_dir={checkpoint_dir}"
        )
        raise JgdyDetailPageFailure(message, total_pages, len(completed_pages), failed_pages, checkpoint_dir)

    frames = [pd.read_parquet(_page_path(checkpoint_dir, page)) for page in range(1, total_pages + 1)]
    combined = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=JGDY_DETAIL_COLUMNS)
    combined["序号"] = range(1, len(combined) + 1)
    return JgdyDetailResult(combined[JGDY_DETAIL_COLUMNS], checkpoint_dir, catalog_path, total_pages, len(completed_pages))
