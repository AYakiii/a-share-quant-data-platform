from __future__ import annotations

import hashlib
import json
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

EASTMONEY_JGDY_DETAIL_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
JGDY_DETAIL_PAGE_SIZE = 50
JGDY_DETAIL_REQUEST_CONTRACT_VERSION = "eastmoney_rpt_org_survey_v1"
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
JGDY_DETAIL_FINGERPRINT_FIELDS = [
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
class JgdyDetailSnapshotDrift(RuntimeError):
    """Raised when live page-1 identity changes during a checkpointed crawl."""

    message: str
    checkpoint_dir: Path
    reason: str

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


def _manifest_path(checkpoint_dir: Path) -> Path:
    return checkpoint_dir / "crawl_manifest.json"


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
    for column in JGDY_DETAIL_FINGERPRINT_FIELDS:
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


def _json_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _page_1_fingerprint(raw_page: pd.DataFrame) -> str:
    """Hash stable raw page-1 business fields using sorted-key canonical JSON."""
    df = raw_page.copy()
    for column in JGDY_DETAIL_FINGERPRINT_FIELDS:
        if column not in df.columns:
            df[column] = pd.NA
    records = [
        {column: _json_scalar(row[column]) for column in JGDY_DETAIL_FINGERPRINT_FIELDS}
        for _, row in df[JGDY_DETAIL_FINGERPRINT_FIELDS].iterrows()
    ]
    canonical = json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")


def _new_manifest(since_date: str, total_pages: int, page_1_fingerprint: str, reset_reason: str = "") -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    manifest: dict[str, Any] = {
        "since_date": since_date,
        "total_pages": int(total_pages),
        "page_1_fingerprint": page_1_fingerprint,
        "created_at": now,
        "last_validated_at": now,
        "request_contract_version": JGDY_DETAIL_REQUEST_CONTRACT_VERSION,
    }
    if reset_reason:
        manifest["reset_reason"] = reset_reason
        manifest["reset_at"] = now
    return manifest


def _manifest_matches(manifest: dict[str, Any], fingerprint: str, total_pages: int) -> bool:
    return (
        str(manifest.get("page_1_fingerprint", "")) == fingerprint
        and int(manifest.get("total_pages", -1)) == int(total_pages)
        and str(manifest.get("request_contract_version", "")) == JGDY_DETAIL_REQUEST_CONTRACT_VERSION
    )


def _update_manifest_validation(path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    updated = dict(manifest)
    updated["last_validated_at"] = datetime.now(UTC).isoformat()
    _write_manifest(path, updated)
    return updated


def _archive_existing_checkpoints(checkpoint_dir: Path, reason: str) -> Path:
    archive_dir = checkpoint_dir / "_drift_archive" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    archive_dir.mkdir(parents=True, exist_ok=True)
    for path in list(checkpoint_dir.glob("page=*.parquet")) + [checkpoint_dir / "page_catalog.csv", checkpoint_dir / "crawl_manifest.json"]:
        if path.exists():
            shutil.move(str(path), str(archive_dir / path.name))
    (archive_dir / "reset_reason.txt").write_text(reason, encoding="utf-8")
    return archive_dir


def _fetch_page_with_retry(
    request_get: RequestGet,
    page: int,
    since_date: str,
    request_timeout_sec: float,
    retry_attempts: int,
    retry_sleep_sec: float,
    retry_backoff: float,
) -> tuple[pd.DataFrame, int]:
    max_attempts = max(1, int(retry_attempts))
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            payload = _request_payload(request_get, page, since_date, float(request_timeout_sec))
            return _parse_page_payload(payload)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < max_attempts:
                sleep_sec = float(retry_sleep_sec) * (float(retry_backoff) ** (attempt - 1))
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
    if last_exc is None:
        raise RuntimeError("unknown_page_fetch_error")
    raise last_exc


def _snapshot_drift_error(checkpoint_dir: Path, reason: str, detail: str) -> JgdyDetailSnapshotDrift:
    return JgdyDetailSnapshotDrift(
        f"stock_jgdy_detail_em snapshot drift: reason={reason}; checkpoint_dir={checkpoint_dir}; {detail}",
        checkpoint_dir,
        reason,
    )


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
    """Fetch stock_jgdy_detail_em with page retries and snapshot-safe checkpoints.

    Fetching is intentionally sequential and low-rate.  Each successful page is
    stored under `_operation_review/stock_jgdy_detail_em_pages/since_date=YYYYMMDD/`
    and reruns reuse readable page parquet files only after live page-1 matches
    the crawl manifest fingerprint and total page count.
    """
    since_date = _since_date_text(date)
    checkpoint_dir = _checkpoint_dir(output_root, since_date)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = checkpoint_dir / "page_catalog.csv"
    manifest_path = _manifest_path(checkpoint_dir)
    existing_catalog = _read_existing_catalog(catalog_path)
    records: list[dict[str, Any]] = existing_catalog.to_dict("records") if not existing_catalog.empty else []
    request_get = request_get or requests.get
    total_pages = _catalog_total_pages(existing_catalog)
    manifest = _read_manifest(manifest_path)
    page_frames: dict[int, pd.DataFrame] = {}
    reset_reason = ""
    prefetched_page_1: tuple[pd.DataFrame, int] | None = None

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

    existing_checkpoints = sorted(checkpoint_dir.glob("page=*.parquet"))
    if existing_checkpoints:
        if manifest is None:
            reset_reason = "missing_manifest_before_resume"
            _archive_existing_checkpoints(checkpoint_dir, reset_reason)
            records = []
            total_pages = None
        else:
            try:
                live_page_1, live_total_pages = _fetch_page_with_retry(
                    request_get,
                    1,
                    since_date,
                    float(request_timeout_sec),
                    retry_attempts,
                    retry_sleep_sec,
                    retry_backoff,
                )
            except Exception as exc:  # noqa: BLE001
                raise _snapshot_drift_error(
                    checkpoint_dir,
                    "snapshot_validation_failed_before_resume",
                    f"error_type={type(exc).__name__}; error_message={exc}",
                ) from exc
            live_fingerprint = _page_1_fingerprint(live_page_1)
            if _manifest_matches(manifest, live_fingerprint, live_total_pages):
                manifest = _update_manifest_validation(manifest_path, manifest)
                total_pages = int(manifest["total_pages"])
            else:
                reset_reason = "snapshot_drift_before_resume"
                _archive_existing_checkpoints(checkpoint_dir, reset_reason)
                records = []
                total_pages = None
                manifest = None
                prefetched_page_1 = (live_page_1, live_total_pages)

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
                if page == 1 and prefetched_page_1 is not None:
                    raw_page, observed_total_pages = prefetched_page_1
                    prefetched_page_1 = None
                else:
                    payload = _request_payload(request_get, page, since_date, float(request_timeout_sec))
                    raw_page, observed_total_pages = _parse_page_payload(payload)
                if total_pages is None:
                    total_pages = observed_total_pages
                page_fingerprint = _page_1_fingerprint(raw_page) if page == 1 else ""
                if page == 1 and manifest is None:
                    manifest = _new_manifest(since_date, observed_total_pages, page_fingerprint, reset_reason)
                    _write_manifest(manifest_path, manifest)
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

    manifest = _read_manifest(manifest_path)
    if manifest is None:
        raise _snapshot_drift_error(checkpoint_dir, "missing_manifest_before_final_success", "manifest_path=crawl_manifest.json")
    try:
        live_page_1, live_total_pages = _fetch_page_with_retry(
            request_get,
            1,
            since_date,
            float(request_timeout_sec),
            retry_attempts,
            retry_sleep_sec,
            retry_backoff,
        )
    except Exception as exc:  # noqa: BLE001
        raise _snapshot_drift_error(
            checkpoint_dir,
            "snapshot_validation_failed_before_final_success",
            f"error_type={type(exc).__name__}; error_message={exc}",
        ) from exc
    live_fingerprint = _page_1_fingerprint(live_page_1)
    if not _manifest_matches(manifest, live_fingerprint, live_total_pages):
        raise _snapshot_drift_error(
            checkpoint_dir,
            "snapshot_drift_before_final_success",
            "manifest_total_pages="
            f"{manifest.get('total_pages')}; live_total_pages={live_total_pages}; "
            f"manifest_page_1_fingerprint={manifest.get('page_1_fingerprint')}; "
            f"live_page_1_fingerprint={live_fingerprint}",
        )
    manifest = _update_manifest_validation(manifest_path, manifest)

    frames = [pd.read_parquet(_page_path(checkpoint_dir, page)) for page in range(1, total_pages + 1)]
    combined = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=JGDY_DETAIL_COLUMNS)
    combined["序号"] = range(1, len(combined) + 1)
    return JgdyDetailResult(combined[JGDY_DETAIL_COLUMNS], checkpoint_dir, catalog_path, int(manifest["total_pages"]), len(completed_pages))
