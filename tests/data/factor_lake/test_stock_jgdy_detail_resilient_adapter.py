from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.raw_ingest import run_raw_coverage_ingest


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _page_payload(page: int, total_pages: int, variant: str = "") -> dict:
    return {
        "result": {
            "pages": total_pages,
            "data": [
                {
                    "SECUCODE": f"00000{page}.SZ",
                    "SECURITY_CODE": f"00000{page}",
                    "SECURITY_NAME_ABBR": f"测试{page}{variant}",
                    "NOTICE_DATE": "2026-01-02 00:00:00",
                    "RECEIVE_START_DATE": "2026-01-01 00:00:00",
                    "RECEIVE_OBJECT": f"机构{page}{variant}",
                    "RECEIVE_PLACE": "会议室",
                    "RECEIVE_WAY_EXPLAIN": "现场调研",
                    "INVESTIGATORS": "张三",
                    "RECEPTIONIST": "李四",
                    "ORG_TYPE": "基金公司",
                    "CLOSE_PRICE": str(10 + page),
                    "CHANGE_RATE": str(page / 10),
                }
            ],
        }
    }


def _adapter_map(fake_get):
    return {
        "__stock_jgdy_detail_em_request_get__": fake_get,
        "__stock_jgdy_detail_em_config__": {
            "retry_attempts": 2,
            "retry_sleep_sec": 0.0,
            "request_sleep_sec": 0.0,
        },
    }


def _run(tmp_path: Path, fake_get, date: str = "20251231", resume: bool = False) -> dict:
    return run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["disclosure_ir"],
        report_dates=[date],
        selected_api_names=["stock_jgdy_detail_em"],
        adapter_map=_adapter_map(fake_get),
        include_disabled=True,
        max_workers=1,
        resume=resume,
    )


def test_stock_jgdy_detail_resilient_success_multipage(tmp_path):
    calls: list[int] = []

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        calls.append(page)
        assert params["reportName"] == "RPT_ORG_SURVEY"
        assert params["pageSize"] == "50"
        assert params["filter"] == '(IS_SOURCE="1")(RECEIVE_START_DATE>\'2025-12-31\')'
        return _FakeResponse(_page_payload(page, 3))

    out = _run(tmp_path, fake_get)

    [row] = out["rows"]
    assert row["status"] == "success"
    assert row["rows"] == 3
    assert calls == [1, 2, 3, 1]
    assert json.loads(row["partition_json"]) == {"since_date": "20251231"}
    assert Path(row["output_path"]).as_posix().endswith("since_date=20251231/data.parquet")

    checkpoint_dir = tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231"
    assert (checkpoint_dir / "page=000001.parquet").exists()
    assert (checkpoint_dir / "page=000002.parquet").exists()
    assert (checkpoint_dir / "page=000003.parquet").exists()
    catalog = pd.read_csv(checkpoint_dir / "page_catalog.csv")
    assert set(catalog["page"]) == {1, 2, 3}
    assert set(catalog["status"]) == {"success"}


def test_stock_jgdy_detail_resilient_retry_recovers_none_like_page(tmp_path):
    attempts = {2: 0}

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        if page == 2:
            attempts[2] += 1
            if attempts[2] == 1:
                return _FakeResponse({"result": None})
        return _FakeResponse(_page_payload(page, 3))

    out = _run(tmp_path, fake_get)

    [row] = out["rows"]
    assert row["status"] == "success"
    catalog = pd.read_csv(tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231" / "page_catalog.csv")
    page2 = catalog[catalog["page"] == 2].iloc[0]
    assert int(page2["retry_count"]) == 1
    assert page2["status"] == "success"


def test_stock_jgdy_detail_resilient_incomplete_is_failed_and_recoverable(tmp_path):
    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        if page == 3:
            return _FakeResponse({"result": None})
        return _FakeResponse(_page_payload(page, 3))

    out = _run(tmp_path, fake_get)

    [row] = out["rows"]
    assert row["status"] == "failed"
    assert row["error_type"] == "JgdyDetailPageFailure"
    assert "total_pages=3" in row["error_message"]
    assert "completed_pages=2" in row["error_message"]
    assert "failed_pages=[3]" in row["error_message"]
    assert "stock_jgdy_detail_em_pages/since_date=20251231" in row["error_message"]
    checkpoint_dir = tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231"
    assert (checkpoint_dir / "page=000001.parquet").exists()
    assert (checkpoint_dir / "page=000002.parquet").exists()
    assert not (checkpoint_dir / "page=000003.parquet").exists()
    recovery = pd.read_csv(tmp_path / "_operation_review" / "recovery_tasks.csv")
    assert set(recovery["status"]) == {"failed"}
    assert "failed_pages=[3]" in recovery.iloc[0]["error_message"]


def test_stock_jgdy_detail_resilient_page_level_resume_validates_snapshot_and_fetches_missing_page(tmp_path):
    phase = {"name": "first"}
    calls: list[tuple[str, int]] = []

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        calls.append((phase["name"], page))
        if phase["name"] == "first" and page == 3:
            return _FakeResponse({"result": None})
        return _FakeResponse(_page_payload(page, 3))

    first = _run(tmp_path, fake_get)
    assert first["rows"][0]["status"] == "failed"
    phase["name"] = "second"
    second = _run(tmp_path, fake_get, resume=True)

    assert second["rows"][0]["status"] == "success"
    assert [page for name, page in calls if name == "second"] == [1, 3, 1]
    catalog = pd.read_csv(tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231" / "page_catalog.csv")
    assert set(catalog["status"]) == {"reused", "success"}
    recovery_text = (tmp_path / "_operation_review" / "recovery_tasks.csv").read_text(encoding="utf-8-sig")
    assert "stock_jgdy_detail_em" not in recovery_text


def test_stock_jgdy_detail_resilient_stable_since_date_partitions_do_not_collide(tmp_path):
    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        return _FakeResponse(_page_payload(1, 1))

    first = _run(tmp_path, fake_get, date="20251231")
    second = _run(tmp_path, fake_get, date="20260131")

    first_path = Path(first["rows"][0]["output_path"])
    second_path = Path(second["rows"][0]["output_path"])
    assert "since_date=20251231" in first_path.as_posix()
    assert "since_date=20260131" in second_path.as_posix()
    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()


def test_stock_jgdy_detail_resilient_resume_snapshot_drift_resets_checkpoints(tmp_path):
    phase = {"name": "first"}
    calls: list[tuple[str, int]] = []

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        calls.append((phase["name"], page))
        if phase["name"] == "first":
            if page == 3:
                return _FakeResponse({"result": None})
            return _FakeResponse(_page_payload(page, 3, "A"))
        return _FakeResponse(_page_payload(page, 3, "B"))

    first = _run(tmp_path, fake_get)
    assert first["rows"][0]["status"] == "failed"
    checkpoint_dir = tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231"
    old_page_1 = pd.read_parquet(checkpoint_dir / "page=000001.parquet")
    assert old_page_1.iloc[0]["名称"] == "测试1A"

    phase["name"] = "second"
    second = _run(tmp_path, fake_get, resume=True)

    assert second["rows"][0]["status"] == "success"
    assert [page for name, page in calls if name == "second"] == [1, 2, 3, 1]
    new_page_1 = pd.read_parquet(checkpoint_dir / "page=000001.parquet")
    assert new_page_1.iloc[0]["名称"] == "测试1B"
    manifest = json.loads((checkpoint_dir / "crawl_manifest.json").read_text(encoding="utf-8"))
    assert manifest["reset_reason"] == "snapshot_drift_before_resume"
    assert (checkpoint_dir / "_drift_archive").exists()


def test_stock_jgdy_detail_resilient_final_snapshot_drift_fails_without_success_parquet(tmp_path):
    call_count = {1: 0}

    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        page = int(params["pageNumber"])
        if page == 1:
            call_count[1] += 1
            if call_count[1] >= 2:
                return _FakeResponse(_page_payload(1, 2, "B"))
        return _FakeResponse(_page_payload(page, 2, "A"))

    out = _run(tmp_path, fake_get)

    [row] = out["rows"]
    assert row["status"] == "failed"
    assert row["error_type"] == "JgdyDetailSnapshotDrift"
    assert "snapshot_drift_before_final_success" in row["error_message"]
    assert "checkpoint_dir=" in row["error_message"]
    assert not row["output_path"]
    checkpoint_dir = tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231"
    assert (checkpoint_dir / "page=000001.parquet").exists()
    assert (checkpoint_dir / "crawl_manifest.json").exists()


def test_stock_jgdy_detail_resilient_manifest_has_deterministic_page_1_fingerprint(tmp_path):
    def fake_get(url: str, params: dict, timeout: float):  # noqa: ARG001
        return _FakeResponse(_page_payload(1, 1))

    out = _run(tmp_path, fake_get)
    assert out["rows"][0]["status"] == "success"

    checkpoint_dir = tmp_path / "_operation_review" / "stock_jgdy_detail_em_pages" / "since_date=20251231"
    manifest = json.loads((checkpoint_dir / "crawl_manifest.json").read_text(encoding="utf-8"))
    expected_records = [
        {
            "SECUCODE": "000001.SZ",
            "SECURITY_CODE": "000001",
            "SECURITY_NAME_ABBR": "测试1",
            "NOTICE_DATE": "2026-01-02 00:00:00",
            "RECEIVE_START_DATE": "2026-01-01 00:00:00",
            "RECEIVE_OBJECT": "机构1",
            "RECEIVE_PLACE": "会议室",
            "RECEIVE_WAY_EXPLAIN": "现场调研",
            "INVESTIGATORS": "张三",
            "RECEPTIONIST": "李四",
            "ORG_TYPE": "基金公司",
            "CLOSE_PRICE": "11",
            "CHANGE_RATE": "0.1",
        }
    ]
    expected_fingerprint = hashlib.sha256(
        json.dumps(expected_records, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert manifest["since_date"] == "20251231"
    assert manifest["total_pages"] == 1
    assert manifest["page_1_fingerprint"] == expected_fingerprint
    assert manifest["request_contract_version"] == "eastmoney_rpt_org_survey_v1"
    assert manifest["created_at"]
    assert manifest["last_validated_at"]
