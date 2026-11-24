"""API 测试：按日取文件、失败时 API 仍返回上一版、下载与行数接口。"""

from __future__ import annotations

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app import api
from app.publisher import publish
from conftest import utc, write_parquet


def _client(settings):
    api.get_settings = lambda: settings  # type: ignore[method-assign]
    api.get_service = lambda: api.DatasetService(settings)  # type: ignore[method-assign]
    return TestClient(api.app)


def test_full_api_flow_and_failure_retains_version(settings):
    client = _client(settings)

    # 未发布时
    assert client.get("/health").json()["current_version"] is None
    assert client.get("/manifest").json()["manifest"] is None
    assert client.get("/days/2026-11-23/files").status_code == 404

    # 第一版：同日文件（快路径）
    write_parquet(settings.input_dir / "a.parquet", [
        ("T1", utc("2026-11-23 09:00:00"), 100),
        ("T2", utc("2026-11-23 21:00:00"), 200),
    ])
    r = client.post("/publish", json={"files": ["a.parquet"]})
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 1
    assert r.json()["outcomes"][0]["mode"] == "reuse_footer"

    r = client.get("/days/2026-11-23/files")
    assert r.status_code == 200
    payload = r.json()
    assert payload["version"] == 1
    assert len(payload["files"]) == 1
    entry = payload["files"][0]
    assert entry["source"] == "original"
    assert entry["rows"] == 2

    # rows 接口
    r = client.get("/days/2026-11-23/rows")
    assert r.status_code == 200
    assert r.json()["rows_total"] == 2
    assert [x["ticket_id"] for x in r.json()["records"]] == ["T1", "T2"]

    # 下载原档
    url = entry["download_url"]
    r = client.get(url)
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.apache.parquet"
    assert pq.read_table(
        __import__("io").BytesIO(r.content)
    ).num_rows == 2

    # 第二版：加入跨日文件
    write_parquet(settings.input_dir / "b.parquet", [
        ("B1", utc("2026-11-23 22:00:00"), 1),
        ("B2", utc("2026-11-24 03:00:00"), 2),
    ])
    r = client.post("/publish", json={"files": ["a.parquet", "b.parquet"]})
    assert r.status_code == 201
    assert r.json()["version"] == 2
    assert r.json()["day_count"] == 2

    r = client.get("/days/2026-11-24/files")
    assert r.json()["version"] == 2
    assert r.json()["files"][0]["source"] == "split"
    r = client.get("/days/2026-11-24/rows")
    assert [x["ticket_id"] for x in r.json()["records"]] == ["B2"]

    # 失败场景：含空时间 → 422，且按日 API 仍是 v2
    write_parquet(settings.input_dir / "bad.parquet", [("X", None, 9)])
    r = client.post("/publish", json={"files": ["a.parquet", "bad.parquet"]})
    assert r.status_code == 422
    assert r.json()["detail"]["retained_version"] == 2
    r = client.get("/manifest")
    assert r.json()["manifest"]["version"] == 2
    r = client.get("/days/2026-11-23/files")
    assert r.json()["version"] == 2

    # 非法日期格式
    assert client.get("/days/2026-13-01/files").status_code == 422
    assert client.get("/days/not-a-date/files").status_code == 422


def test_empty_day_returns_empty_list_not_old_or_fake(settings):
    client = _client(settings)
    write_parquet(settings.input_dir / "a.parquet", [
        ("T1", utc("2026-11-23 09:00:00"), 1),
    ])
    client.post("/publish", json={"files": ["a.parquet"]})
    r = client.get("/days/2026-12-01/files")
    assert r.status_code == 200
    assert r.json()["files"] == []
    r = client.get("/days/2026-12-01/rows")
    assert r.status_code == 200
    assert r.json()["rows_total"] == 0
    assert r.json()["records"] == []


def test_publish_validates_request_body(settings):
    client = _client(settings)
    assert client.post("/publish", json={"files": []}).status_code == 422
    assert client.post("/publish", json={}).status_code == 422


def test_double_publish_is_idempotent_and_bumps_version(settings):
    client = _client(settings)
    write_parquet(settings.input_dir / "a.parquet", [
        ("T1", utc("2026-11-23 09:00:00"), 1),
    ])
    v1 = client.post("/publish", json={"files": ["a.parquet"]}).json()["version"]
    v2 = client.post("/publish", json={"files": ["a.parquet"]}).json()["version"]
    assert (v1, v2) == (1, 2)
    # 两版内容一致；v2 同样走快路径
    assert client.get("/days/2026-11-23/files").json()["version"] == 2
