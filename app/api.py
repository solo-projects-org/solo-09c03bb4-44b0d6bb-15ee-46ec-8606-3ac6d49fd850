"""FastAPI 应用：按 UTC 过磅日取文件/取数，以及触发发布换版。"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .config import Settings
from .publisher import PublishError, publish
from .service import DatasetService
from .timeutil import parse_date

app = FastAPI(
    title="码头过磅明细按日数据集",
    version="1.0.0",
    description="按过磅时间的 UTC 日期发布/读取 Parquet；失败保留上一版清单。",
)


def get_settings() -> Settings:
    return Settings.from_env()


def get_service() -> DatasetService:
    return DatasetService(get_settings())


class PublishRequest(BaseModel):
    files: list[str] = Field(
        ..., min_length=1, description="相对 input_dir 的同结构 Parquet 路径清单"
    )


class OutcomeOut(BaseModel):
    input_file: str
    mode: str
    days: list[str]
    rows: int


class PublishResponse(BaseModel):
    version: int
    created_at: str
    time_column: str
    input_files: list[str]
    day_count: int
    outcomes: list[OutcomeOut]


@app.get("/health")
def health():
    service = get_service()
    manifest = service.current_manifest()
    return {
        "status": "ok",
        "current_version": manifest.version if manifest else None,
        "input_dir": str(service.settings.input_dir),
        "output_dir": str(service.settings.output_dir),
    }


@app.get("/manifest")
def get_manifest():
    manifest = get_service().current_manifest()
    if manifest is None:
        return {"message": "尚未发布任何版本", "manifest": None}
    return {"manifest": manifest.to_dict()}


@app.get("/days/{day}/files")
def list_day_files(day: str):
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    service = get_service()
    manifest = service.current_manifest()
    if manifest is None:
        # 无版本可返回：明确告知，而不是伪造空清单
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    entries = manifest.files_for_date(day)
    return {
        "version": manifest.version,
        "date": day,
        "files": [
            {
                "path": e.path,
                "source": e.source,
                "source_file": e.source_file,
                "rows": e.rows,
                "download_url": f"/days/{day}/files/{e.source}/{_b64(e.path)}",
            }
            for e in entries
        ],
    }


@app.get("/days/{day}/files/{source}/{name}")
def download_day_file(day: str, source: str, name: str):
    if source not in ("original", "split"):
        raise HTTPException(status_code=404, detail="source 仅支持 original/split")
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    import base64

    try:
        padded = name + "=" * (-len(name) % 4)
        rel_path = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=404, detail="文件名编码无效")
    service = get_service()
    manifest = service.current_manifest()
    if manifest is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    for entry in manifest.files_for_date(day):
        if entry.source == source and entry.path == rel_path:
            fs_path = service.resolve(entry)
            if not fs_path.is_file():
                raise HTTPException(status_code=410, detail="清单文件已丢失")
            return FileResponse(
                fs_path,
                media_type="application/vnd.apache.parquet",
                filename=fs_path.name,
            )
    raise HTTPException(status_code=404, detail="当前版本清单中无此文件")


@app.get("/days/{day}/rows")
def read_day_rows(
    day: str,
    limit: int = Query(default=10_000, ge=1, le=1_000_000),
):
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    service = get_service()
    if service.current_manifest() is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    table = service.read_date(day)
    if table is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    truncated = table.slice(0, limit)
    return {
        "date": day,
        "version": service.current_manifest().version,
        "rows_returned": truncated.num_rows,
        "rows_total": table.num_rows,
        "columns": table.column_names,
        "records": truncated.to_pylist(),
    }


@app.post("/publish", response_model=PublishResponse, status_code=201)
def publish_files(req: PublishRequest):
    settings = get_settings()
    try:
        result = publish(req.files, settings)
    except PublishError as exc:
        # 发布失败：current.json 未被替换，按日 API 仍读到上一版
        manifest = DatasetService(settings).current_manifest()
        raise HTTPException(
            status_code=422,
            detail={
                "error": str(exc),
                "retained_version": manifest.version if manifest else None,
            },
        )
    m = result.manifest
    return PublishResponse(
        version=m.version,
        created_at=m.created_at,
        time_column=m.time_column,
        input_files=m.input_files,
        day_count=len({d.date for d in m.days}),
        outcomes=[OutcomeOut(**vars(o)) for o in result.outcomes],
    )


def _b64(s: str) -> str:
    import base64

    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")
