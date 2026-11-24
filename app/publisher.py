"""发布核心。

处理每个输入 Parquet 的三档路径：
1. 页脚（footer 统计信息）足以证明「时间列非空且全部同日」→ 直接登记原档，
   完全不读数据行（不调用 read_table）。
2. 页脚证明不了（无统计/字符串统计无法判定等）→ 只读时间列明细确认：
   非空且同日仍复用原档；跨日才读全表拆写。
3. 页脚已能证明跨日 → 直接进入拆写。

任何输入文件出现空时间（null/空白/无法解析）都会中止本次发布，current.json
保持上一版不动；新拆写文件全部落在 staged/v<新版本号>/ 下，清单最后才换版。
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import fcntl
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import Settings
from .manifest import DayFile, Manifest, ManifestStore
from .timeutil import TimeColumnError, date_to_str, to_utc_date_array

# 页脚判定三态
SAME_DAY = "same_day"
MIXED = "mixed"
EMPTY_TIME = "empty_time"
UNKNOWN = "unknown"  # 页脚证明不了，需要读明细


class PublishError(RuntimeError):
    """发布失败；触发后上一版清单继续生效。"""


@dataclasses.dataclass
class FileOutcome:
    input_file: str
    mode: str  # "reuse_footer" | "reuse_scan" | "split"
    days: list[str] = dataclasses.field(default_factory=list)
    rows: int = 0


@dataclasses.dataclass
class PublishResult:
    manifest: Manifest
    outcomes: list[FileOutcome]


class _FileLock:
    def __init__(self, path: Path, timeout: float):
        self._path = path
        self._timeout = timeout
        self._fd: int | None = None

    def __enter__(self) -> "_FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        deadline = time.monotonic() + max(self._timeout, 0.0)
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise PublishError("另一个发布任务正在执行，获取锁超时")
                time.sleep(0.1)

    def __exit__(self, *exc) -> None:
        assert self._fd is not None
        with contextlib.suppress(OSError):
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


# ---------------------------------------------------------------- 页脚判定

def _footer_scalar_to_date(value, field_type: pa.DataType, settings: Settings) -> _dt.date | None:
    """把页脚统计的 min/max 标量转成 UTC 日期；无法判定返回 None。"""
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(_dt.timezone.utc).date()
        return value.date()  # 朴素时间戳按 UTC 存储约定取字面日期
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        # 物理 INT64 但未转成 datetime 的统计：借箭头列类型还原
        try:
            if pa.types.is_timestamp(field_type):
                scalar = pa.scalar(value, type=field_type)
            elif pa.types.is_integer(field_type) or pa.types.is_floating(field_type):
                scalar = pc.cast(pa.scalar(value, type=field_type), pa.timestamp("s", "UTC"))
            else:
                return None
            return pc.cast(scalar, pa.date32()).as_py()
        except (pa.ArrowException, TypeError):
            return None
    if isinstance(value, float):
        return None
    if isinstance(value, str):
        return _footer_string_to_date(value, settings)
    return None


def _footer_string_to_date(value: str, settings: Settings) -> _dt.date | None:
    if not value.strip():
        return None
    formats = (settings.time_string_format,) if settings.time_string_format else (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return _dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def footer_verdict(
    pf: pq.ParquetFile, settings: Settings
) -> tuple[str, _dt.date | None]:
    """只看页脚元数据/统计信息，不读任何数据行。

    返回 (状态, 同日时的日期)。状态为 UNKNOWN 表示页脚不足以证明，
    调用方需要降级读明细。
    """
    meta = pf.metadata
    field_index = meta.schema.names.index(settings.time_column)
    arrow_type = pf.schema_arrow.field(settings.time_column).type
    dates: set[_dt.date] = set()
    for rg in range(meta.num_row_groups):
        stats = meta.row_group(rg).column(field_index).statistics
        if stats is None or not stats.has_null_count:
            return UNKNOWN, None
        if stats.null_count > 0:
            return EMPTY_TIME, None
        if stats.min is None or stats.max is None:
            return UNKNOWN, None
        d_min = _footer_scalar_to_date(stats.min, arrow_type, settings)
        d_max = _footer_scalar_to_date(stats.max, arrow_type, settings)
        if d_min is None or d_max is None:
            return UNKNOWN, None
        dates.add(d_min)
        dates.add(d_max)
    if not dates:  # 零行文件：页脚没有可证明的内容
        return UNKNOWN, None
    if len(dates) == 1:
        return SAME_DAY, next(iter(dates))
    return MIXED, None


# ---------------------------------------------------------------- 明细处理

def _read_time_dates(
    pf: pq.ParquetFile, settings: Settings, rel_input: str
) -> pa.ChunkedArray:
    table = pf.read(columns=[settings.time_column])
    dates = to_utc_date_array(table.column(0), settings)
    null_count = dates.null_count
    if null_count > 0:
        raise PublishError(
            f"{rel_input} 存在 {null_count} 条空/无法解析的过磅时间，发布中止"
        )
    return dates


def _split_and_write(
    pf: pq.ParquetFile,
    dates: pa.ChunkedArray,
    dest_dir: Path,
    settings: Settings,
    rel_input: str,
) -> list[DayFile]:
    table = pf.read()  # 跨日才读全表
    if table.num_rows != dates.length():
        raise PublishError(f"{rel_input} 日期数组与表行数不一致，放弃发布")
    unique = pc.unique(dates).to_pylist()
    day_files: list[DayFile] = []
    used_names: set[str] = set()
    stem = Path(rel_input).stem
    for day in sorted(unique):
        day_str = day.isoformat()
        name = f"{stem}__{day_str}.parquet"
        if name in used_names:
            name = f"{stem}__{day_str}__{len(used_names) + 1}.parquet"
        used_names.add(name)
        dest = dest_dir / name
        mask = pc.equal(dates, pa.scalar(day, type=pa.date32()))
        part = table.filter(mask)
        if part.num_rows == 0:
            continue
        # 临时文件 + os.replace，保证输出目录里不会出现半成品
        tmp = dest.with_suffix(".parquet.tmp")
        pq.write_table(part, tmp, compression="snappy")
        os.replace(tmp, dest)
        day_files.append(
            DayFile(
                date=day_str,
                # staged_dir 形如 <output>/staged/v000006，路径相对 staged/ 存放
                path=os.path.join(dest_dir.name, name),
                source="split",
                source_file=rel_input,
                rows=part.num_rows,
            )
        )
    return day_files


def _process_file(
    abs_path: Path,
    rel_input: str,
    settings: Settings,
    staged_dir: Path,
) -> tuple[list[DayFile], FileOutcome]:
    pf = pq.ParquetFile(abs_path)
    total_rows = pf.metadata.num_rows
    if total_rows == 0:
        # 零行文件没有任何记录，不产生按日条目，也不做拆写
        return [], FileOutcome(rel_input, "empty", [], 0)
    verdict, day = footer_verdict(pf, settings)

    if verdict == EMPTY_TIME:
        raise PublishError(f"{rel_input} 页脚证明存在空过磅时间，发布中止")

    if verdict == SAME_DAY:
        # 快路径：完全不读数据行，直接登记原档
        day_str = date_to_str(day)
        return [
            DayFile(
                date=day_str,
                path=rel_input,
                source="original",
                source_file=rel_input,
                rows=total_rows,
            )
        ], FileOutcome(rel_input, "reuse_footer", [day_str], total_rows)

    # 页脚证明不了，或已证明跨日 —— 都需要读列/读表
    dates = _read_time_dates(pf, settings, rel_input)
    unique = sorted(pc.unique(dates).to_pylist())
    if len(unique) == 1:
        # 读明细确认同日：仍复用原档，不重写
        day_str = unique[0].isoformat()
        return [
            DayFile(
                date=day_str,
                path=rel_input,
                source="original",
                source_file=rel_input,
                rows=total_rows,
            )
        ], FileOutcome(rel_input, "reuse_scan", [day_str], total_rows)

    day_files = _split_and_write(pf, dates, staged_dir, settings, rel_input)
    return day_files, FileOutcome(
        rel_input,
        "split",
        [d.date for d in day_files],
        total_rows,
    )


# ---------------------------------------------------------------- 发布入口

def _validate_inputs(input_files: list[str], settings: Settings) -> list[Path]:
    if not input_files:
        raise PublishError("输入文件清单为空")
    resolved: list[Path] = []
    seen: set[str] = set()
    root = settings.input_dir.resolve()
    for rel in input_files:
        p = Path(rel)
        if p.is_absolute() or ".." in p.parts:
            raise PublishError(f"只允许 input_dir 内的相对路径: {rel}")
        abs_path = (root / p).resolve()
        if root not in abs_path.parents:
            raise PublishError(f"路径越界: {rel}")
        if not abs_path.is_file():
            raise PublishError(f"输入文件不存在: {rel}")
        key = str(abs_path)
        if key in seen:
            raise PublishError(f"输入清单存在重复文件: {rel}")
        seen.add(key)
        resolved.append(abs_path)
    return resolved


def _check_same_schema(paths: list[Path], settings: Settings) -> None:
    base_schema = pq.ParquetFile(paths[0]).schema_arrow
    if settings.time_column not in base_schema.names:
        raise PublishError(
            f"文件 {paths[0].name} 缺少过磅时间列 {settings.time_column!r}"
        )
    for p in paths[1:]:
        schema = pq.ParquetFile(p).schema_arrow
        if settings.time_column not in schema.names:
            raise PublishError(f"文件 {p.name} 缺少过磅时间列 {settings.time_column!r}")
        if not schema.equals(base_schema):
            raise PublishError(
                f"文件 {p.name} 与首文件结构不一致，拒绝混结构发布"
            )


def publish(input_files: list[str], settings: Settings) -> PublishResult:
    """按输入清单发布新版本；失败抛 PublishError，旧版清单保持不变。"""
    settings.input_dir.mkdir(parents=True, exist_ok=True)
    store = ManifestStore(settings.output_dir)
    store.ensure_dirs()

    abs_paths = _validate_inputs(input_files, settings)
    _check_same_schema(abs_paths, settings)

    with _FileLock(store.lock_path, settings.lock_timeout_seconds):
        # 锁内重新读取版本号，避免并发发布撞版本目录
        version = store.next_version()
        staged_dir = store.staged_path_for(version)
        all_day_files: list[DayFile] = []
        outcomes: list[FileOutcome] = []
        try:
            rel_paths = []
            for abs_path in abs_paths:
                rel = abs_path.relative_to(settings.input_dir).as_posix()
                rel_paths.append(rel)
                day_files, outcome = _process_file(
                    abs_path, rel, settings, staged_dir
                )
                all_day_files.extend(day_files)
                outcomes.append(outcome)

            # 每个输入文件的记录必须且只能落在其实际过磅日；
            # 按 (date, source_file) 复核行数不重不漏
            _check_coverage(abs_paths, rel_paths, all_day_files, settings)

            all_day_files.sort(key=lambda d: (d.date, d.source_file, d.path))
            manifest = Manifest(
                version=version,
                created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
                time_column=settings.time_column,
                input_files=rel_paths,
                days=all_day_files,
            )
            store.commit(manifest)  # 全部新文件就绪后才换版
        except BaseException:
            # 清理本次半成品暂存目录；current.json 从未被触碰
            with contextlib.suppress(OSError):
                for child in sorted(staged_dir.rglob("*"), reverse=True):
                    child.unlink() if child.is_file() else child.rmdir()
                staged_dir.rmdir()
            raise

    return PublishResult(manifest=store.load_current(), outcomes=outcomes)


def _check_coverage(
    abs_paths: list[Path],
    rel_paths: list[str],
    day_files: list[DayFile],
    settings: Settings,
) -> None:
    expected_rows = {
        rel: pq.ParquetFile(abs_path).metadata.num_rows
        for rel, abs_path in zip(rel_paths, abs_paths)
    }
    actual_rows: dict[str, int] = {rel: 0 for rel in rel_paths}
    for df in day_files:
        if df.source_file not in actual_rows:
            raise PublishError(f"清单条目来源未知: {df.source_file}")
        actual_rows[df.source_file] += df.rows
    for rel, expected in expected_rows.items():
        if actual_rows[rel] != expected:
            raise PublishError(
                f"{rel} 行数核对失败: 输入 {expected} 行, 发布 {actual_rows[rel]} 行"
            )
