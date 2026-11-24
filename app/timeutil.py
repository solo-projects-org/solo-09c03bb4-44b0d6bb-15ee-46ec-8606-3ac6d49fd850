"""过磅时间 → UTC 日期 的转换工具。

约定：
* timestamp 带时区的，先转 UTC 再取日期；
* timestamp 不带时区的，列本身即视为 UTC（码头落地统一 UTC 存储）；
* date32/date64 直接取日期；
* 字符串按配置格式或 ISO 解析，解析失败与空白均视为空时间（发布失败信号）。
"""

from __future__ import annotations

import datetime as _dt

import pyarrow as pa
import pyarrow.compute as pc

from .config import Settings

# strptime 多格式兜底：未显式配置格式时按常见 ISO 形式依次尝试
_FALLBACK_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d",
)


class TimeColumnError(ValueError):
    """时间列缺失、类型不支持或存在无法解析/为空的时间。"""


def _parse_string_dates(col: pa.Array, settings: Settings) -> pa.Array:
    formats = (settings.time_string_format,) if settings.time_string_format else _FALLBACK_FORMATS
    last_err: Exception | None = None
    # 空白字符串先归一为 null，避免 strptime 报解析错
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        trimmed = pc.utf8_trim_whitespace(col)
        blank = pc.equal(trimmed, "")  # null 位置保持 null，if_else 下仍为 null
        col = pc.if_else(blank, None, col)
    for fmt in formats:
        try:
            ts = pc.strptime(col, format=fmt, unit="us", error_is_null=True)
        except pa.ArrowNotImplementedError as exc:  # pragma: no cover - 类型不支持
            last_err = exc
            continue
        # error_is_null=True：解析不出来的位置变成 null，交由调用方统一判空
        return pc.cast(ts, pa.date32(), safe=False)
    raise TimeColumnError(f"字符串时间列无法解析: {last_err}")


def to_utc_date_array(col: pa.ChunkedArray | pa.Array, settings: Settings) -> pa.ChunkedArray:
    """把任意受支持的时间列转成 UTC 日期（date32）分块数组；不做 null 过滤。"""
    t = col.type
    if pa.types.is_timestamp(t):
        if t.tz is not None:
            # 带时区：统一转 UTC 时间轴后再取日
            col = pc.cast(col, pa.timestamp(t.unit, "UTC"))
        # cast 到 date32：带时区时按 UTC 当天取日；无时区按字面值取日
        return pc.cast(col, pa.date32(), safe=False)
    if pa.types.is_date(t):
        return pc.cast(col, pa.date32(), safe=False)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        if isinstance(col, pa.ChunkedArray):
            chunks = [_parse_string_dates(chunk, settings) for chunk in col.chunks]
            return pa.chunked_array(chunks, type=pa.date32())
        return pa.chunked_array([_parse_string_dates(col, settings)], type=pa.date32())
    if pa.types.is_integer(t) or pa.types.is_floating(t):
        # 数字列按 Unix 秒处理（历史系统导出兜底）
        ts = pc.cast(col, pa.timestamp("s", "UTC"), safe=False)
        return pc.cast(ts, pa.date32(), safe=False)
    raise TimeColumnError(f"不支持的时间列类型: {t}")


def date_to_str(d: _dt.date | pa.Date32Scalar) -> str:
    if isinstance(d, pa.Scalar):
        d = d.as_py()
    return d.isoformat()


def parse_date(s: str) -> _dt.date:
    return _dt.date.fromisoformat(s)
