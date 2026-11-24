"""运行配置：全部通过环境变量注入，Docker Compose 里给出默认值。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    # 挂载进来的原始 Parquet 目录（只读使用，原档不动）
    input_dir: Path
    # 发布目录：清单 versions/ 与跨日拆写文件 staged/ 都放这里
    output_dir: Path
    # 过磅时间列名
    time_column: str
    # 字符串时间列的解析格式（pyarrow strptime 格式，None 表示自动 ISO）
    time_string_format: str | None
    # 发布时是否在同一进程内串行执行（测试用）
    lock_timeout_seconds: float

    @staticmethod
    def from_env() -> "Settings":
        input_dir = Path(os.environ.get("WEIGH_INPUT_DIR", "/data/input")).resolve()
        output_dir = Path(os.environ.get("WEIGH_OUTPUT_DIR", "/data/output")).resolve()
        return Settings(
            input_dir=input_dir,
            output_dir=output_dir,
            time_column=os.environ.get("WEIGH_TIME_COLUMN", "weigh_time"),
            time_string_format=os.environ.get("WEIGH_TIME_STRING_FORMAT") or None,
            lock_timeout_seconds=float(os.environ.get("WEIGH_LOCK_TIMEOUT", "10")),
        )
