"""发布清单（manifest）模型与原子换版存储。

目录布局（output_dir 下）::

    current.json                 # 指向当前生效版本（原子 rename）
    versions/manifest-v000001.json
    versions/manifest-v000002.json
    staged/v000003/<文件>          # 跨日拆写出的新 Parquet
    .publish.lock                # 发布互斥锁

换版原则：新文件全部写完、清单落临时文件成功后，才用 os.rename 原子替换
current.json；任何一步失败都保留上一版清单不动。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

MANIFEST_NAME = "current.json"
VERSIONS_DIR = "versions"
STAGED_DIR = "staged"
LOCK_NAME = ".publish.lock"


@dataclass(frozen=True)
class DayFile:
    """某一 UTC 日期下可读的一个 Parquet 文件。"""

    date: str  # YYYY-MM-DD（UTC）
    path: str  # 相对 output_dir（staged 文件）或 input_dir（复用原档）的 POSIX 路径
    source: str  # "original" 直接登记原档 | "split" 跨日拆写产物
    source_file: str  # 输入文件相对 input_dir 的路径
    rows: int


@dataclass(frozen=True)
class Manifest:
    version: int
    created_at: str  # ISO8601 UTC
    time_column: str
    input_files: list[str]
    days: list[DayFile] = field(default_factory=list)

    def files_for_date(self, day: str) -> list[DayFile]:
        return [d for d in self.days if d.date == day]

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "created_at": self.created_at,
            "time_column": self.time_column,
            "input_files": list(self.input_files),
            "days": [asdict(d) for d in self.days],
        }

    @staticmethod
    def from_dict(data: dict) -> "Manifest":
        return Manifest(
            version=int(data["version"]),
            created_at=data["created_at"],
            time_column=data["time_column"],
            input_files=list(data["input_files"]),
            days=[DayFile(**item) for item in data.get("days", [])],
        )


class ManifestStore:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir.resolve()
        self.versions_dir = self.output_dir / VERSIONS_DIR
        self.staged_dir = self.output_dir / STAGED_DIR
        self.current_path = self.output_dir / MANIFEST_NAME
        self.lock_path = self.output_dir / LOCK_NAME

    def ensure_dirs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.staged_dir.mkdir(parents=True, exist_ok=True)

    # ---- 读取 ----------------------------------------------------------
    def load_current(self) -> Manifest | None:
        if not self.current_path.exists():
            return None
        with open(self.current_path, "r", encoding="utf-8") as f:
            return Manifest.from_dict(json.load(f))

    def next_version(self) -> int:
        current = self.load_current()
        return (current.version + 1) if current else 1

    def staged_path_for(self, version: int) -> Path:
        p = self.staged_dir / f"v{version:06d}"
        p.mkdir(parents=True, exist_ok=True)
        return p

    # ---- 原子换版 ------------------------------------------------------
    def commit(self, manifest: Manifest) -> None:
        """先写版本归档，再原子替换 current.json。"""
        self.ensure_dirs()
        archive = self.versions_dir / f"manifest-v{manifest.version:06d}.json"
        self._atomic_write_json(archive, manifest.to_dict())
        self._atomic_write_json(self.current_path, manifest.to_dict())

    @staticmethod
    def _atomic_write_json(path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def resolve(self, day_file: "DayFile", input_dir: Path | None = None) -> Path:
        """把清单条目解析成实际文件路径，并防目录穿越。"""
        if day_file.source == "original":
            if input_dir is None:
                raise ValueError("original 条目需要 input_dir 才能解析")
            root = input_dir.resolve()
            candidate = (root / day_file.path).resolve()
            if root not in candidate.parents and candidate != root:
                raise ValueError("路径越界")
            return candidate
        root = self.staged_dir.resolve()
        candidate = (self.staged_dir / day_file.path).resolve()
        if root not in candidate.parents and candidate != root:
            raise ValueError("路径越界")
        return candidate
