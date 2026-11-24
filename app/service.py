"""按日取数的服务层：清单解析、文件定位与 Parquet 读取。"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from .manifest import DayFile, ManifestStore


class DatasetService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = ManifestStore(settings.output_dir)

    def current_manifest(self):
        return self.store.load_current()

    def resolve(self, day_file: DayFile) -> Path:
        return self.store.resolve(day_file, input_dir=self.settings.input_dir)

    def files_for_date(self, day: str) -> list[DayFile]:
        manifest = self.current_manifest()
        if manifest is None:
            return []
        return manifest.files_for_date(day)

    def read_date(self, day: str) -> pa.Table | None:
        """按 UTC 日期读取当前版清单里的全部文件并拼接；无清单返回 None。"""
        entries = self.files_for_date(day)
        if not entries:
            manifest = self.current_manifest()
            return None if manifest is None else self._empty_like()
        tables = [pq.read_table(self.resolve(entry)) for entry in entries]
        return pa.concat_tables(tables, promote_options="default")

    def _empty_like(self) -> pa.Table:
        # 清单存在但该日无数据：用任一文件的 schema 造空表
        manifest = self.current_manifest()
        assert manifest is not None
        if manifest.days:
            return pq.read_table(self.resolve(manifest.days[0])).slice(0, 0)
        # 整版都是零行文件：直接从输入文件取 schema
        first_input = self.settings.input_dir / manifest.input_files[0]
        return pq.read_table(first_input).slice(0, 0)
