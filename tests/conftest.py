"""pytest 公共夹具与造数工具。"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.config import Settings

SCHEMA = pa.schema(
    [
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("gross_weight_kg", pa.int64()),
    ]
)


def utc(s: str):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=dt.timezone.utc
    )


def write_parquet(path, rows, *, write_statistics=True, schema=SCHEMA):
    table = pa.table(
        {
            "ticket_id": [r[0] for r in rows],
            "weigh_time": [r[1] for r in rows],
            "gross_weight_kg": [r[2] for r in rows],
        },
        schema=schema,
    )
    pq.write_table(
        table, path, compression="snappy", write_statistics=write_statistics
    )
    return table


@pytest.fixture()
def settings(tmp_path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    return Settings(
        input_dir=in_dir,
        output_dir=out_dir,
        time_column="weigh_time",
        time_string_format=None,
        lock_timeout_seconds=5,
    )
