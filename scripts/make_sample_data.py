#!/usr/bin/env python3
"""生成演示用的码头过磅明细 Parquet（数据是虚构的）。

放到 scripts/ 下，与被处理的数据目录分离；输出目录由参数指定：
    python scripts/make_sample_data.py /workspace/data_in

生成文件：
  batch_a_sameday.parquet      同日记录（页脚即可证明，走快路径复用原档）
  batch_b_mixed.parquet        两个 UTC 日混存（跨日拆写）
  batch_c_null.parquet         含空过磅时间（发布失败演示）
  batch_d_nostats.parquet      write_statistics=False（降级读明细，确认同日复用）
  batch_e_tz.parquet           带时区时间戳，含 UTC 跨日边界记录
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("container_no", pa.string()),
        pa.field("gross_weight_kg", pa.int64()),
    ]
)


def _ts(s: str):
    import datetime as dt

    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=dt.timezone.utc
    )


def main(out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1) 同日：2026-11-23
    rows_a = [
        ("T-1001", _ts("2026-11-23 01:10:00"), "CNTU1000001", 21400),
        ("T-1002", _ts("2026-11-23 08:25:00"), "CNTU1000002", 18900),
        ("T-1003", _ts("2026-11-23 22:40:00"), "CNTU1000003", 24600),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_a] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_a_sameday.parquet",
        compression="snappy",
    )

    # 2) 跨日混存：23 日与 24 日
    rows_b = [
        ("T-2001", _ts("2026-11-23 23:20:00"), "CNTU2000001", 20100),
        ("T-2002", _ts("2026-11-24 00:05:00"), "CNTU2000002", 19800),
        ("T-2003", _ts("2026-11-24 09:15:00"), "CNTU2000003", 22300),
        ("T-2004", _ts("2026-11-24 18:30:00"), "CNTU2000004", 17750),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_b] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_b_mixed.parquet",
        compression="snappy",
    )

    # 3) 含空时间
    rows_c = [
        ("T-3001", _ts("2026-11-24 07:00:00"), "CNTU3000001", 20000),
        ("T-3002", None, "CNTU3000002", 19500),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_c] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_c_null.parquet",
        compression="snappy",
    )

    # 4) 关闭页脚统计：同日但页脚证明不了，必须读明细
    rows_d = [
        ("T-4001", _ts("2026-11-24 03:00:00"), "CNTU4000001", 18000),
        ("T-4002", _ts("2026-11-24 12:00:00"), "CNTU4000002", 19000),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_d] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_d_nostats.parquet",
        compression="snappy",
        write_statistics=False,
    )

    # 5) 带时区：UTC+8 的 11-24 07:30 对应 UTC 11-23 23:30，验证按 UTC 取日
    tz_schema = pa.schema(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("weigh_time", pa.timestamp("us", tz="Asia/Shanghai")),
            pa.field("container_no", pa.string()),
            pa.field("gross_weight_kg", pa.int64()),
        ]
    )
    rows_e = [
        ("T-5001", _ts("2026-11-23 23:30:00"), "CNTU5000001", 20500),
        ("T-5002", _ts("2026-11-24 00:30:00"), "CNTU5000002", 21200),
    ]
    # 上面构造的是 UTC 瞬间；写入 tz=Asia/Shanghai 的列时 Arrow 保留同一瞬间
    table_e = pa.table(
        {
            "ticket_id": [r[0] for r in rows_e],
            "weigh_time": pa.array([r[1] for r in rows_e], type=pa.timestamp("us", "UTC")),
            "container_no": [r[2] for r in rows_e],
            "gross_weight_kg": [r[3] for r in rows_e],
        }
    ).cast(tz_schema)
    pq.write_table(table_e, out / "batch_e_tz.parquet", compression="snappy")

    for p in sorted(out.glob("*.parquet")):
        print(f"wrote {p} ({pq.ParquetFile(p).metadata.num_rows} rows)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        raise SystemExit(1)
    main(sys.argv[1])
