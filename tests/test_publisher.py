"""发布核心测试：页脚快路径、读明细复用、跨日拆写、失败回滚、覆盖完整性。"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from app.manifest import ManifestStore
from app.publisher import PublishError, footer_verdict, publish
from conftest import SCHEMA, utc, write_parquet


def test_footer_fast_path_same_day_no_row_read(settings, monkeypatch):
    """页脚可证明非空同日：直接登记原档，且完全不触发 read_table。"""
    path = settings.input_dir / "a.parquet"
    write_parquet(path, [
        ("T1", utc("2026-11-23 01:00:00"), 100),
        ("T2", utc("2026-11-23 23:00:00"), 200),
    ])

    def boom(*args, **kwargs):  # 快路径下读数据行的调用一律不允许发生
        raise AssertionError("快路径不应读取数据行")

    monkeypatch.setattr(pq, "read_table", boom)
    monkeypatch.setattr(pq.ParquetFile, "read", boom)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", boom)

    result = publish(["a.parquet"], settings)
    assert len(result.outcomes) == 1
    assert result.outcomes[0].mode == "reuse_footer"
    m = result.manifest
    assert m.version == 1
    assert len(m.days) == 1
    d = m.days[0]
    assert d.date == "2026-11-23"
    assert d.source == "original"
    assert d.path == "a.parquet"
    assert d.rows == 2
    # 原档未被改动/复制：staged 目录应为空
    assert not any((settings.output_dir / "staged").rglob("*.parquet"))


def test_footer_verdict_helper_mixed_and_null(settings):
    mixed = settings.input_dir / "m.parquet"
    write_parquet(mixed, [
        ("T1", utc("2026-11-23 23:00:00"), 1),
        ("T2", utc("2026-11-24 01:00:00"), 2),
    ])
    verdict, day = footer_verdict(pq.ParquetFile(mixed), settings)
    assert verdict == "mixed" and day is None

    null_file = settings.input_dir / "n.parquet"
    write_parquet(null_file, [("T1", None, 1)])
    verdict, _ = footer_verdict(pq.ParquetFile(null_file), settings)
    assert verdict == "empty_time"


def test_no_statistics_scan_confirms_same_day_and_reuses(settings):
    """无统计信息：降级读时间列；确认同日仍复用原档。"""
    path = settings.input_dir / "nostats.parquet"
    write_parquet(
        path,
        [("T1", utc("2026-11-24 02:00:00"), 1), ("T2", utc("2026-11-24 20:00:00"), 2)],
        write_statistics=False,
    )
    verdict, _ = footer_verdict(pq.ParquetFile(path), settings)
    assert verdict == "unknown"

    result = publish(["nostats.parquet"], settings)
    assert result.outcomes[0].mode == "reuse_scan"
    assert result.manifest.days[0].source == "original"
    assert not any((settings.output_dir / "staged").rglob("*.parquet"))


def test_mixed_day_split_and_each_record_appears_once(settings):
    """跨日拆写：每条记录只在实际过磅日出现一次，行数合计与输入一致。"""
    path = settings.input_dir / "mixed.parquet"
    write_parquet(path, [
        ("T1", utc("2026-11-23 23:30:00"), 10),
        ("T2", utc("2026-11-24 00:30:00"), 20),
        ("T3", utc("2026-11-24 12:00:00"), 30),
    ])
    result = publish(["mixed.parquet"], settings)
    assert result.outcomes[0].mode == "split"
    m = result.manifest
    days = {d.date: d for d in m.days}
    assert sorted(days) == ["2026-11-23", "2026-11-24"]
    assert days["2026-11-23"].rows == 1
    assert days["2026-11-24"].rows == 2
    for d in m.days:
        assert d.source == "split"
        p = ManifestStore(settings.output_dir).resolve(d, settings.input_dir)
        t = pq.read_table(p)
        assert t.num_rows == d.rows
        ids = t.column("ticket_id").to_pylist()
        if d.date == "2026-11-23":
            assert ids == ["T1"]
        else:
            assert ids == ["T2", "T3"]
    # 原档不动
    assert pq.read_table(path).num_rows == 3


def test_null_time_aborts_and_keeps_previous_version(settings):
    good = settings.input_dir / "good.parquet"
    write_parquet(good, [("T1", utc("2026-11-23 09:00:00"), 1)])
    r1 = publish(["good.parquet"], settings)
    assert r1.manifest.version == 1

    bad = settings.input_dir / "bad.parquet"
    write_parquet(bad, [("T9", None, 2)])
    with pytest.raises(PublishError, match="空"):
        publish(["good.parquet", "bad.parquet"], settings)

    store = ManifestStore(settings.output_dir)
    current = store.load_current()
    assert current.version == 1  # 仍旧版
    assert current.input_files == ["good.parquet"]
    # 失败的暂存目录被清理
    staged = settings.output_dir / "staged"
    assert not [p for p in staged.rglob("*") if p.is_file()]
    # 没有产生 v2 归档
    assert not (settings.output_dir / "versions" / "manifest-v000002.json").exists()


def test_write_failure_mid_publish_keeps_previous_version(settings, monkeypatch):
    good = settings.input_dir / "good.parquet"
    write_parquet(good, [("T1", utc("2026-11-23 09:00:00"), 1)])
    publish(["good.parquet"], settings)

    mixed = settings.input_dir / "mixed.parquet"
    write_parquet(mixed, [
        ("A", utc("2026-11-23 22:00:00"), 1),
        ("B", utc("2026-11-24 02:00:00"), 2),
    ])

    real_write = pq.write_table

    def flaky_write(table, where, *args, **kwargs):
        if str(where).endswith(".tmp"):
            raise OSError("模拟磁盘写入失败")
        return real_write(table, where, *args, **kwargs)

    monkeypatch.setattr(pq, "write_table", flaky_write)
    with pytest.raises(OSError):
        publish(["mixed.parquet"], settings)

    current = ManifestStore(settings.output_dir).load_current()
    assert current.version == 1
    assert current.input_files == ["good.parquet"]


def test_successful_republish_swaps_version(settings):
    f1 = settings.input_dir / "b1.parquet"
    write_parquet(f1, [("T1", utc("2026-11-23 09:00:00"), 1)])
    r1 = publish(["b1.parquet"], settings)

    f2 = settings.input_dir / "b2.parquet"
    write_parquet(f2, [
        ("T2", utc("2026-11-24 10:00:00"), 2),
        ("T3", utc("2026-11-24 11:00:00"), 3),
    ])
    r2 = publish(["b1.parquet", "b2.parquet"], settings)
    assert r2.manifest.version == 2
    assert r2.manifest.input_files == ["b1.parquet", "b2.parquet"]
    # 旧版本归档仍在
    store = ManifestStore(settings.output_dir)
    assert (store.versions_dir / "manifest-v000001.json").exists()
    assert (store.versions_dir / "manifest-v000002.json").exists()
    # 按日取数得到的是新版
    day23 = pq.read_table(store.resolve(r2.manifest.files_for_date("2026-11-23")[0],
                                        settings.input_dir))
    assert day23.column("ticket_id").to_pylist() == ["T1"]


def test_multi_row_group_mixed_file_splits_per_footer(settings):
    """多个 row group 各自带统计：页脚跨日判定后直接拆写。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    def one_row(ticket, when):
        return pa.table(
            {
                "ticket_id": [ticket],
                "weigh_time": [when],
                "gross_weight_kg": [1],
            },
            schema=SCHEMA,
        )

    writer = pq.ParquetWriter(settings.input_dir / "mrg.parquet", SCHEMA)
    writer.write_table(one_row("A", utc("2026-11-23 10:00:00")))
    writer.write_table(one_row("B", utc("2026-11-24 10:00:00")))
    writer.close()
    pf = pq.ParquetFile(settings.input_dir / "mrg.parquet")
    assert pf.metadata.num_row_groups == 2
    verdict, _ = footer_verdict(pf, settings)
    assert verdict == "mixed"
    result = publish(["mrg.parquet"], settings)
    assert result.outcomes[0].mode == "split"
    assert sorted(d.date for d in result.manifest.days) == [
        "2026-11-23",
        "2026-11-24",
    ]


def test_zero_row_file_produces_no_entries_and_empty_mode(settings):
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(SCHEMA.empty_table(), settings.input_dir / "empty.parquet")
    result = publish(["empty.parquet"], settings)
    assert result.outcomes[0].mode == "empty"
    assert result.manifest.days == []
    store = ManifestStore(settings.output_dir)
    assert store.load_current().version == 1
    # 服务层按日取数应返回空表而不是报错
    from app.service import DatasetService

    table = DatasetService(settings).read_date("2026-11-24")
    assert table is not None and table.num_rows == 0


def test_rejects_missing_and_escaping_paths(settings):
    with pytest.raises(PublishError):
        publish(["nope.parquet"], settings)
    with pytest.raises(PublishError):
        publish(["../escape.parquet"], settings)
    with pytest.raises(PublishError):
        publish(["/etc/passwd"], settings)


def test_rejects_different_schema(settings):
    import pyarrow as pa

    f1 = settings.input_dir / "s1.parquet"
    write_parquet(f1, [("T1", utc("2026-11-23 09:00:00"), 1)])
    other = pa.schema([
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("extra", pa.string()),
    ])
    p = settings.input_dir / "s2.parquet"
    pq.write_table(
        pa.table(
            {"ticket_id": ["X"], "weigh_time": [utc("2026-11-23 10:00:00")], "extra": ["z"]},
            schema=other,
        ),
        p,
    )
    with pytest.raises(PublishError, match="结构不一致"):
        publish(["s1.parquet", "s2.parquet"], settings)


def test_string_time_column_parse_and_blank_is_empty(settings):
    import pyarrow as pa
    import pyarrow.parquet as pq

    str_schema = pa.schema([
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.string()),
        pa.field("gross_weight_kg", pa.int64()),
    ])
    good = settings.input_dir / "str.parquet"
    pq.write_table(
        pa.table(
            {
                "ticket_id": ["A", "B"],
                "weigh_time": ["2026-11-23T08:00:00", "2026-11-23T20:00:00"],
                "gross_weight_kg": [1, 2],
            },
            schema=str_schema,
        ),
        good,
    )
    result = publish(["str.parquet"], settings)
    # 字符串页脚 min/max 也能判定同日；若不能则降级扫描，两种都应成功
    assert result.manifest.days[0].date == "2026-11-23"

    blank = settings.input_dir / "str_blank.parquet"
    pq.write_table(
        pa.table(
            {
                "ticket_id": ["A", "B"],
                "weigh_time": ["2026-11-23T08:00:00", "  "],
                "gross_weight_kg": [1, 2],
            },
            schema=str_schema,
        ),
        blank,
        write_statistics=False,
    )
    with pytest.raises(PublishError):
        publish(["str_blank.parquet"], settings)


def test_tz_aware_timestamp_uses_utc_date(settings):
    import pyarrow as pa
    import pyarrow.parquet as pq

    tz_schema = pa.schema([
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us", tz="Asia/Shanghai")),
        pa.field("gross_weight_kg", pa.int64()),
    ])
    # UTC 23:30 = 北京次日 07:30；UTC 00:30 = 北京 08:30
    table = pa.table(
        {
            "ticket_id": ["A", "B"],
            "weigh_time": pa.array(
                [utc("2026-11-23 23:30:00"), utc("2026-11-24 00:30:00")],
                type=pa.timestamp("us", "UTC"),
            ),
            "gross_weight_kg": [1, 2],
        }
    ).cast(tz_schema)
    pq.write_table(table, settings.input_dir / "tz.parquet")
    result = publish(["tz.parquet"], settings)
    dates = sorted(d.date for d in result.manifest.days)
    assert dates == ["2026-11-23", "2026-11-24"]
