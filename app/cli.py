"""命令行入口：发布 / 查看当前清单 / 按日读取。

用法：
  python -m app.cli publish rel1.parquet rel2.parquet
  python -m app.cli manifest
  python -m app.cli read 2026-11-23
"""

from __future__ import annotations

import argparse
import json
import sys

from .config import Settings
from .publisher import PublishError, publish
from .service import DatasetService


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="weigh-dataset")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pub = sub.add_parser("publish", help="按输入清单发布新版本")
    p_pub.add_argument("files", nargs="+")

    sub.add_parser("manifest", help="打印当前生效清单")

    p_read = sub.add_parser("read", help="按 UTC 日期读取并打印 JSON 行")
    p_read.add_argument("day")
    p_read.add_argument("--limit", type=int, default=1000)

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "publish":
        try:
            result = publish(args.files, settings)
        except PublishError as exc:
            print(f"发布失败，已保留上一版清单: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(result.manifest.to_dict(), ensure_ascii=False, indent=2))
        print("处理方式:", file=sys.stderr)
        for o in result.outcomes:
            print(f"  - {o.input_file}: {o.mode} -> {o.days}", file=sys.stderr)
        return 0

    service = DatasetService(settings)
    manifest = service.current_manifest()
    if args.cmd == "manifest":
        if manifest is None:
            print("尚无已发布版本")
        else:
            print(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "read":
        if manifest is None:
            print("尚无已发布版本", file=sys.stderr)
            return 1
        table = service.read_date(args.day)
        if table is None or table.num_rows == 0:
            print("[]")
            return 0
        for row in table.slice(0, args.limit).to_pylist():
            print(json.dumps(row, ensure_ascii=False, default=str))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
