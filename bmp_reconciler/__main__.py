"""命令行入口：读取 JSON 输入，输出对账报告 JSON。

用法：
    python -m bmp_reconciler input.json [-o report.json]

输入格式：
    {
      "reconnect_intervals": [[start_ts, end_ts], ...],
      "messages": [
        {"msg_type": "initiation", "seq": 1, "ts": 1000.0, ...},
        {"msg_type": "peer_up", "seq": 2, "ts": 1000.1, "peer": "10.0.0.1", ...},
        {"msg_type": "route_monitoring", "seq": 3, "ts": 1000.2,
         "peer": "10.0.0.1", "action": "announce",
         "prefix": "192.0.2.0/24", "attrs": {"as_path": "65001"}},
        ...
      ],
      "rib_snapshot": {"10.0.0.1": {"192.0.2.0/24": {"as_path": "65001"}}}
    }

本工具仅做离线计算，不连接真实路由器。
"""

import argparse
import json
import sys

from .messages import make_message
from .reconciler import Reconciler


def run(input_path, output_path=None):
    with open(input_path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    messages = [make_message(raw) for raw in payload.get("messages", [])]
    reconciler = Reconciler(
        reconnect_intervals=payload.get("reconnect_intervals", [])
    )
    report = reconciler.process(messages)
    snapshot = payload.get("rib_snapshot")
    if snapshot:
        reconciler.compare_with_snapshot(snapshot)
    text = json.dumps(report.to_dict(), indent=2, ensure_ascii=False, sort_keys=True)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    else:
        print(text)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="bmp_reconciler",
        description="BMP 路由监控离线对账器（不连接真实路由器）",
    )
    parser.add_argument("input", help="输入 JSON 文件路径")
    parser.add_argument("-o", "--output", help="报告输出路径（默认 stdout）")
    args = parser.parse_args(argv)
    run(args.input, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
