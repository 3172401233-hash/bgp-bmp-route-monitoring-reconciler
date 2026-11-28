# bgp-bmp-route-monitoring-reconciler
对账 BMP 路由监控、RIB 和事件时间线

## 概述

BMP 监控连接中断重连后，部分 Route Monitoring 消息可能丢失，而 Peer Up
和 Statistics 消息仍有记录，导致监控平台重建的 RIB 与真实路由器不一致。
本项目实现一个**纯离线**的 BMP 对账器：读取监控平台记录的消息流（含监控
序号）、重连区间与本地 RIB 快照，重建每个 peer 在任意时间点的有效 RIB，
并输出对账报告。

> **注意**：本工具不连接真实路由器，不建立 BMP/BGP 会话，不收发任何网络
> 报文。所有输入均为离线的 JSON 记录。

## 功能

- 输入：Initiation、Peer Up/Down、Route Monitoring、Statistics 消息，
  监控序号（`seq`）、重连区间、本地 RIB 快照
- 消息去重：相同序号的消息只应用一次
- 序号缺口检测：记录缺失区间，并标记是否落在重连区间内
- 撤销晚到：序号小于该前缀已应用更新的撤销/更新被忽略并计数
- 前缀属性更新：同前缀的 announce 覆盖旧属性，时间线保留历史
- Peer restart：Peer Down/Up、未先 Down 的重复 Peer Up、监控会话重建
  （再次 Initiation）均会清空对应 RIB 并标记全量重同步
- 输出：不一致前缀、缺失区间、最终 RIB、重同步范围

## 环境

- Python 3.8.2+（仅标准库，无第三方依赖）

## 使用

```bash
python -m bmp_reconciler input.json [-o report.json]
```

输入 JSON 格式：

```json
{
  "reconnect_intervals": [[105.0, 115.0]],
  "messages": [
    {"msg_type": "initiation", "seq": 1, "ts": 100.0, "sys_descr": "router-a"},
    {"msg_type": "peer_up", "seq": 2, "ts": 100.1, "peer": "10.0.0.1", "session": 1},
    {"msg_type": "route_monitoring", "seq": 3, "ts": 100.2, "peer": "10.0.0.1",
     "action": "announce", "prefix": "192.0.2.0/24",
     "attrs": {"next_hop": "192.0.2.1"}},
    {"msg_type": "statistics", "seq": 4, "ts": 100.3, "peer": "10.0.0.1",
     "counters": {"prefixes": 1}}
  ],
  "rib_snapshot": {
    "10.0.0.1": {"192.0.2.0/24": {"next_hop": "192.0.2.1"}}
  }
}
```

报告字段：

- `final_rib`：每个 peer 的最终有效 RIB
- `inconsistent_prefixes`：与本地快照对比的差异
  （`missing_in_monitor` / `extra_in_monitor` / `attr_mismatch`）
- `missing_intervals`：序号缺失区间及是否处于重连区间
- `resync_ranges`：每个 peer 需要的重同步范围（`seq_range` 或 `full`）
- `duplicates` / `late_withdrawals` / `late_updates` / `restarts` / `anomalies`

也可以在代码中查询任意时间点的有效 RIB：

```python
from bmp_reconciler import Reconciler, make_message

rec = Reconciler(reconnect_intervals=[(105.0, 115.0)])
report = rec.process([make_message(m) for m in messages])
rec.compare_with_snapshot(snapshot)
rib = rec.rib_at("10.0.0.1", 110.0)  # ts=None 表示最新
```

## 测试

```bash
python -m unittest
```

测试覆盖：重连缺口、重复路由、peer 重启、撤销晚到、属性更新与快照对比。

## 结构

- `bmp_reconciler/messages.py`：BMP 消息模型与校验
- `bmp_reconciler/reconciler.py`：对账核心（去重、缺口、晚到、重启、时间线回放）
- `bmp_reconciler/__main__.py`：命令行入口（JSON 输入/输出）
- `tests/test_reconciler.py`：单元测试
