"""BMP 路由监控对账器。

根据 BMP 消息流（Initiation / Peer Up / Peer Down / Route Monitoring /
Statistics）、监控序号、重连区间与本地 RIB 快照，重建每个 peer 在任意
时间点的有效 RIB，并输出不一致前缀、缺失区间与重同步范围。

本工具纯离线计算，不连接真实路由器。
"""

from .messages import (
    Message,
    MessageType,
    PeerDownReason,
    make_message,
)
from .reconciler import Reconciler, ReconcileReport

__all__ = [
    "Message",
    "MessageType",
    "PeerDownReason",
    "make_message",
    "Reconciler",
    "ReconcileReport",
]

__version__ = "0.1.0"
