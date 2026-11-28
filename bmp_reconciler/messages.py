"""BMP 消息模型。

所有消息均来自监控平台记录（离线输入），携带监控序号 ``seq`` 与时间戳
``ts``。Route Monitoring 消息额外携带前缀、属性与动作（announce /
withdraw）。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class MessageType(object):
    INITIATION = "initiation"
    PEER_UP = "peer_up"
    PEER_DOWN = "peer_down"
    ROUTE_MONITORING = "route_monitoring"
    STATISTICS = "statistics"
    TERMINATION = "termination"

    ALL = frozenset(
        [
            INITIATION,
            PEER_UP,
            PEER_DOWN,
            ROUTE_MONITORING,
            STATISTICS,
            TERMINATION,
        ]
    )


class PeerDownReason(object):
    LOCAL_NOTIFICATION = 1
    LOCAL_FSM_EVENT = 2
    REMOTE_NOTIFICATION = 3
    REMOTE_NO_DATA = 4
    PEER_DE_CONFIGURED = 5


@dataclass
class Message(object):
    """一条 BMP 监控记录。"""

    msg_type: str
    seq: int
    ts: float
    peer: Optional[str] = None
    session: int = 0
    action: Optional[str] = None  # announce / withdraw（仅 route_monitoring）
    prefix: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    reason: Optional[int] = None  # peer_down 原因码
    counters: Dict[str, int] = field(default_factory=dict)  # statistics
    sys_descr: str = ""  # initiation

    def __post_init__(self):
        if self.msg_type not in MessageType.ALL:
            raise ValueError("unknown message type: %r" % self.msg_type)
        if self.msg_type == MessageType.ROUTE_MONITORING:
            if self.action not in ("announce", "withdraw"):
                raise ValueError("route_monitoring requires announce/withdraw")
            if not self.prefix:
                raise ValueError("route_monitoring requires prefix")


def make_message(raw):
    # type: (Dict[str, Any]) -> Message
    """从字典（如 JSON 记录）构造 Message。"""
    known = {
        "msg_type",
        "type",
        "seq",
        "ts",
        "peer",
        "session",
        "action",
        "prefix",
        "attrs",
        "reason",
        "counters",
        "sys_descr",
    }
    extra = {k: v for k, v in raw.items() if k not in known}
    if extra:
        raise ValueError("unknown message fields: %s" % sorted(extra))
    msg_type = raw.get("msg_type", raw.get("type"))
    if msg_type is None:
        raise ValueError("message missing 'msg_type'")
    return Message(
        msg_type=msg_type,
        seq=int(raw["seq"]),
        ts=float(raw["ts"]),
        peer=raw.get("peer"),
        session=int(raw.get("session", 0)),
        action=raw.get("action"),
        prefix=raw.get("prefix"),
        attrs=dict(raw.get("attrs", {})),
        reason=raw.get("reason"),
        counters=dict(raw.get("counters", {})),
        sys_descr=raw.get("sys_descr", ""),
    )
