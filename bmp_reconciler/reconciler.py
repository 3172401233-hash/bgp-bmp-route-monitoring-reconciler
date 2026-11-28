"""BMP 对账器核心逻辑。

按监控序号处理消息流，处理以下情况：

- 消息重复：相同序号的消息只应用一次；
- 序号缺口：序号跳变时记录缺失区间，并标记是否落在重连区间内；
- 撤销晚到：序号小于该前缀已应用更新的撤销/更新被忽略并计数；
- 前缀属性更新：同前缀的 announce 覆盖旧属性；
- peer restart：Peer Down/Up 或重复 Peer Up 触发 RIB 清空与全量重同步。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .messages import Message, MessageType


@dataclass
class MissingInterval(object):
    start_seq: int
    end_seq: int
    start_ts: Optional[float]
    end_ts: Optional[float]
    during_reconnect: bool = False

    def to_dict(self):
        return {
            "start_seq": self.start_seq,
            "end_seq": self.end_seq,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "during_reconnect": self.during_reconnect,
        }


@dataclass
class PeerState(object):
    peer: str
    up: bool = False
    session: int = 0
    restarts: int = 0
    # 时间线条目：("reset", ts, session) 或 ("route", ts, session, action, prefix, attrs)
    timeline: List[tuple] = field(default_factory=list)
    last_route_seq: Dict[str, int] = field(default_factory=dict)
    statistics: List[Message] = field(default_factory=list)
    resync_ranges: List[Dict[str, Any]] = field(default_factory=list)

    def reset(self, ts, session, reason):
        self.timeline.append(("reset", ts, session))
        self.last_route_seq = {}
        self.restarts += 1
        self.resync_ranges.append(
            {"type": "full", "ts": ts, "session": session, "reason": reason}
        )


@dataclass
class ReconcileReport(object):
    final_rib: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    inconsistent_prefixes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    missing_intervals: List[MissingInterval] = field(default_factory=list)
    resync_ranges: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    duplicates: List[int] = field(default_factory=list)
    late_withdrawals: int = 0
    late_updates: int = 0
    restarts: Dict[str, int] = field(default_factory=dict)
    anomalies: List[str] = field(default_factory=list)

    def to_dict(self):
        return {
            "final_rib": self.final_rib,
            "inconsistent_prefixes": self.inconsistent_prefixes,
            "missing_intervals": [m.to_dict() for m in self.missing_intervals],
            "resync_ranges": self.resync_ranges,
            "duplicates": sorted(self.duplicates),
            "late_withdrawals": self.late_withdrawals,
            "late_updates": self.late_updates,
            "restarts": self.restarts,
            "anomalies": self.anomalies,
        }


class Reconciler(object):
    """离线 BMP 对账器。"""

    def __init__(self, reconnect_intervals=None):
        # type: (Optional[List[Tuple[float, float]]]) -> None
        self.reconnect_intervals = [
            (float(a), float(b)) for a, b in (reconnect_intervals or [])
        ]
        self.peers = {}  # type: Dict[str, PeerState]
        self.seen_seqs = set()
        self.max_seq = None  # type: Optional[int]
        self.last_ts = None  # type: Optional[float]
        self.initiated = False
        self.report = ReconcileReport()

    # ------------------------------------------------------------------ API

    def process(self, messages):
        # type: (List[Message]) -> ReconcileReport
        for msg in messages:
            self._process_one(msg)
        for peer in sorted(self.peers):
            state = self.peers[peer]
            self.report.final_rib[peer] = self.rib_at(peer, None)
            self.report.resync_ranges[peer] = list(state.resync_ranges)
            if state.restarts:
                self.report.restarts[peer] = state.restarts
        return self.report

    def compare_with_snapshot(self, snapshot):
        # type: (Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]
        """将最终 RIB 与本地 RIB 快照对比，填充 inconsistent_prefixes。"""
        peers = sorted(set(snapshot) | set(self.report.final_rib))
        for peer in peers:
            expected = snapshot.get(peer, {})
            actual = self.report.final_rib.get(peer, {})
            diff = {}
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            attr_mismatch = sorted(
                p for p in set(expected) & set(actual) if expected[p] != actual[p]
            )
            if missing:
                diff["missing_in_monitor"] = missing
            if extra:
                diff["extra_in_monitor"] = extra
            if attr_mismatch:
                diff["attr_mismatch"] = {
                    p: {"expected": expected[p], "actual": actual[p]}
                    for p in attr_mismatch
                }
            if diff:
                self.report.inconsistent_prefixes[peer] = diff
        return self.report.inconsistent_prefixes

    def rib_at(self, peer, ts):
        # type: (str, Optional[float]) -> Dict[str, Dict[str, Any]]
        """重放时间线，计算 peer 在 ts（None 表示最新）时刻的有效 RIB。"""
        state = self.peers.get(peer)
        if state is None:
            return {}
        table = {}  # type: Dict[str, Dict[str, Any]]
        for entry in state.timeline:
            if ts is not None and entry[1] > ts:
                continue
            if entry[0] == "reset":
                table = {}
            else:
                _, _, _, action, prefix, attrs = entry
                if action == "announce":
                    table[prefix] = dict(attrs)
                elif action == "withdraw":
                    table.pop(prefix, None)
        return table

    # ------------------------------------------------------------- internal

    def _peer_state(self, peer):
        state = self.peers.get(peer)
        if state is None:
            state = PeerState(peer=peer)
            self.peers[peer] = state
        return state

    def _in_reconnect(self, start_ts, end_ts):
        """时间段 [start_ts, end_ts] 是否与任意重连区间重叠。"""
        if start_ts is None or end_ts is None:
            return False
        return any(
            start_ts <= b and a <= end_ts for a, b in self.reconnect_intervals
        )

    def _track_seq(self, msg):
        seq = msg.seq
        if seq in self.seen_seqs:
            self.report.duplicates.append(seq)
            return False
        self.seen_seqs.add(seq)
        if self.max_seq is not None and seq > self.max_seq + 1:
            interval = MissingInterval(
                start_seq=self.max_seq + 1,
                end_seq=seq - 1,
                start_ts=self.last_ts,
                end_ts=msg.ts,
                during_reconnect=self._in_reconnect(self.last_ts, msg.ts),
            )
            self.report.missing_intervals.append(interval)
            self._attribute_gap_to_peers(interval)
        if self.max_seq is None or seq > self.max_seq:
            self.max_seq = seq
        self.last_ts = msg.ts
        return True

    def _attribute_gap_to_peers(self, interval):
        """序号缺口期间处于 up 状态的 peer 需要按序号范围重同步。"""
        for state in self.peers.values():
            if state.up:
                state.resync_ranges.append(
                    {
                        "type": "seq_range",
                        "start_seq": interval.start_seq,
                        "end_seq": interval.end_seq,
                        "during_reconnect": interval.during_reconnect,
                    }
                )

    def _process_one(self, msg):
        if not self._track_seq(msg):
            return
        handler = {
            MessageType.INITIATION: self._on_initiation,
            MessageType.PEER_UP: self._on_peer_up,
            MessageType.PEER_DOWN: self._on_peer_down,
            MessageType.ROUTE_MONITORING: self._on_route,
            MessageType.STATISTICS: self._on_statistics,
            MessageType.TERMINATION: self._on_termination,
        }[msg.msg_type]
        handler(msg)

    def _on_initiation(self, msg):
        if self.initiated:
            # 监控会话重建：所有 peer 状态不可信，全部重置。
            for state in self.peers.values():
                state.up = False
                state.reset(msg.ts, msg.session, "monitor_session_restart")
        self.initiated = True

    def _on_termination(self, msg):
        for state in self.peers.values():
            state.up = False

    def _on_peer_up(self, msg):
        state = self._peer_state(msg.peer)
        if state.up:
            # 未收到 Peer Down 却再次 Peer Up：视为 peer restart。
            state.reset(msg.ts, msg.session, "peer_restart")
        else:
            state.timeline.append(("reset", msg.ts, msg.session))
        state.up = True
        state.session = msg.session

    def _on_peer_down(self, msg):
        state = self._peer_state(msg.peer)
        state.up = False
        # Peer Down 后路由不再有效，清空当前 RIB。
        state.timeline.append(("reset", msg.ts, msg.session))
        state.last_route_seq = {}
        state.resync_ranges.append(
            {
                "type": "full",
                "ts": msg.ts,
                "session": msg.session,
                "reason": "peer_down",
            }
        )

    def _on_statistics(self, msg):
        if msg.peer is not None:
            self._peer_state(msg.peer).statistics.append(msg)

    def _on_route(self, msg):
        if msg.peer is None:
            self.report.anomalies.append(
                "route_monitoring without peer at seq %d" % msg.seq
            )
            return
        state = self._peer_state(msg.peer)
        if not state.up:
            self.report.anomalies.append(
                "route_monitoring for down peer %s at seq %d" % (msg.peer, msg.seq)
            )
            return
        last_seq = state.last_route_seq.get(msg.prefix)
        if last_seq is not None and msg.seq < last_seq:
            # 晚到的乱序消息：不得覆盖更新的状态。
            if msg.action == "withdraw":
                self.report.late_withdrawals += 1
            else:
                self.report.late_updates += 1
            return
        state.last_route_seq[msg.prefix] = msg.seq
        state.timeline.append(
            ("route", msg.ts, msg.session, msg.action, msg.prefix, dict(msg.attrs))
        )
