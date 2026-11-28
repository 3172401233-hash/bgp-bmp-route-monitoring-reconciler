import unittest

from bmp_reconciler import MessageType, Reconciler, make_message

PEER = "10.0.0.1"


def msg(msg_type, seq, ts, **kw):
    raw = {"msg_type": msg_type, "seq": seq, "ts": ts}
    raw.update(kw)
    return make_message(raw)


def peer_up(seq, ts, peer=PEER, session=1):
    return msg(MessageType.PEER_UP, seq, ts, peer=peer, session=session)


def peer_down(seq, ts, peer=PEER, session=1, reason=3):
    return msg(
        MessageType.PEER_DOWN, seq, ts, peer=peer, session=session, reason=reason
    )


def announce(seq, ts, prefix, attrs=None, peer=PEER):
    return msg(
        MessageType.ROUTE_MONITORING,
        seq,
        ts,
        peer=peer,
        action="announce",
        prefix=prefix,
        attrs=attrs or {"next_hop": "192.0.2.1"},
    )


def withdraw(seq, ts, prefix, peer=PEER):
    return msg(
        MessageType.ROUTE_MONITORING,
        seq,
        ts,
        peer=peer,
        action="withdraw",
        prefix=prefix,
    )


def base_session():
    return [
        msg(MessageType.INITIATION, 1, 100.0, sys_descr="router-a"),
        peer_up(2, 100.1),
    ]


class ReconnectGapTest(unittest.TestCase):
    """重连导致序号缺口：缺失区间被记录并标记，缺失路由进入不一致清单。"""

    def test_gap_during_reconnect(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            announce(4, 100.3, "198.51.100.0/24"),
            # seq 5..9 在重连区间 [105.0, 115.0] 内丢失
            announce(10, 120.0, "203.0.113.0/24"),
            msg(
                MessageType.STATISTICS,
                11,
                120.1,
                peer=PEER,
                counters={"prefixes": 3},
            ),
        ]
        rec = Reconciler(reconnect_intervals=[(105.0, 115.0)])
        report = rec.process(messages)

        self.assertEqual(len(report.missing_intervals), 1)
        gap = report.missing_intervals[0]
        self.assertEqual((gap.start_seq, gap.end_seq), (5, 9))
        self.assertTrue(gap.during_reconnect)

        # 缺口期间 peer 在线 -> 需要按序号范围重同步
        ranges = report.resync_ranges[PEER]
        seq_ranges = [r for r in ranges if r["type"] == "seq_range"]
        self.assertEqual(len(seq_ranges), 1)
        self.assertEqual((seq_ranges[0]["start_seq"], seq_ranges[0]["end_seq"]), (5, 9))

        # 快照中包含缺口里丢失的路由 -> 报告不一致前缀
        snapshot = {
            PEER: {
                "192.0.2.0/24": {"next_hop": "192.0.2.1"},
                "198.51.100.0/24": {"next_hop": "192.0.2.1"},
                "203.0.113.0/24": {"next_hop": "192.0.2.1"},
                "203.0.113.128/25": {"next_hop": "192.0.2.1"},
            }
        }
        diff = rec.compare_with_snapshot(snapshot)
        self.assertEqual(diff[PEER]["missing_in_monitor"], ["203.0.113.128/25"])
        self.assertNotIn("extra_in_monitor", diff[PEER])

    def test_gap_outside_reconnect_not_flagged(self):
        messages = base_session()
        messages += [announce(3, 100.2, "192.0.2.0/24"), announce(7, 100.6, "198.51.100.0/24")]
        rec = Reconciler(reconnect_intervals=[(200.0, 300.0)])
        report = rec.process(messages)
        self.assertEqual(len(report.missing_intervals), 1)
        self.assertFalse(report.missing_intervals[0].during_reconnect)


class DuplicateRouteTest(unittest.TestCase):
    """重复消息（同序号）只应用一次，路由不重复计数。"""

    def test_duplicate_messages_deduplicated(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            announce(3, 100.2, "192.0.2.0/24"),  # 完全重复
            announce(4, 100.3, "198.51.100.0/24", {"next_hop": "192.0.2.2"}),
            peer_up(2, 100.1),  # 重复的控制消息
            withdraw(5, 100.4, "198.51.100.0/24"),
            withdraw(5, 100.4, "198.51.100.0/24"),  # 重复撤销
        ]
        rec = Reconciler()
        report = rec.process(messages)

        self.assertEqual(sorted(report.duplicates), [2, 3, 5])
        rib = report.final_rib[PEER]
        self.assertEqual(set(rib), {"192.0.2.0/24"})
        self.assertEqual(rib["192.0.2.0/24"], {"next_hop": "192.0.2.1"})

    def test_duplicate_statistics_ignored(self):
        messages = base_session()
        stat = msg(
            MessageType.STATISTICS, 3, 100.2, peer=PEER, counters={"prefixes": 1}
        )
        rec = Reconciler()
        report = rec.process(messages + [stat, stat])
        self.assertEqual(report.duplicates, [3])
        self.assertEqual(len(rec.peers[PEER].statistics), 1)


class PeerRestartTest(unittest.TestCase):
    """Peer 重启：旧 RIB 清空，要求全量重同步，新会话路由独立生效。"""

    def test_peer_down_up_clears_rib(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            announce(4, 100.3, "198.51.100.0/24"),
            peer_down(5, 110.0),
            peer_up(6, 120.0, session=2),
            announce(7, 120.1, "203.0.113.0/24"),
        ]
        rec = Reconciler()
        report = rec.process(messages)

        # 重启前的时间点仍能看到旧 RIB
        rib_before = rec.rib_at(PEER, 105.0)
        self.assertEqual(set(rib_before), {"192.0.2.0/24", "198.51.100.0/24"})
        # down 之后、up 之前 RIB 为空
        self.assertEqual(rec.rib_at(PEER, 115.0), {})
        # 最终 RIB 只包含新会话路由
        self.assertEqual(set(report.final_rib[PEER]), {"203.0.113.0/24"})
        # 需要全量重同步
        full = [r for r in report.resync_ranges[PEER] if r["type"] == "full"]
        self.assertTrue(any(r["reason"] == "peer_down" for r in full))

    def test_peer_up_without_down_counts_restart(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            peer_up(4, 110.0, session=2),  # 未收到 Peer Down 的重复 Peer Up
            announce(5, 110.1, "198.51.100.0/24"),
        ]
        rec = Reconciler()
        report = rec.process(messages)

        self.assertEqual(report.restarts.get(PEER), 1)
        self.assertEqual(set(report.final_rib[PEER]), {"198.51.100.0/24"})
        full = [r for r in report.resync_ranges[PEER] if r["type"] == "full"]
        self.assertTrue(any(r["reason"] == "peer_restart" for r in full))

    def test_monitor_session_restart_resets_all_peers(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            msg(MessageType.INITIATION, 4, 110.0, sys_descr="router-a"),
            peer_up(5, 110.1, session=2),
            announce(6, 110.2, "203.0.113.0/24"),
        ]
        rec = Reconciler()
        report = rec.process(messages)
        self.assertEqual(set(report.final_rib[PEER]), {"203.0.113.0/24"})
        full = [r for r in report.resync_ranges[PEER] if r["type"] == "full"]
        self.assertTrue(
            any(r["reason"] == "monitor_session_restart" for r in full)
        )


class LateAndUpdateTest(unittest.TestCase):
    """撤销晚到与前缀属性更新。"""

    def test_late_withdraw_ignored(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24", {"local_pref": 200}),
            # 乱序：先收到较新的 announce，再收到更旧的 withdraw
            announce(5, 100.4, "192.0.2.0/24", {"local_pref": 100}),
            withdraw(4, 100.3, "192.0.2.0/24"),
        ]
        rec = Reconciler()
        report = rec.process(messages)
        self.assertEqual(report.late_withdrawals, 1)
        self.assertEqual(
            report.final_rib[PEER]["192.0.2.0/24"], {"local_pref": 100}
        )

    def test_attribute_update_overwrites(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24", {"local_pref": 100}),
            announce(4, 100.3, "192.0.2.0/24", {"local_pref": 300}),
        ]
        rec = Reconciler()
        report = rec.process(messages)
        self.assertEqual(
            report.final_rib[PEER]["192.0.2.0/24"], {"local_pref": 300}
        )
        # 中间时间点的属性仍是旧值
        self.assertEqual(
            rec.rib_at(PEER, 100.25)["192.0.2.0/24"], {"local_pref": 100}
        )

    def test_withdraw_removes_prefix(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24"),
            withdraw(4, 100.3, "192.0.2.0/24"),
        ]
        rec = Reconciler()
        report = rec.process(messages)
        self.assertEqual(report.final_rib[PEER], {})


class SnapshotCompareTest(unittest.TestCase):
    def test_extra_and_attr_mismatch(self):
        messages = base_session()
        messages += [
            announce(3, 100.2, "192.0.2.0/24", {"local_pref": 100}),
            announce(4, 100.3, "198.51.100.0/24"),
        ]
        rec = Reconciler()
        rec.process(messages)
        snapshot = {
            PEER: {
                "192.0.2.0/24": {"local_pref": 200},
                "203.0.113.0/24": {"next_hop": "192.0.2.1"},
            }
        }
        diff = rec.compare_with_snapshot(snapshot)
        entry = diff[PEER]
        self.assertEqual(entry["missing_in_monitor"], ["203.0.113.0/24"])
        self.assertEqual(entry["extra_in_monitor"], ["198.51.100.0/24"])
        self.assertEqual(
            entry["attr_mismatch"]["192.0.2.0/24"],
            {"expected": {"local_pref": 200}, "actual": {"local_pref": 100}},
        )


if __name__ == "__main__":
    unittest.main()
