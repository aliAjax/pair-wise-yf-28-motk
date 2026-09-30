import tempfile
import unittest
from collections import Counter
from pathlib import Path

from app import BusinessError, RandomizationStore


class RandomizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RandomizationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "多中心降压研究", "v1.0", ["A", "B"], ["risk"], 4, "seed-2026-001"
        )
        self.store.start_trial("coord", self.trial["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_stratified_block_randomization_and_two_person_unblinding(self):
        participants = [
            self.store.enroll("site1", self.trial["id"], f"S001-{i:03d}", {"risk": "low"})
            for i in range(1, 5)
        ]
        self.assertNotIn("arm", participants[0])
        with self.store.connect() as conn:
            arms = [r["arm"] for r in conn.execute(
                "SELECT a.arm FROM allocations a JOIN participants p ON p.allocation_id=a.id WHERE p.trial_id=? ORDER BY p.id",
                (self.trial["id"],),
            ).fetchall()]
        self.assertEqual(Counter(arms), Counter({"A": 2, "B": 2}))
        request = self.store.request_unblinding("site1", participants[0]["id"], "受试者发生严重不良事件需要紧急处理")
        first = self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(first["status"], "pending")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_unblinding("monitor1", request["id"])
        self.assertEqual(ctx.exception.code, "distinct_approver_required")
        second = self.store.approve_unblinding("monitor2", request["id"])
        self.assertEqual(second["status"], "approved")
        self.assertIn(second["arm"], {"A", "B"})

    def test_idempotent_enrollment_site_isolation_and_protocol_lock(self):
        first = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        again = self.store.enroll("site1", self.trial["id"], "S001-001", {"risk": "high"})
        self.assertEqual(first["id"], again["id"])
        self.assertTrue(again["idempotent"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_participant("site2", first["id"])
        self.assertEqual(ctx.exception.code, "site_isolation")
        with self.assertRaises(BusinessError) as ctx:
            self.store.update_protocol("coord", self.trial["id"], "v2")
        self.assertEqual(ctx.exception.code, "protocol_locked")


class HostedSegmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RandomizationStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "多中心降糖研究", "v1.0", ["A", "B"], ["risk"], 4, "seed-2026-002"
        )
        self.store.start_trial("coord", self.trial["id"])
        self.tid = self.trial["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_segments_are_contiguous_and_centers_never_collide(self):
        s1 = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 5)
        s2 = self.store.reserve_segment("coord", self.tid, "S002", {"risk": "low"}, 5)
        s3 = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "high"}, 3)
        self.assertEqual((s1["seq_start"], s1["seq_end"]), (1, 5))
        self.assertEqual((s2["seq_start"], s2["seq_end"]), (6, 10))
        self.assertEqual((s3["seq_start"], s3["seq_end"]), (1, 3))
        with self.store.connect() as conn:
            overlap = conn.execute(
                """SELECT a.sequence FROM segment_numbers a JOIN segment_numbers b
                   ON a.trial_id=b.trial_id AND a.stratum_key=b.stratum_key AND a.sequence=b.sequence
                  WHERE a.state='reserved' AND b.state='reserved' AND a.id<b.id"""
            ).fetchall()
            self.assertEqual(overlap, [])

    def test_upload_reconcile_idempotency_and_cross_center_external_id(self):
        seg1 = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 4)
        seg2 = self.store.reserve_segment("coord", self.tid, "S002", {"risk": "low"}, 4)
        items = [{"external_id": f"P-{i}", "sequence": seg1["seq_start"] + i} for i in range(3)]
        result = self.store.upload_offline("site1", self.tid, seg1["id"], items)
        self.assertEqual(result["summary"], {"received": 3, "reconciled": 3, "failed": 0})
        again = self.store.upload_offline("site1", self.tid, seg1["id"], items[:1])
        self.assertEqual(again["results"][0]["status"], "reconciled")
        self.assertTrue(again["results"][0]["idempotent"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 3)
        clash = self.store.upload_offline("site2", self.tid, seg2["id"],
                                          [{"external_id": "P-0", "sequence": seg2["seq_start"]}])
        self.assertEqual(clash["results"][0]["status"], "failed")
        self.assertEqual(clash["results"][0]["code"], "external_id_consumed")
        # 失败保留原号段：编号仍是预留，可继续核销别的号
        follow = self.store.upload_offline("site2", self.tid, seg2["id"],
                                           [{"external_id": "Q-0", "sequence": seg2["seq_start"]}])
        self.assertEqual(follow["summary"]["reconciled"], 1)

    def test_failed_items_persist_and_retry_after_restart(self):
        seg = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 4)
        out = [{"external_id": "BAD-1", "sequence": 999}]
        good = [{"external_id": "OK-1", "sequence": seg["seq_start"]}]
        result = self.store.upload_offline("site1", self.tid, seg["id"], out + good)
        self.assertEqual(result["summary"]["failed"], 1)
        # 模拟重启：重新打开 store（同一 db 文件），未核销清单还在
        reopened = RandomizationStore(Path(self.tmp.name) / "test.db")
        detail = reopened.get_segment("site1", seg["id"])
        self.assertEqual(detail["diff"]["unreconciled"], 1)
        bad_seq = seg["seq_start"] + 1
        retry = reopened.upload_offline("site1", self.tid, seg["id"],
                                        [{"external_id": "BAD-1", "sequence": bad_seq}])
        # 同一外部编号只核销一次：原 failed 台账按新序号重试成功
        self.assertEqual(retry["results"][0]["status"], "reconciled")
        self.assertEqual(retry["results"][0]["sequence"], bad_seq)
        empty = reopened.retry_segment("site1", seg["id"])
        self.assertEqual(empty["summary"]["retried"], 0)

    def test_release_keeps_used_and_recycles_only_unused(self):
        seg = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 6)
        self.store.upload_offline("site1", self.tid, seg["id"], [
            {"external_id": f"U-{i}", "sequence": seg["seq_start"] + i} for i in range(2)
        ])
        released = self.store.release_segment("coord", seg["id"])
        self.assertEqual(released["status"], "released")
        self.assertEqual(released["counts"]["used"], 2)
        self.assertEqual(released["counts"]["released"], 4)
        # 已核销编号不回收：参与者仍在
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 2)
        # 释放出的连续号可被新号段复用（同一分层）
        again = self.store.reserve_segment("coord", self.tid, "S002", {"risk": "low"}, 3)
        self.assertEqual((again["seq_start"], again["seq_end"]), (3, 5))

    def test_release_blocked_while_pending_items_exist(self):
        seg = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 4)
        self.store.upload_offline("site1", self.tid, seg["id"], [{"external_id": "X", "sequence": 404}])
        with self.assertRaises(BusinessError) as ctx:
            self.store.release_segment("coord", seg["id"])
        self.assertEqual(ctx.exception.code, "pending_items")

    def test_site_deactivation_releases_unused_segments_and_keeps_audit(self):
        seg = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 4)
        self.store.upload_offline("site1", self.tid, seg["id"], [
            {"external_id": "KEEP-1", "sequence": seg["seq_start"]}
        ])
        result = self.store.set_site_active("coord", "S001", False)
        self.assertEqual(result["released_segments"][0]["released_numbers"], 3)
        with self.assertRaises(BusinessError):
            self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 2)
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0], 1)
            actions = {r["action"] for r in conn.execute("SELECT action FROM audit_log").fetchall()}
        self.assertIn("site.set_active", actions)
        self.assertIn("offline.reconcile", actions)

    def test_diff_view_and_site_isolation(self):
        seg = self.store.reserve_segment("coord", self.tid, "S001", {"risk": "low"}, 3)
        self.store.upload_offline("site1", self.tid, seg["id"], [
            {"external_id": "D-1", "sequence": seg["seq_start"]},
            {"external_id": "D-2", "sequence": 999},
        ])
        detail = self.store.get_segment("coord", seg["id"])
        self.assertEqual(detail["diff"]["reconciled"], 1)
        self.assertEqual(detail["diff"]["unreconciled"], 1)
        self.assertEqual(detail["diff"]["unused"], 2)
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_segment("site2", seg["id"])
        self.assertEqual(ctx.exception.code, "site_isolation")
        listing = self.store.list_segments("site2", self.tid)
        self.assertEqual(listing, [])


if __name__ == "__main__":
    unittest.main()
