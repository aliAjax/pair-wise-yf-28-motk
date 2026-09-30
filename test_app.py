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


class SegmentManagementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "seg.db"
        self.store = RandomizationStore(self.db)
        self.store.seed()
        self.trial = self.store.create_trial(
            "coord", "号段托管试验", "v1.0", ["A", "B"], ["risk"], 4, "seed-seg-001"
        )
        self.store.start_trial("coord", self.trial["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def _seg(self, site, count, factors=None):
        return self.store.create_segment("coord", self.trial["id"], site, factors or {"risk": "low"}, count)

    def test_segments_are_disjoint_across_centers(self):
        s1 = self._seg("S001", 3)
        s2 = self._seg("S002", 3)
        s3 = self._seg("S001", 2)
        ranges = [(s1["start_no"], s1["end_no"]), (s2["start_no"], s2["end_no"]), (s3["start_no"], s3["end_no"])]
        self.assertEqual(ranges, [(1, 3), (4, 6), (7, 8)])
        # 其他中心不能领取同一序列：编号全局唯一
        with self.store.connect() as conn:
            dup = conn.execute(
                "SELECT COUNT(*) c FROM (SELECT seq_no FROM segment_numbers WHERE trial_id=? GROUP BY seq_no HAVING COUNT(*)>1)",
                (self.trial["id"],),
            ).fetchone()["c"]
        self.assertEqual(dup, 0)

    def test_write_off_idempotent_and_only_once(self):
        seg = self._seg("S001", 2)
        p1 = self.store.write_off_number("site1", seg["id"], "S001-001", {"risk": "low"})
        p2 = self.store.write_off_number("site1", seg["id"], "S001-001", {"risk": "low"})
        self.assertEqual(p1["id"], p2["id"])
        self.assertTrue(p2["idempotent"])
        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants WHERE trial_id=? AND external_id=?",
                                          (self.trial["id"], "S001-001")).fetchone()[0], 1)
        # 号段已无可用名额
        self.store.write_off_number("site1", seg["id"], "S001-002", {"risk": "low"})
        with self.assertRaises(BusinessError) as ctx:
            self.store.write_off_number("site1", seg["id"], "S001-003", {"risk": "low"})
        self.assertEqual(ctx.exception.code, "no_number_available")

    def test_release_recycles_unused_but_not_written_off(self):
        seg = self._seg("S001", 5)
        self.store.write_off_number("site1", seg["id"], "S001-001", {"risk": "low"})
        self.store.write_off_number("site1", seg["id"], "S001-002", {"risk": "low"})
        released = self.store.release_segment("coord", seg["id"], confirm=True)
        self.assertEqual(released["released_count"], 3)
        self.assertEqual(released["written_off_count"], 2)
        # 释放需确认
        with self.assertRaises(BusinessError) as ctx:
            self.store.release_segment("coord", seg["id"])
        self.assertEqual(ctx.exception.code, "confirm_required")
        # 重复释放报错
        with self.assertRaises(BusinessError) as ctx:
            self.store.release_segment("coord", seg["id"], confirm=True)
        self.assertEqual(ctx.exception.code, "already_released")
        # 新号段回收未用编号（3-5），已核销 1-2 不回收
        again = self._seg("S001", 3)
        self.assertEqual((again["start_no"], again["end_no"]), (3, 5))

    def test_batch_upload_persists_and_resumes_after_restart(self):
        seg = self._seg("S001", 2)
        items = [{"external_id": f"S001-{i:03d}", "factors": {"risk": "low"}} for i in range(1, 6)]
        batch = self.store.create_upload_batch("site1", self.trial["id"], items, segment_id=seg["id"])
        self.assertEqual(batch["status"], "partial")
        self.assertEqual(batch["processed_count"], 2)
        # 模拟重启：新 store 实例，同一数据库
        restarted = RandomizationStore(self.db)
        seg2 = restarted.create_segment("coord", self.trial["id"], "S001", {"risk": "low"}, 3)
        self.assertEqual((seg2["start_no"], seg2["end_no"]), (3, 5))
        restarted.resume_pending_batches()
        after = restarted.get_upload_batch("site1", batch["id"])
        self.assertEqual(after["status"], "completed")
        self.assertEqual(after["processed_count"], 5)
        # 同一外部编号只核销一次
        with restarted.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM participants WHERE trial_id=?",
                                          (self.trial["id"],)).fetchone()[0], 5)

    def test_batch_retry_keeps_segment_and_continues(self):
        seg = self._seg("S001", 1)
        items = [{"external_id": "S001-001", "factors": {"risk": "low"}},
                 {"external_id": "S001-002", "factors": {"risk": "low"}}]
        batch = self.store.create_upload_batch("site1", self.trial["id"], items, segment_id=seg["id"])
        self.assertEqual(batch["status"], "partial")
        self.assertEqual(batch["items"][1]["status"], "failed")
        # 补领后重试，原号段保留
        self._seg("S001", 1)
        retried = self.store.process_upload_batch("site1", batch["id"])
        self.assertEqual(retried["status"], "completed")
        self.assertEqual(retried["processed_count"], 2)

    def test_site_deactivation_releases_unused_keeps_issued_and_audit(self):
        seg = self._seg("S001", 5)
        self.store.write_off_number("site1", seg["id"], "S001-001", {"risk": "low"})
        result = self.store.deactivate_site("coord", "S001")
        self.assertEqual(result["released_numbers"], 4)
        # 停用后中心用户不能操作
        with self.assertRaises(BusinessError) as ctx:
            self.store.write_off_number("site1", seg["id"], "S001-002", {"risk": "low"})
        self.assertEqual(ctx.exception.code, "site_deactivated")
        # 已发编号和审计可查
        p = self.store.get_participant("coord", 1)
        self.assertEqual(p["external_id"], "S001-001")
        audit = self.store.list_audit("coord", self.trial["id"])
        self.assertTrue(any(a["action"] == "site.deactivate" for a in audit))
        # 重新启用后可继续
        self.store.activate_site("coord", "S001")
        seg2 = self._seg("S001", 2)
        self.assertEqual(seg2["status"], "active")

    def test_site_isolation_for_segments(self):
        self._seg("S001", 2)
        self._seg("S002", 2)
        s1 = self.store.list_segments("site1", self.trial["id"])
        s2 = self.store.list_segments("site2", self.trial["id"])
        self.assertEqual([s["site_id"] for s in s1], ["S001"])
        self.assertEqual([s["site_id"] for s in s2], ["S002"])
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_segment("site2", s1[0]["id"])
        self.assertEqual(ctx.exception.code, "site_isolation")


if __name__ == "__main__":
    unittest.main()
