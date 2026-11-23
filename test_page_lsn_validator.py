"""page_lsn_validator 的单元测试。

覆盖：页刷新竞争、WAL 尾部丢失、双写副本、重复刷盘与部分页写的区分。
"""

import unittest

from page_lsn_validator import (
    BufferedPage,
    DiskPage,
    DoubleWriteCopy,
    FlushAnomaly,
    FlushEvent,
    PageLSNValidator,
    PageStatus,
    RecoverySnapshot,
    analyze_flush_events,
)


def make_snapshot(**kwargs):
    defaults = dict(wal_persisted_lsn=1000, checkpoint_lsn=500)
    defaults.update(kwargs)
    return RecoverySnapshot(**defaults)


class FlushRaceTest(unittest.TestCase):
    """后台刷盘与 WAL 写入并发推进时的竞争场景。"""

    def test_page_flushed_ahead_of_wal_is_corrupted(self):
        # 刷盘线程把 page_lsn=1200 的页写盘，WAL 线程只持久化到 1000。
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=7, file_lsn=1200)],
            buffered_pages=[BufferedPage(page_id=7, page_lsn=1200, dirty=False)],
            flush_events=[FlushEvent(page_id=7, lsn=1200, sequence=1)],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual(len(report.corrupted), 1)
        self.assertEqual(report.corrupted[0].page_id, 7)
        self.assertEqual(report.corrupted[0].status, PageStatus.CORRUPTED)
        self.assertFalse(report.acceptable)
        self.assertFalse(report.needs_replay)

    def test_stale_flush_detected_in_race(self):
        # 同一页先刷 lsn=900 后又被旧版本 lsn=800 覆盖（写回竞争）。
        events = [
            FlushEvent(page_id=3, lsn=900, sequence=1),
            FlushEvent(page_id=3, lsn=800, sequence=2),
        ]
        issues = analyze_flush_events(events)
        anomalies = [i.anomaly for i in issues]
        self.assertIn(FlushAnomaly.STALE_FLUSH, anomalies)
        self.assertNotIn(FlushAnomaly.REPEATED_FLUSH, anomalies)

    def test_clean_page_at_wal_boundary_is_acceptable(self):
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=1, file_lsn=1000)],
            buffered_pages=[BufferedPage(page_id=1, page_lsn=1000, dirty=False)],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual([v.page_id for v in report.acceptable], [1])


class WalTailLossTest(unittest.TestCase):
    """WAL 尾部丢失：持久化点之后的 WAL 记录不可用。"""

    def test_mixed_pages_after_wal_tail_loss(self):
        snapshot = make_snapshot(
            wal_persisted_lsn=1000,
            disk_pages=[
                DiskPage(page_id=1, file_lsn=1000),
                DiskPage(page_id=2, file_lsn=700),
                DiskPage(page_id=3, file_lsn=1500),
            ],
            buffered_pages=[
                BufferedPage(page_id=1, page_lsn=1000, dirty=False),
                BufferedPage(page_id=2, page_lsn=950, dirty=True),
                BufferedPage(page_id=3, page_lsn=1500, dirty=False),
            ],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual([v.page_id for v in report.acceptable], [1])
        self.assertEqual([v.page_id for v in report.needs_replay], [2])
        self.assertEqual([v.page_id for v in report.corrupted], [3])
        self.assertEqual(report.needs_replay[0].redo_from_lsn, 700)

    def test_unflushed_dirty_page_needs_replay_not_corruption(self):
        # 脏页未刷盘：磁盘页落后于 WAL，重放即可恢复，不算损坏。
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=9, file_lsn=600)],
            buffered_pages=[BufferedPage(page_id=9, page_lsn=1000, dirty=True)],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual(len(report.needs_replay), 1)
        self.assertFalse(report.corrupted)

    def test_checkpoint_above_wal_persisted_rejected(self):
        snapshot = make_snapshot(wal_persisted_lsn=1000, checkpoint_lsn=1200)
        with self.assertRaises(ValueError):
            PageLSNValidator().validate(snapshot)


class DoubleWriteTest(unittest.TestCase):
    """双写副本对部分页写的修复。"""

    def test_torn_page_recovered_via_double_write(self):
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=4, file_lsn=800, torn=True)],
            double_write_copies=[DoubleWriteCopy(page_id=4, page_lsn=800)],
            flush_events=[
                FlushEvent(page_id=4, lsn=800, complete=False, sequence=1),
            ],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual(len(report.needs_replay), 1)
        verdict = report.needs_replay[0]
        self.assertTrue(verdict.via_double_write)
        self.assertEqual(verdict.redo_from_lsn, 800)
        self.assertFalse(report.corrupted)
        anomalies = [i.anomaly for i in report.flush_issues]
        self.assertIn(FlushAnomaly.PARTIAL_FLUSH, anomalies)

    def test_torn_page_without_copy_is_corrupted(self):
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=5, file_lsn=800, torn=True)],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual([v.page_id for v in report.corrupted], [5])

    def test_double_write_copy_ahead_of_wal_is_corrupted(self):
        # 副本本身也包含未持久化 WAL 描述的修改，无法安全恢复。
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=6, file_lsn=900, torn=True)],
            double_write_copies=[DoubleWriteCopy(page_id=6, page_lsn=1100)],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual(len(report.corrupted), 1)
        self.assertTrue(report.corrupted[0].via_double_write)

    def test_damaged_copy_does_not_save_torn_page(self):
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=8, file_lsn=800, torn=True)],
            double_write_copies=[
                DoubleWriteCopy(page_id=8, page_lsn=800, intact=False),
            ],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual([v.page_id for v in report.corrupted], [8])


class RepeatedFlushTest(unittest.TestCase):
    """重复刷盘与部分页写必须区分开。"""

    def test_repeated_flush_is_idempotent(self):
        events = [
            FlushEvent(page_id=2, lsn=900, sequence=1),
            FlushEvent(page_id=2, lsn=900, sequence=2),
        ]
        issues = analyze_flush_events(events)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].anomaly, FlushAnomaly.REPEATED_FLUSH)

    def test_partial_flush_not_confused_with_repeat(self):
        events = [
            FlushEvent(page_id=2, lsn=900, sequence=1),
            FlushEvent(page_id=2, lsn=900, complete=False, sequence=2),
        ]
        issues = analyze_flush_events(events)
        anomalies = [i.anomaly for i in issues]
        self.assertIn(FlushAnomaly.PARTIAL_FLUSH, anomalies)
        self.assertNotIn(FlushAnomaly.REPEATED_FLUSH, anomalies)

    def test_repeated_flush_keeps_page_acceptable(self):
        snapshot = make_snapshot(
            disk_pages=[DiskPage(page_id=2, file_lsn=900)],
            buffered_pages=[BufferedPage(page_id=2, page_lsn=900, dirty=False)],
            flush_events=[
                FlushEvent(page_id=2, lsn=900, sequence=1),
                FlushEvent(page_id=2, lsn=900, sequence=2),
            ],
        )
        report = PageLSNValidator().validate(snapshot)
        self.assertEqual([v.page_id for v in report.acceptable], [2])
        self.assertEqual(len(report.flush_issues), 1)
        self.assertEqual(report.flush_issues[0].anomaly,
                         FlushAnomaly.REPEATED_FLUSH)


if __name__ == "__main__":
    unittest.main()
