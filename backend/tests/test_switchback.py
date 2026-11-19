"""switchback 引擎规则测试（纯标准库，python -m unittest 可运行）。"""
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.switchback.design import (  # noqa: E402
    build_design,
    lock_design,
    design_lock_checksum,
)
from src.switchback.events import RawEvent, ingest_events, IMPRESSION, CLICK  # noqa: E402
from src.switchback.reports import (  # noqa: E402
    MISSING,
    CORRECTED,
    MetricBatch,
    Stream,
    ReportError,
)
from src.switchback.store import ExperimentStore, PENDING, SEALED  # noqa: E402
from src.switchback.analysis import (  # noqa: E402
    paired_swap_test,
    seal_conclusion,
)
from src.switchback.ledger import Ledger  # noqa: E402


def make_store(seed=7, pairs=4, seg_len=60, buf=10):
    design = lock_design(
        build_design("exp-test", segment_count=pairs * 2,
                     segment_length=seg_len, buffer_length=buf, rng_seed=seed)
    )
    return ExperimentStore(design), design


class DesignTests(unittest.TestCase):
    def test_pairing_adjacent_equal_segments_and_random_order(self):
        d1 = lock_design(build_design("e", 8, 30, 5, rng_seed=1))
        d2 = lock_design(build_design("e", 8, 30, 5, rng_seed=2))
        # 相邻等长
        for pid, (a, b) in d1.pairs():
            self.assertEqual(a.length, b.length)
            self.assertEqual(a.end_offset, b.start_offset)
            # 对内必须是 A/B 互换
            self.assertNotEqual(a.assigned_version, b.assigned_version)
        # 不同种子产生不同先后序列
        orders1 = [x.assigned_version for s in d1.segments for x in [s]]
        orders2 = [s.assigned_version for s in d2.segments]
        self.assertNotEqual(orders1, orders2)

    def test_odd_segment_count_rejected(self):
        with self.assertRaises(ValueError):
            build_design("e", 7, 30, 5)

    def test_lock_is_immutable_and_tamper_detected(self):
        d = lock_design(build_design("e", 4, 30, 5, rng_seed=3))
        with self.assertRaises(ValueError):
            lock_design(d)  # 重复锁定拒绝
        # 篡改锁定内容后校验失败（Segment 为冻结对象，用底层赋值模拟外部改写）
        seg = d.segments[0]
        tampered = "B" if seg.assigned_version == "A" else "A"
        object.__setattr__(seg, "assigned_version", tampered)
        self.assertNotEqual(design_lock_checksum(d), d.lock_checksum)
        with self.assertRaises(ValueError):
            ExperimentStore(d)

    def test_buffer_must_be_shorter_than_segment(self):
        with self.assertRaises(ValueError):
            build_design("e", 4, 10, 10)


class EventAttributionTests(unittest.TestCase):
    def test_buffer_events_excluded(self):
        _, d = make_store()
        seg0 = d.segments[0]
        events = [
            RawEvent("e1", seg0.start_offset + 1, IMPRESSION, "A"),       # 缓冲内
            RawEvent("e2", seg0.start_offset + 1, CLICK, "A"),            # 缓冲内
            RawEvent("e3", seg0.buffer_end_offset, IMPRESSION, "A"),      # 缓冲外边界
        ]
        res = ingest_events(d, events)
        t = res.tallies[seg0.segment_id]
        self.assertEqual(t.buffered_impressions, 1)
        self.assertEqual(t.buffered_clicks, 1)
        self.assertEqual(t.impressions, 1)   # 只有 e3
        self.assertEqual(t.clicks, 0)
        self.assertIn("e1", t.dropped_event_ids)

    def test_misdelivery_attributed_to_assigned_version(self):
        _, d = make_store()
        seg = next(s for s in d.segments if s.assigned_version == "A")
        events = [
            RawEvent("m1", seg.buffer_end_offset + 1, IMPRESSION, "B"),  # 错播成 B
            RawEvent("m2", seg.buffer_end_offset + 2, CLICK, "B"),       # 错播成 B
        ]
        res = ingest_events(d, events)
        t = res.tallies[seg.segment_id]
        # 仍归原分派 A：计数进入该段（assigned=A），并留错播痕
        self.assertEqual(t.impressions, 1)
        self.assertEqual(t.clicks, 1)
        self.assertEqual(t.misdelivered_impressions, 1)
        self.assertEqual(t.misdelivered_clicks, 1)
        self.assertEqual(t.assigned_version, "A")

    def test_duplicate_event_rejected_and_out_of_range(self):
        _, d = make_store()
        seg = d.segments[0]
        with self.assertRaises(Exception):
            ingest_events(d, [
                RawEvent("dup", seg.buffer_end_offset, IMPRESSION, "A"),
                RawEvent("dup", seg.buffer_end_offset + 1, IMPRESSION, "A"),
            ])
        res = ingest_events(d, [RawEvent("x", 10 ** 9, IMPRESSION, "A")])
        self.assertEqual(res.out_of_range_event_ids, ["x"])


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.store, _ = make_store()
        self.ids = self.store.segment_ids

    def _batch(self, stream, seq, values, **kw):
        return MetricBatch(batch_id=f"b-{stream.value}-{seq}", stream=stream,
                           seq=seq, values=values, **kw)

    def test_zero_value_vs_missing_are_distinct(self):
        # 第一段显式零值，第二段不上报
        self.store.ingest_batch(self._batch(Stream.IMPRESSIONS, 1, {self.ids[0]: 0}))
        rep0 = self.store.segment_report(self.ids[0])
        rep1 = self.store.segment_report(self.ids[1])
        self.assertEqual(rep0.impressions, 0)                    # 显式零
        self.assertTrue(rep0.impressions_state.is_complete)
        self.assertIsNone(rep1.impressions)                      # 缺报
        self.assertIn("impressions", rep1.missing_streams())
        self.assertFalse(self.store.is_complete())

    def test_late_separate_streams_fill_in(self):
        # 触达先到、点击迟到
        self.store.ingest_batch(self._batch(
            Stream.IMPRESSIONS, 1, {sid: 100 for sid in self.ids}))
        self.assertFalse(self.store.is_complete())  # 点击全缺
        self.store.ingest_batch(self._batch(
            Stream.CLICKS, 1, {sid: 5 for sid in self.ids}))
        self.assertTrue(self.store.is_complete())

    def test_out_of_order_batch_rejected(self):
        with self.assertRaises(ReportError):
            self.store.ingest_batch(self._batch(Stream.IMPRESSIONS, 99, {self.ids[0]: 1}))

    def test_correction_appends_new_batch_and_keeps_history(self):
        self.store.ingest_batch(self._batch(Stream.IMPRESSIONS, 1, {self.ids[0]: 10}))
        corr = self.store.register_correction(
            Stream.IMPRESSIONS, {self.ids[0]: 12},
            corrects_batch_id="b-impressions-1", note="迟到更正")
        st = self.store.segment_report(self.ids[0]).impressions_state
        self.assertEqual(st.status, CORRECTED)
        self.assertEqual(st.latest_value, 12)
        self.assertEqual(len(st.history), 2)          # 旧批次保留
        self.assertTrue(corr.is_correction)


class SealingConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.store, _ = make_store(pairs=3)
        self.ids = self.store.segment_ids

    def test_pending_when_any_segment_incomplete(self):
        # 只报一半段
        self.store.ingest_batch(MetricBatch(
            "i1", Stream.IMPRESSIONS, 1, {sid: 100 for sid in self.ids[:3]}))
        snap = self.store.seal(note="首次封存")
        self.assertEqual(snap.status, PENDING)
        self.assertEqual(len(snap.incomplete_segments), 6)
        with self.assertRaises(ReportError):
            self.store.snapshot_metrics(snap)

    def test_sealed_after_complete_and_same_batch_consistency(self):
        self.store.ingest_batch(MetricBatch(
            "i1", Stream.IMPRESSIONS, 1, {sid: 100 for sid in self.ids}))
        self.store.ingest_batch(MetricBatch(
            "c1", Stream.CLICKS, 1, {sid: 10 for sid in self.ids}))
        snap = self.store.seal()
        self.assertEqual(snap.status, SEALED)
        metrics = self.store.snapshot_metrics(snap)
        # 分子分母都来自该封存快照
        self.assertTrue(all(m["impressions"] == 100 and m["clicks"] == 10
                            for m in metrics.values()))

    def test_concurrent_backfill_and_seal_no_half_batch(self):
        errors = []

        def backfill():
            try:
                for i, sid in enumerate(self.ids, start=1):
                    self.store.ingest_batch(MetricBatch(
                        f"i{i}", Stream.IMPRESSIONS, i, {sid: 50}))
            except ReportError as e:  # 编号竞争时以异常失败，绝不静默混用
                errors.append(e)

        def seal_loop():
            for _ in range(50):
                self.store.seal()

        t1 = threading.Thread(target=backfill)
        t2 = threading.Thread(target=seal_loop)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertFalse(errors)
        # 每个封存快照要么 PENDING，要么其所有段计数自洽且来源批次齐全
        for snap in self.store.snapshots():
            if snap.status == SEALED:
                for entry in snap.segments:
                    self.assertTrue(entry["complete"])
                    self.assertIsNotNone(entry["impressions_batch_id"])
                    self.assertIsNotNone(entry["clicks_batch_id"])

    def test_snapshot_immutable_when_later_data_arrives(self):
        self.store.ingest_batch(MetricBatch(
            "i1", Stream.IMPRESSIONS, 1, {sid: 100 for sid in self.ids}))
        self.store.ingest_batch(MetricBatch(
            "c1", Stream.CLICKS, 1, {sid: 10 for sid in self.ids}))
        snap1 = self.store.seal()
        fp1 = snap1.data_fingerprint
        # 封存后更正：旧快照指纹不变
        self.store.register_correction(
            Stream.CLICKS, {sid: 11 for sid in self.ids},
            corrects_batch_id="c1")
        self.assertEqual(snap1.data_fingerprint, fp1)
        snap2 = self.store.seal(note="更正后重封存")
        self.assertEqual(snap2.version, snap1.version + 1)
        self.assertNotEqual(snap2.data_fingerprint, fp1)


class SwapAnalysisTests(unittest.TestCase):
    def test_null_effect_pvalue_uniform_high_and_symmetric(self):
        # 每对两版 CTR 相同 -> 观测统计量为 0，双侧 p 必为 1
        store, design = make_store(seed=11, pairs=5)
        ids = store.segment_ids
        store.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, {s: 200 for s in ids}))
        store.ingest_batch(MetricBatch("c", Stream.CLICKS, 1, {s: 20 for s in ids}))
        snap = store.seal()
        res = paired_swap_test(design, snap, store.snapshot_metrics(snap))
        self.assertFalse(res.deferred)
        self.assertEqual(res.observed_stat, 0.0)
        self.assertEqual(res.p_value, 1.0)
        self.assertEqual(res.method, "exact-enumeration")
        self.assertEqual(res.draws, 2 ** 5)

    def test_strong_effect_significant_and_direction_b_minus_a(self):
        store, design = make_store(seed=5, pairs=6)
        ids = store.segment_ids
        impressions = {s.segment_id: 1000 for s in design.segments}
        # A 段点击率 5%，B 段 15%
        clicks = {
            s.segment_id: 150 if s.assigned_version == "B" else 50
            for s in design.segments
        }
        store.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, impressions))
        store.ingest_batch(MetricBatch("c", Stream.CLICKS, 1, clicks))
        snap = store.seal()
        res = paired_swap_test(design, snap, store.snapshot_metrics(snap))
        self.assertAlmostEqual(res.mean_diff, 0.10, places=10)
        self.assertLessEqual(res.p_value, 0.05)

    def test_zero_impression_pair_excluded_and_all_zero_defers(self):
        store, design = make_store(seed=5, pairs=3)
        ids = store.segment_ids
        imp = {s: 100 for s in ids}
        imp[ids[0]] = 0  # 第一段零触达 -> 其配对被剔除
        clk = {s: 5 for s in ids}
        store.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, imp))
        store.ingest_batch(MetricBatch("c", Stream.CLICKS, 1, clk))
        snap = store.seal()
        res = paired_swap_test(design, snap, store.snapshot_metrics(snap))
        self.assertEqual(res.usable_pairs, 2)
        self.assertEqual(len(res.excluded_pairs), 1)

        # 全部零触达 -> 暂缓
        store2, design2 = make_store(seed=5, pairs=2)
        ids2 = store2.segment_ids
        store2.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, {s: 0 for s in ids2}))
        store2.ingest_batch(MetricBatch("c", Stream.CLICKS, 1, {s: 0 for s in ids2}))
        snap2 = store2.seal()
        res2 = paired_swap_test(design2, snap2, store2.snapshot_metrics(snap2))
        self.assertTrue(res2.deferred)
        self.assertIsNone(res2.p_value)

    def test_monte_carlo_path_for_many_pairs(self):
        store, design = make_store(seed=9, pairs=25)  # 2^25 > 阈值
        ids = store.segment_ids
        store.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, {s: 100 for s in ids}))
        store.ingest_batch(MetricBatch("c", Stream.CLICKS, 1, {s: 10 for s in ids}))
        snap = store.seal()
        res = paired_swap_test(design, snap, store.snapshot_metrics(snap))
        self.assertEqual(res.method, "monte-carlo")
        self.assertEqual(res.draws, 10000)
        self.assertEqual(res.p_value, 1.0)  # 零效应，所有抽样都满足

    def test_recompute_keeps_assignment_and_all_prior_conclusions(self):
        store, design = make_store(seed=3, pairs=4)
        ids = store.segment_ids
        ledger = Ledger(design)
        store.ingest_batch(MetricBatch("i", Stream.IMPRESSIONS, 1, {s: 100 for s in ids}))
        click_batch = MetricBatch(
            "c1", Stream.CLICKS, 1,
            {s.segment_id: (12 if s.assigned_version == "B" else 8)
             for s in design.segments})
        store.ingest_batch(click_batch)
        snap1, res1, rec1 = seal_conclusion(store, "首轮")
        ledger.record_seal(snap1); ledger.record_conclusion(rec1)
        assignment_before = [(s.segment_id, s.assigned_version) for s in design.segments]

        # 迟到更正点击数据后重算
        store.register_correction(
            Stream.CLICKS,
            {s.segment_id: (20 if s.assigned_version == "B" else 5) for s in design.segments},
            corrects_batch_id=click_batch.batch_id, note="迟到补正")
        snap2, res2, rec2 = seal_conclusion(store, "更正重算")
        ledger.record_seal(snap2); ledger.record_conclusion(rec2)

        # 原分派不变
        self.assertEqual(
            assignment_before,
            [(s.segment_id, s.assigned_version) for s in design.segments])
        # 历次结论都在，且更正确实改变了检验结果（指纹与对内平均差）
        self.assertEqual(len(ledger.conclusions()), 2)
        self.assertNotEqual(rec1.data_fingerprint, rec2.data_fingerprint)
        self.assertNotEqual(rec1.result["mean_diff"], rec2.result["mean_diff"])
        self.assertNotEqual(rec1.result["observed_stat"], rec2.result["observed_stat"])
        self.assertEqual(rec2.version, 2)
        # 页面可渲染且包含原分派与两次结论
        page = ledger.render_html(store)
        self.assertIn("锁定分派", page)
        self.assertIn("v1 结论", page)
        self.assertIn("v2 结论", page)


if __name__ == "__main__":
    unittest.main(verbosity=2)
