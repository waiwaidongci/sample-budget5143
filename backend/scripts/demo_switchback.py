"""端到端演示：同入口轮流投两版的配对分析全流程。

运行：python3 -I /workspace/backend/scripts/demo_switchback.py
仅使用标准库；在 /tmp 下生成自包含 HTML 页面。
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.switchback.design import build_design, lock_design, swap_order_summary
from src.switchback.events import RawEvent, ingest_events, IMPRESSION, CLICK
from src.switchback.reports import MetricBatch, Stream
from src.switchback.store import ExperimentStore, PENDING
from src.switchback.analysis import seal_conclusion
from src.switchback.ledger import Ledger

OUT = "/tmp/switchback_demo_page.html"


def title(n):
    print("\n" + "=" * 72)
    print(n)
    print("=" * 72)


def main():
    # 1) 开投前：8 段（4 对）相邻等长 60 分钟，每段开头 10 分钟缓冲
    title("1. 开投前配对、随机定先后、锁定分派与缓冲")
    design = lock_design(
        build_design("榆灯·周刊入口-轮换实验",
                     segment_count=8, segment_length=60,
                     buffer_length=10, rng_seed=0)
    )
    print(f"锁定校验和: {design.lock_checksum[:24]}…")
    for pid, first_v, second_v in swap_order_summary(design):
        print(f"  {pid}: 第一段 {first_v} → 第二段 {second_v}（对内互换）")

    store = ExperimentStore(design)
    ledger = Ledger(design)

    # 2) 原始事件归集：含缓冲内事件与一次错播
    title("2. 事件归集：缓冲内不计；错播仍归原分派")
    seg_a = next(s for s in design.segments if s.assigned_version == "A")
    events = [
        RawEvent("evt-001", seg_a.start_offset + 3, IMPRESSION, "A"),   # 缓冲内
        RawEvent("evt-002", seg_a.start_offset + 4, CLICK, "A"),        # 缓冲内
        RawEvent("evt-003", seg_a.buffer_end_offset + 1, IMPRESSION, "A"),
        RawEvent("evt-004", seg_a.buffer_end_offset + 2, CLICK, "A"),
        RawEvent("evt-005", seg_a.buffer_end_offset + 3, IMPRESSION, "B"),  # 错播
        RawEvent("evt-006", seg_a.buffer_end_offset + 4, CLICK, "B"),       # 错播
    ]
    ingested = ingest_events(design, events)
    t = ingested.tallies[seg_a.segment_id]
    print(f"  段 {seg_a.segment_id}（原分派 A）:")
    print(f"    缓冲内丢弃 触达{t.buffered_impressions}/点击{t.buffered_clicks}")
    print(f"    错播 触达{t.misdelivered_impressions}/点击{t.misdelivered_clicks}，仍计入 A")
    print(f"    有效计数 触达{t.impressions} 点击{t.clicks}")

    # 3) 迟到补报：触达流先到（含显式零值段），点击流稍后到
    title("3. 两条流分别补报：显式零值与缺报分开")
    ids = store.segment_ids
    imp_values = {sid: 1000 for sid in ids}
    imp_values[ids[2]] = 0  # 该段显式上报零触达
    b_imp = MetricBatch("batch-imp-1", Stream.IMPRESSIONS, 1, imp_values,
                        note="触达流 T+1h 到达")
    store.ingest_batch(b_imp)
    rep_zero = store.segment_report(ids[2])
    rep_missing = store.segment_report(ids[3])
    print(f"  段 {ids[2]} 触达=显式零值（算报齐）: {rep_zero.impressions_state.status}")
    print(f"  段 {ids[3]} 点击=缺报（未报齐）: {rep_missing.missing_streams()}")

    # 4) 未报齐即封存 → 暂缓结论
    title("4. 封存时点击流未齐 → 暂缓，不出指标")
    snap0 = store.seal(note="点击流迟到，先封存一次留痕")
    print(f"  快照 {snap0.seal_id} 状态={snap0.status}，缺失段 {snap0.incomplete_segments}")
    ledger.record_seal(snap0)

    # 5) 点击流补报：B 段点击率高于 A 段
    title("5. 点击迟到补报齐后封存，对内互换检验")
    click_values = {
        s.segment_id: (140 if s.assigned_version == "B" else 80)
        for s in design.segments
    }
    b_clk = MetricBatch("batch-clk-1", Stream.CLICKS, 1, click_values,
                        note="点击流 T+6h 迟到补报")
    store.ingest_batch(b_clk)
    snap1, res1, rec1 = seal_conclusion(store, note="首次报齐")
    ledger.record_seal(snap1); ledger.record_conclusion(rec1)
    print(f"  快照状态={snap1.status}，可用配对={res1.usable_pairs}，方法={res1.method}")
    print(f"  {res1.interpretation}")
    for ps in res1.pair_stats:
        if ps["usable"]:
            print(f"    {ps['pair_id']}: CTR_A={ps['ctr_a']:.3f} CTR_B={ps['ctr_b']:.3f} "
                  f"对内差={ps['diff']:+.3f}")
        else:
            print(f"    {ps['pair_id']}: 剔除（{ps['reason']}）")

    # 6) 更正后重算：原分派与历次结论保留
    title("6. 迟到更正点击数据 → 重新封存与重算（历史不覆盖）")
    corrected_clicks = {
        s.segment_id: (170 if s.assigned_version == "B" else 70)
        for s in design.segments
    }
    corr = store.register_correction(
        Stream.CLICKS, corrected_clicks,
        corrects_batch_id=b_clk.batch_id, note="上游修正口径后补正")
    print(f"  更正批次 {corr.batch_id} 追加，旧批次 {b_clk.batch_id} 原样保留")
    snap2, res2, rec2 = seal_conclusion(store, note="更正后重算")
    ledger.record_seal(snap2); ledger.record_conclusion(rec2)
    print(f"  新结论版本 v{rec2.version}：{res2.interpretation}")
    print(f"  台账内结论数={len(ledger.conclusions())}（v1 与 v2 均保留）")

    # 7) 页面：原分派 + 逐段状态 + 历次结论
    title("7. 生成页面（保留原分派与历次结论）")
    page = ledger.render_html(store)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"  已写入 {OUT}（{os.path.getsize(OUT)} 字节）")
    print(f"  封存历史版本: {[s.version for s in store.snapshots()]} "
          f"状态: {[s.status for s in store.snapshots()]}")


if __name__ == "__main__":
    main()
