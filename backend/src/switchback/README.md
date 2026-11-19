# 同入口轮流投放 · 配对互换分析引擎（switchback）

纯 Python 标准库实现（无需 NumPy/数据库），把“同一入口轮流投两版、流量随时段起伏”的
实验后分析规则固化为可测试、可复现、可留痕的模块。

## 规则与模块对应

| 规则原文要点 | 落地位置 | 关键行为 |
|---|---|---|
| 开投前配对相邻等长时段 | `design.build_design` | 窗口切成等长段，相邻两段 `(0,1)(2,3)…` 配对；奇数段拒绝 |
| 随机定先后 | `design.build_design` | 每对独立抛硬币决定 A/B 谁先，`rng_seed` 可复现 |
| 锁定分派和每段开头缓冲 | `design.lock_design` | 生成 SHA256 校验和；建仓时复验，篡改即拒绝 |
| 错播仍归原分派 | `events.ingest_events` | `served_version ≠ assigned_version` 时计数仍进原分派段，错播单独留痕 |
| 缓冲内触达和点击不计 | `events.ingest_events` | 事件时间落在 `[start, start+buffer)` 直接丢弃，缓冲/有效计数分开 |
| 迟到的触达和点击分别补报 | `reports.Stream` + `store.ingest_batch` | 触达流、点击流独立批次、独立单调批次号，可先后到达 |
| 缺报与零值分开 | `reports.SegmentStreamState` | 显式 `0` → `reported`（算报齐）；未上报 → `missing`，二者绝不相等 |
| 补报与封存并发，分子分母只取同批数据 | `store.ExperimentStore`（RLock 互斥） | 封存对“已提交完整批次”取不可变快照，记录每段两条流的来源批次号 |
| 任一段未报齐就暂缓结论 | `store.seal` | 生成 `pending` 快照并留痕；`snapshot_metrics` 对 pending 快照直接拒绝 |
| 报齐后按对内互换检验点击率差 | `analysis.paired_swap_test` | 对内差 `CTR(B)-CTR(A)`，符号翻转随机化检验：≤20 对精确枚举 2^P，多于则固定 seed 蒙特卡洛 |
| 更正后可重算 | `store.register_correction` + 再次 `seal_conclusion` | 更正只追加新批次、新版本快照/结论；旧批次与旧指纹原样保留 |
| 页面保留原分派与历次结论 | `ledger.Ledger.render_html` | 自包含 HTML：锁定分派表、逐段缺报/零值状态、历次封存与结论时间线 |

## 口径细节

- **随机化检验**：原假设为两版无差异，每对内 A/B 互换等概率，统计量
  `S = Σ (CTR_B − CTR_A)`，双侧 p = `P(|S| ≥ |S_obs|)`（含等号）。
  配对数 ≤ 20 枚举全部 2^P 种符号配置；否则用固定 seed `20261119` 抽 10000 次，
  p 采用 `(x+1)/(n+1)` 收缩避免确定性零值。
- **零触达处理**：某段触达为 0 时点击率无定义，该对标记 `unusable` 剔除并留痕；
  所有对都不可用时返回 `deferred=True` 暂缓结论，而不是输出 p 值。
- **同批一致性**：封存快照逐段记录 `impressions_batch_id`/`clicks_batch_id`，
  指标只能由 `store.snapshot_metrics(snapshot)` 从快照取数，杜绝把封存后到达的
  补报混进旧结论的分子或分母。
- **并发模型**：进程内 `threading.RLock` 保证补报/更正与封存串行化；
  压测（2 写线程 + 2 封存线程、400 次封存）验证无半批次、无异常、快照全自洽。
  多进程部署时需把锁替换为数据库行锁/事务，快照表结构已按此口径设计字段。

## 运行

```bash
# 测试（20 个用例，零依赖）
python3 -I /workspace/backend/tests/test_switchback.py

# 端到端演示，页面输出到 /tmp/switchback_demo_page.html
python3 -I /workspace/backend/scripts/demo_switchback.py
```

## 最小用法

```python
from src.switchback.design import build_design, lock_design
from src.switchback.reports import MetricBatch, Stream
from src.switchback.store import ExperimentStore
from src.switchback.analysis import seal_conclusion

design = lock_design(build_design("exp-1", segment_count=4,
                                  segment_length=60, buffer_length=10, rng_seed=0))
store = ExperimentStore(design)
ids = store.segment_ids
store.ingest_batch(MetricBatch("i1", Stream.IMPRESSIONS, 1, {s: 1000 for s in ids}))
store.ingest_batch(MetricBatch("c1", Stream.CLICKS, 1,
    {s.segment_id: 140 if s.assigned_version == "B" else 80 for s in design.segments}))
snapshot, result, conclusion = seal_conclusion(store)
print(result.interpretation)
```
