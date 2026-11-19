"""同入口轮流投放（switchback）配对分析引擎。

规则对应（与本目录各模块一一对应）：
1. 配对相邻等长时段、随机定先后、开投前锁定分派与每段开头缓冲 —— design.py
2. 错播仍归原分派；缓冲内触达/点击不计 —— events.py
3. 迟到的触达、点击分别补报；缺报与零值分开 —— reports.py
4. 补报与封存并发：分子分母只取同批数据，任一段未报齐即暂缓结论 —— store.py
5. 报齐后按对内互换检验点击率差；更正可重算；原分派与历次结论保留 —— analysis.py
"""
from src.switchback.design import ExperimentDesign, Segment, build_design, lock_design
from src.switchback.events import AttributionError, ingest_events
from src.switchback.reports import ReportError, accept_batch, correct_metrics
from src.switchback.store import ExperimentStore, PENDING, SEALED
from src.switchback.analysis import paired_swap_test, seal_conclusion, SwapResult

__all__ = [
    "ExperimentDesign",
    "Segment",
    "build_design",
    "lock_design",
    "AttributionError",
    "ingest_events",
    "ReportError",
    "accept_batch",
    "correct_metrics",
    "ExperimentStore",
    "PENDING",
    "SEALED",
    "paired_swap_test",
    "seal_conclusion",
    "SwapResult",
]
