"""报齐后的对内互换（paired randomization）检验与结论留痕。

设计上每一对相邻等长时段内 A/B 先后是随机抛硬币决定的，因此对内两版
点击率之差 d_p = CTR(B) - CTR(A) 在“无真实差异”原假设下，符号可正可负、
等概率互换。检验统计量取对内差之和 S = Σ d_p，其随机化分布通过枚举
每对符号翻转的 2^P 种等可能配置精确得到（对数太多时用固定 seed 蒙特卡洛）。

要点：
- 只接受 SEALED 快照；数据来自 store.snapshot_metrics，保证同批、完整。
- 某段触达为 0 时点击率无定义，该对标记 unusable 并剔除、单独留痕；
  没有任何可用对时暂缓结论，而不是强行给 p 值。
- 更正后可用新快照重新封存、重新检验；结论以新版本追加，历史结论保留。
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.switchback.design import ExperimentDesign
from src.switchback.store import SEALED, SealedSnapshot, ExperimentStore
from src.switchback.reports import ReportError

MONTE_CARLO_THRESHOLD = 20          # 2^20 ≈ 104 万，超过即转蒙特卡洛
MONTE_CARLO_DRAWS = 10000
FIXED_SEED = 20261119


@dataclass
class PairStat:
    pair_id: str
    seg_a_id: str
    seg_b_id: str
    impressions_a: int
    clicks_a: int
    impressions_b: int
    clicks_b: int
    ctr_a: Optional[float]
    ctr_b: Optional[float]
    diff: Optional[float]           # CTR(B) - CTR(A)，无定义时为 None
    usable: bool
    reason: str = ""


@dataclass
class SwapResult:
    seal_id: str
    version: int
    usable_pairs: int
    excluded_pairs: List[Dict[str, str]]
    observed_stat: Optional[float]
    mean_diff: Optional[float]
    p_value: Optional[float]
    method: str
    draws: int
    pair_stats: List[Dict[str, Any]]
    deferred: bool
    defer_reason: str = ""
    interpretation: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "seal_id": self.seal_id,
            "version": self.version,
            "usable_pairs": self.usable_pairs,
            "excluded_pairs": self.excluded_pairs,
            "observed_stat": self.observed_stat,
            "mean_diff": self.mean_diff,
            "p_value": self.p_value,
            "method": self.method,
            "draws": self.draws,
            "deferred": self.deferred,
            "defer_reason": self.defer_reason,
            "interpretation": self.interpretation,
            "pair_stats": self.pair_stats,
        }


def _ctr(clicks: int, impressions: int) -> Optional[float]:
    if impressions == 0:
        return None
    if not (0 <= clicks <= impressions):
        raise ReportError(f"点击数 {clicks} 不能超过触达数 {impressions}")
    return clicks / impressions


def build_pair_stats(
    design: ExperimentDesign, metrics: Dict[str, Dict[str, int]]
) -> List[PairStat]:
    """按锁定分派把每对内两版对齐，计算点击率与对内差。"""
    stats: List[PairStat] = []
    for pair_id, (first, second) in design.pairs():
        # 按“版本”对齐，而不是按段顺序：first/second 的先后本身是随机的
        seg_by_version = {
            first.assigned_version: first,
            second.assigned_version: second,
        }
        seg_a = seg_by_version["A"]
        seg_b = seg_by_version["B"]
        ma, mb = metrics[seg_a.segment_id], metrics[seg_b.segment_id]
        ctr_a = _ctr(ma["clicks"], ma["impressions"])
        ctr_b = _ctr(mb["clicks"], mb["impressions"])

        usable = ctr_a is not None and ctr_b is not None
        reason = ""
        diff: Optional[float] = None
        if not usable:
            zero_segments = [
                sid
                for sid, m in ((seg_a.segment_id, ma), (seg_b.segment_id, mb))
                if m["impressions"] == 0
            ]
            reason = f"触达为0、点击率无定义: {zero_segments}"
        else:
            diff = ctr_b - ctr_a  # type: ignore[operator]

        stats.append(
            PairStat(
                pair_id=pair_id,
                seg_a_id=seg_a.segment_id,
                seg_b_id=seg_b.segment_id,
                impressions_a=ma["impressions"],
                clicks_a=ma["clicks"],
                impressions_b=mb["impressions"],
                clicks_b=mb["clicks"],
                ctr_a=ctr_a,
                ctr_b=ctr_b,
                diff=diff,
                usable=usable,
                reason=reason,
            )
        )
    return stats


def _randomization_p_value(diffs: List[float]) -> Tuple[float, str, int]:
    """对内符号互换的随机化检验，双侧 p 值。

    原假设下每对差值等概率取 +d_p 或 -d_p，统计量 S = Σ ±d_p。
    双侧 p = P(|S| >= |S_obs|)，含等号（离散检验的标准口径）。
    """
    observed = math.fsum(diffs)
    p = len(diffs)
    if p <= MONTE_CARLO_THRESHOLD:
        method = "exact-enumeration"
        configs = 1 << p
        ge = 0
        # Gray-code 之外的直观枚举：2^p 个符号配置
        for mask in range(configs):
            total = 0.0
            for i, d in enumerate(diffs):
                total += d if (mask >> i) & 1 else -d
            if abs(total) >= abs(observed) - 1e-15:
                ge += 1
        return ge / configs, method, configs

    # 对数太多：固定 seed 蒙特卡洛，保证可复现
    method = "monte-carlo"
    rng = random.Random(FIXED_SEED)
    ge = 0
    for _ in range(MONTE_CARLO_DRAWS):
        total = math.fsum(d if rng.random() < 0.5 else -d for d in diffs)
        if abs(total) >= abs(observed) - 1e-15:
            ge += 1
    # (x+1)/(n+1) 收缩，避免蒙特卡洛给出确定性的 0
    p_value = (ge + 1) / (MONTE_CARLO_DRAWS + 1)
    return p_value, method, MONTE_CARLO_DRAWS


def paired_swap_test(
    design: ExperimentDesign, snapshot: SealedSnapshot, metrics: Dict[str, Dict[str, int]]
) -> SwapResult:
    """对一个已封存快照执行对内互换检验。"""
    if snapshot.status != SEALED:
        return SwapResult(
            seal_id=snapshot.seal_id,
            version=snapshot.version,
            usable_pairs=0,
            excluded_pairs=[],
            observed_stat=None,
            mean_diff=None,
            p_value=None,
            method="none",
            draws=0,
            pair_stats=[],
            deferred=True,
            defer_reason=f"数据未报齐，缺失段: {snapshot.incomplete_segments}",
            interpretation="任一段未报齐，按规则暂缓结论，不计算点击率差。",
        )

    pair_stats = build_pair_stats(design, metrics)
    usable = [ps for ps in pair_stats if ps.usable]
    excluded = [
        {"pair_id": ps.pair_id, "reason": ps.reason}
        for ps in pair_stats
        if not ps.usable
    ]

    if not usable:
        return SwapResult(
            seal_id=snapshot.seal_id,
            version=snapshot.version,
            usable_pairs=0,
            excluded_pairs=excluded,
            observed_stat=None,
            mean_diff=None,
            p_value=None,
            method="none",
            draws=0,
            pair_stats=[ps.__dict__.copy() for ps in pair_stats],
            deferred=True,
            defer_reason="所有配对均含零触达段，点击率无定义",
            interpretation="无可用配对，暂缓结论。",
        )

    diffs = [ps.diff for ps in usable]  # type: ignore[arg-type]
    observed = math.fsum(diffs)  # type: ignore[arg-type]
    mean_diff = observed / len(usable)
    p_value, method, draws = _randomization_p_value(diffs)  # type: ignore[arg-type]

    direction = "B 高于 A" if mean_diff > 0 else ("A 高于 B" if mean_diff < 0 else "两版持平")
    interpretation = (
        f"基于 {len(usable)} 个相邻等长配对的对内互换随机化检验（{method}），"
        f"平均对内点击率差 CTR(B)-CTR(A)={mean_diff:.6f}（{direction}），"
        f"双侧 p={p_value:.4f}。"
        + (f" 另有 {len(excluded)} 对因零触达剔除。" if excluded else "")
    )
    return SwapResult(
        seal_id=snapshot.seal_id,
        version=snapshot.version,
        usable_pairs=len(usable),
        excluded_pairs=excluded,
        observed_stat=observed,
        mean_diff=mean_diff,
        p_value=p_value,
        method=method,
        draws=draws,
        pair_stats=[ps.__dict__.copy() for ps in pair_stats],
        deferred=False,
        interpretation=interpretation,
    )


@dataclass
class ConclusionRecord:
    """一次结论：绑定封存快照与数据指纹，追加保存、永不覆盖。"""

    conclusion_id: str
    seal_id: str
    version: int
    data_fingerprint: str
    result: Dict[str, Any]
    created_at: str


def seal_conclusion(store: ExperimentStore, note: str = "") -> Tuple[SealedSnapshot, SwapResult, Optional[ConclusionRecord]]:
    """封存当前数据并检验；结论按版本追加，历次结论与原分派都保留。

    返回 (快照, 检验结果, 结论记录)。暂缓时结论记录为 None，但暂缓快照仍留痕。
    """
    import uuid
    from datetime import datetime, timezone

    snapshot = store.seal(note=note)
    if snapshot.status != SEALED:
        result = paired_swap_test(store.design, snapshot, {})
        return snapshot, result, None

    metrics = store.snapshot_metrics(snapshot)
    result = paired_swap_test(store.design, snapshot, metrics)
    record = ConclusionRecord(
        conclusion_id=f"conclusion-{uuid.uuid4().hex[:12]}",
        seal_id=snapshot.seal_id,
        version=snapshot.version,
        data_fingerprint=snapshot.data_fingerprint,
        result=result.as_dict(),
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    return snapshot, result, record
