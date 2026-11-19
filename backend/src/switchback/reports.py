"""迟到数据的补报：触达与点击分别上报，缺报与零值严格分开。

两条数据流相互独立，允许其中一条先到、另一条迟到补报：
- 一个 MetricBatch 只承载一种流（impressions 或 clicks），带单调批次号。
- 显式上报的 0 是合法零值（该段确实没有触达/点击）；
  根本没上报的段记为缺失（None），零值与缺失绝不等价。
- 更正（correction）以新批次形式提交，绝不原地改写旧批次；
  封存与补报/更正并发时由 store 保证只用同批、完整的数据。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class Stream(str, Enum):
    IMPRESSIONS = "impressions"
    CLICKS = "clicks"


# 段级上报状态
MISSING = "missing"     # 该流此段从未上报
REPORTED = "reported"   # 已上报（含显式零值）
CORRECTED = "corrected"  # 曾被后续更正批次覆盖


class ReportError(ValueError):
    """上报数据不合法。"""


@dataclass(frozen=True)
class MetricBatch:
    batch_id: str
    stream: Stream
    seq: int                                   # 单调递增批次号
    values: Dict[str, int]                    # segment_id -> 非负计数
    is_correction: bool = False
    corrects_batch_id: Optional[str] = None
    note: str = ""

    def __post_init__(self):
        if not isinstance(self.seq, int) or self.seq < 1:
            raise ReportError("批次号必须为 >=1 的整数")
        if not self.values:
            raise ReportError("空批次拒绝受理")
        for seg_id, v in self.values.items():
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ReportError(f"段 {seg_id} 计数必须为非负整数")
        if self.is_correction and not self.corrects_batch_id:
            raise ReportError("更正批次必须注明被更正的批次")


@dataclass
class SegmentStreamState:
    segment_id: str
    stream: Stream
    status: str = MISSING
    latest_value: Optional[int] = None
    latest_batch_id: Optional[str] = None
    history: List[MetricBatch] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return self.status in (REPORTED, CORRECTED)


@dataclass
class SegmentReport:
    """一个段在两条流上的合并视图。"""

    segment_id: str
    impressions_state: SegmentStreamState
    clicks_state: SegmentStreamState

    @property
    def is_complete(self) -> bool:
        # 两条流都已上报（显式零值算报齐；任一缺失即未报齐）
        return self.impressions_state.is_complete and self.clicks_state.is_complete

    @property
    def impressions(self) -> Optional[int]:
        return self.impressions_state.latest_value

    @property
    def clicks(self) -> Optional[int]:
        return self.clicks_state.latest_value

    def missing_streams(self) -> List[str]:
        out = []
        if not self.impressions_state.is_complete:
            out.append(Stream.IMPRESSIONS.value)
        if not self.clicks_state.is_complete:
            out.append(Stream.CLICKS.value)
        return out


def validate_batch_segments(batch: MetricBatch, known_segment_ids: List[str]) -> None:
    unknown = set(batch.values) - set(known_segment_ids)
    if unknown:
        raise ReportError(f"批次含未知时段: {sorted(unknown)}")


def accept_batch(
    states: Dict[str, Dict[Stream, SegmentStreamState]],
    batch: MetricBatch,
    known_segment_ids: List[str],
    expected_seq: int,
) -> None:
    """把一个补报批次写入段级状态（原地更新，供 store 在锁内调用）。

    expected_seq 由 store 按流单调发放；乱序/重复批次拒绝。
    缺报与零值的区分落在 SegmentStreamState：未出现的段保持 MISSING，
    显式 0 则落为 REPORTED 且 latest_value == 0。
    """
    validate_batch_segments(batch, known_segment_ids)
    if batch.seq != expected_seq:
        raise ReportError(
            f"批次号乱序：期望 {expected_seq}，收到 {batch.seq}（迟到数据请按受理顺序编号）"
        )

    for seg_id, value in batch.values.items():
        st = states[seg_id][batch.stream]
        prior_status = st.status
        st.history.append(batch)
        st.latest_value = value
        st.latest_batch_id = batch.batch_id
        # 该流此前已有值且本次是更正 -> CORRECTED；首次到达 -> REPORTED
        st.status = CORRECTED if prior_status != MISSING else REPORTED


def correct_metrics(
    batch_id: str,
    stream: Stream,
    seq: int,
    values: Dict[str, int],
    corrects_batch_id: str,
    note: str = "",
) -> MetricBatch:
    """构造一个更正批次。更正只以新批次追加，旧批次原样保留。"""
    return MetricBatch(
        batch_id=batch_id,
        stream=stream,
        seq=seq,
        values=dict(values),
        is_correction=True,
        corrects_batch_id=corrects_batch_id,
        note=note,
    )
