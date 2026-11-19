"""事件归集：错播仍归原分派；缓冲内触达与点击一律不计。

本模块处理最细粒度的原始事件流，输出可用于上报的逐段计数。
计数口径（投放后任何阶段都不得改变）：
- 事件按发生时间落入唯一时段；段开头缓冲 [start, start+buffer) 内的
  触达/点击直接丢弃，不计入任何分子或分母。
- 实际展示版本 served_version 与锁定分派 assigned_version 不一致即错播，
  事件仍计入 assigned_version（原分派），并在 misdelivered 中留痕。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from src.switchback.design import (
    ExperimentDesign,
    Segment,
    VERSION_A,
    VERSION_B,
    assert_locked,
)

IMPRESSION = "impression"
CLICK = "click"
EVENT_TYPES = (IMPRESSION, CLICK)


class AttributionError(ValueError):
    """事件无法在锁定设计下合法归集。"""


@dataclass
class RawEvent:
    event_id: str
    event_offset: int          # 相对实验起点的时间单位
    event_type: str            # impression / click
    served_version: str        # 实际展示的版本（可能错播）

    def __post_init__(self):
        if self.event_type not in EVENT_TYPES:
            raise AttributionError(f"未知事件类型: {self.event_type}")
        if self.served_version not in (VERSION_A, VERSION_B):
            raise AttributionError(f"未知展示版本: {self.served_version}")


@dataclass
class SegmentTally:
    segment_id: str
    assigned_version: str
    impressions: int = 0
    clicks: int = 0
    buffered_impressions: int = 0
    buffered_clicks: int = 0
    misdelivered_impressions: int = 0
    misdelivered_clicks: int = 0
    dropped_event_ids: List[str] = field(default_factory=list)
    misdelivered_event_ids: List[str] = field(default_factory=list)

    def as_counts(self) -> Dict[str, int]:
        """供上报使用的计数：仅含缓冲外、按原分派归因的口径。"""
        return {
            "segment_id": self.segment_id,
            "impressions": self.impressions,
            "clicks": self.clicks,
        }


@dataclass
class IngestionResult:
    tallies: Dict[str, SegmentTally]
    out_of_range_event_ids: List[str] = field(default_factory=list)

    def counts_payload(self) -> List[Dict[str, int]]:
        return [self.tallies[sid].as_counts() for sid in sorted(self.tallies)]


def _empty_tallies(design: ExperimentDesign) -> Dict[str, SegmentTally]:
    return {
        s.segment_id: SegmentTally(s.segment_id, s.assigned_version)
        for s in design.segments
    }


def ingest_events(design: ExperimentDesign, events: Iterable[RawEvent]) -> IngestionResult:
    """按锁定分派归集一批原始事件。

    归集前强制要求设计已锁定，确保分派口径先于投放固定。
    同一 event_id 重复出现会被拒绝，避免重复计数。
    """
    assert_locked(design)
    tallies = _empty_tallies(design)
    out_of_range: List[str] = []
    seen: set = set()

    for ev in events:
        if ev.event_id in seen:
            raise AttributionError(f"事件重复，拒绝重复计数: {ev.event_id}")
        seen.add(ev.event_id)

        seg: Optional[Segment] = design.locate(ev.event_offset)
        if seg is None:
            out_of_range.append(ev.event_id)
            continue
        tally = tallies[seg.segment_id]

        # 缓冲内：触达和点击都不计
        if seg.in_buffer(ev.event_offset):
            if ev.event_type == IMPRESSION:
                tally.buffered_impressions += 1
            else:
                tally.buffered_clicks += 1
            tally.dropped_event_ids.append(ev.event_id)
            continue

        is_misdelivered = ev.served_version != seg.assigned_version
        if is_misdelivered:
            tally.misdelivered_event_ids.append(ev.event_id)
            if ev.event_type == IMPRESSION:
                tally.misdelivered_impressions += 1
            else:
                tally.misdelivered_clicks += 1

        # 错播仍归原分派：计入 assigned_version 的计数
        if ev.event_type == IMPRESSION:
            tally.impressions += 1
        else:
            tally.clicks += 1

    return IngestionResult(tallies=tallies, out_of_range_event_ids=out_of_range)
