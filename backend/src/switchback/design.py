"""开投前：配对相邻等长时段、随机定先后、锁定分派与每段开头缓冲。

设计一旦 lock，分派即冻结：错播事件在 events.py 中仍按此处锁定的
assigned_version 归因，不允许事后改派。
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Sequence, Tuple

VERSION_A = "A"
VERSION_B = "B"
VERSIONS = (VERSION_A, VERSION_B)


@dataclass(frozen=True)
class Segment:
    """一个等长投放时段。

    start_offset/end_offset 为相对实验起点的等长整数时间单位（如分钟）。
    buffer_length 为该段开头的缓冲长度；缓冲区间为 [start, start+buffer)，
    与缓冲外区间 [start+buffer, end) 互斥。
    """

    segment_id: str
    pair_id: str
    pair_index: int          # 对内位置 0/1
    seg_index: int           # 全局序号，从 0 开始
    start_offset: int
    end_offset: int
    assigned_version: str
    buffer_length: int

    def __post_init__(self):
        if self.end_offset <= self.start_offset:
            raise ValueError("时段长度必须为正")
        if not (0 <= self.buffer_length < self.length):
            raise ValueError("缓冲长度必须满足 0 <= buffer < 段长")
        if self.assigned_version not in VERSIONS:
            raise ValueError("分派版本只能是 A 或 B")

    @property
    def length(self) -> int:
        return self.end_offset - self.start_offset

    @property
    def buffer_end_offset(self) -> int:
        return self.start_offset + self.buffer_length

    def in_buffer(self, event_offset: int) -> bool:
        """事件时间是否落在该段开头缓冲内（左闭右开）。"""
        return self.start_offset <= event_offset < self.buffer_end_offset

    def contains(self, event_offset: int) -> bool:
        return self.start_offset <= event_offset < self.end_offset


@dataclass
class ExperimentDesign:
    experiment_id: str
    segment_length: int
    segment_count: int
    buffer_length: int
    versions: Tuple[str, str] = VERSIONS
    segments: List[Segment] = field(default_factory=list)
    locked: bool = False
    rng_seed: Optional[int] = None
    lock_checksum: Optional[str] = None

    def pair_ids(self) -> List[str]:
        seen: List[str] = []
        for s in self.segments:
            if s.pair_id not in seen:
                seen.append(s.pair_id)
        return seen

    def pairs(self) -> List[Tuple[str, Tuple[Segment, Segment]]]:
        grouped = {}
        for s in self.segments:
            grouped.setdefault(s.pair_id, []).append(s)
        out = []
        for pid in self.pair_ids():
            members = sorted(grouped[pid], key=lambda x: x.pair_index)
            if len(members) != 2:
                raise ValueError(f"配对 {pid} 不是两个等长时段")
            out.append((pid, (members[0], members[1])))
        return out

    def segment(self, segment_id: str) -> Segment:
        for s in self.segments:
            if s.segment_id == segment_id:
                return s
        raise KeyError(f"时段不存在: {segment_id}")

    def locate(self, event_offset: int) -> Optional[Segment]:
        """按时间定位事件所属时段；越界返回 None。"""
        for s in self.segments:
            if s.contains(event_offset):
                return s
        return None


def build_design(
    experiment_id: str,
    segment_count: int,
    segment_length: int,
    buffer_length: int,
    rng_seed: Optional[int] = None,
) -> ExperimentDesign:
    """把实验窗口切成相邻等长时段并两两配对；对内随机决定 A/B 先后。

    - 时段数必须为偶数，否则无法全部两两配对（奇数尾段不得入对、不参与结论）。
    - 相邻两段为一对（(0,1),(2,3),...），保证对内等长且时间相邻。
    - 每对独立抛一枚硬币决定先后，随机源可由 rng_seed 复现。
    - 缓冲在每段开头，长度对所有段一致并在投放前固定。
    """
    if segment_count <= 0 or segment_count % 2 != 0:
        raise ValueError("时段数必须为正偶数，才能全部配对为相邻等长时段")
    if segment_length <= 0:
        raise ValueError("时段长度必须为正")
    if not (0 <= buffer_length < segment_length):
        raise ValueError("缓冲长度必须满足 0 <= buffer < 段长")

    rng = random.Random(rng_seed)
    segments: List[Segment] = []
    pair_total = segment_count // 2
    for p in range(pair_total):
        pair_id = f"pair-{p + 1:03d}"
        # 随机定先后：一半对 A 在前，一半对 B 在前
        first = VERSION_A if rng.random() < 0.5 else VERSION_B
        second = VERSION_B if first == VERSION_A else VERSION_A
        for pair_index, version in enumerate((first, second)):
            seg_index = p * 2 + pair_index
            segments.append(
                Segment(
                    segment_id=f"seg-{seg_index + 1:03d}",
                    pair_id=pair_id,
                    pair_index=pair_index,
                    seg_index=seg_index,
                    start_offset=seg_index * segment_length,
                    end_offset=(seg_index + 1) * segment_length,
                    assigned_version=version,
                    buffer_length=buffer_length,
                )
            )
    return ExperimentDesign(
        experiment_id=experiment_id,
        segment_length=segment_length,
        segment_count=segment_count,
        buffer_length=buffer_length,
        segments=segments,
        rng_seed=rng_seed,
    )


def _canonical_payload(design: ExperimentDesign) -> bytes:
    payload = {
        "experiment_id": design.experiment_id,
        "segment_length": design.segment_length,
        "segment_count": design.segment_count,
        "buffer_length": design.buffer_length,
        "segments": [asdict(s) for s in design.segments],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def lock_design(design: ExperimentDesign) -> ExperimentDesign:
    """开投前锁定：固化分派与缓冲，生成校验和。锁定后不可再改。"""
    if design.locked:
        raise ValueError("设计已锁定，分派不可重复锁定或修改")
    design.lock_checksum = hashlib.sha256(_canonical_payload(design)).hexdigest()
    design.locked = True
    return design


def design_checksum(design: ExperimentDesign) -> str:
    return hashlib.sha256(_canonical_payload(design)).hexdigest()


def assert_locked(design: ExperimentDesign) -> None:
    if not design.locked:
        raise ValueError("设计尚未锁定：必须在开投前锁定分派与缓冲")


def verify_lock(design: ExperimentDesign) -> bool:
    """校验锁定内容未被篡改（重新计算 checksum 比对）。"""
    if not design.locked or not design.lock_checksum:
        return False
    return design_lock_checksum(design) == design.lock_checksum


def design_lock_checksum(design: ExperimentDesign) -> str:
    return hashlib.sha256(_canonical_payload(design)).hexdigest()


def swap_order_summary(design: ExperimentDesign) -> Sequence[Tuple[str, str, str]]:
    """返回 (pair_id, 第一段分派, 第二段分派)，便于页面展示对内互换。"""
    return [
        (pid, first.assigned_version, second.assigned_version)
        for pid, (first, second) in design.pairs()
    ]
