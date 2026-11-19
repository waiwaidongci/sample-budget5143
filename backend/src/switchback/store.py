"""实验数据仓库：补报与封存并发控制、同批一致性、结论留痕。

并发口径（对应规则第 4 条）：
- 所有状态变更在同一把可重入锁内完成；补报/更正与封存互斥。
- 封存只对“调用时刻已提交的完整批次”取快照：封存进行中到达的补报
  要么在封存前提交（被本次封存纳入），要么在封存后提交（留给下一轮），
  不会出现半个批次进入分子/分母的情况。
- 封存快照为每段记录 impressions/clicks 各自来源批次号，分子分母只取
  同批、完整的数据；任一段触达或点击未报齐，整轮结论暂缓。
- 更正以新批次追加；重算封存生成新版本结论，原分派与历次结论永不覆盖。
"""
from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.switchback.design import ExperimentDesign, assert_locked, verify_lock
from src.switchback.reports import (
    CORRECTED,
    MISSING,
    MetricBatch,
    ReportError,
    SegmentReport,
    SegmentStreamState,
    Stream,
    accept_batch,
)

PENDING = "pending"     # 未报齐，结论暂缓
SEALED = "sealed"       # 已封存，结论冻结在该快照上


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SealedSnapshot:
    """一次封存所冻结的完整数据视图。"""

    seal_id: str
    version: int
    created_at: str
    status: str                          # SEALED 或 PENDING（暂缓也是一次留痕）
    segments: List[Dict[str, Any]]      # 逐段：值 + 来源批次 + 完整标记
    incomplete_segments: List[str]
    data_fingerprint: str
    triggered_by_batch: Optional[str] = None
    note: str = ""


@dataclass
class ExperimentStore:
    design: ExperimentDesign
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _states: Dict[str, Dict[Stream, SegmentStreamState]] = field(default_factory=dict)
    _next_seq: Dict[Stream, int] = field(default_factory=dict)
    _batches: List[MetricBatch] = field(default_factory=list)
    _snapshots: List[SealedSnapshot] = field(default_factory=list)

    def __post_init__(self):
        assert_locked(self.design)
        if not verify_lock(self.design):
            raise ValueError("设计锁定校验和不一致，拒绝基于被篡改的分派建仓")
        self._states = {
            s.segment_id: {
                Stream.IMPRESSIONS: SegmentStreamState(s.segment_id, Stream.IMPRESSIONS),
                Stream.CLICKS: SegmentStreamState(s.segment_id, Stream.CLICKS),
            }
            for s in self.segments
        }
        self._next_seq = {Stream.IMPRESSIONS: 1, Stream.CLICKS: 1}

    # ---- 基本视图 -------------------------------------------------------
    @property
    def segments(self):
        return self.design.segments

    @property
    def segment_ids(self) -> List[str]:
        return [s.segment_id for s in self.design.segments]

    def segment_report(self, segment_id: str) -> SegmentReport:
        pair = self._states[segment_id]
        return SegmentReport(segment_id, pair[Stream.IMPRESSIONS], pair[Stream.CLICKS])

    def all_segment_reports(self) -> List[SegmentReport]:
        return [self.segment_report(sid) for sid in self.segment_ids]

    def batches(self) -> List[MetricBatch]:
        with self._lock:
            return list(self._batches)

    def snapshots(self) -> List[SealedSnapshot]:
        with self._lock:
            return list(self._snapshots)

    def latest_snapshot(self) -> Optional[SealedSnapshot]:
        with self._lock:
            return self._snapshots[-1] if self._snapshots else None

    # ---- 补报 / 更正（与封存互斥）---------------------------------------
    def ingest_batch(self, batch: MetricBatch) -> Dict[str, Any]:
        """受理一个补报批次，返回受理回执。"""
        with self._lock:
            expected = self._next_seq[batch.stream]
            accept_batch(self._states, batch, self.segment_ids, expected)
            self._next_seq[batch.stream] = expected + 1
            self._batches.append(batch)
            return {
                "accepted": True,
                "batch_id": batch.batch_id,
                "stream": batch.stream.value,
                "seq": batch.seq,
                "is_correction": batch.is_correction,
                "accepted_at": _utc_now_iso(),
            }

    def register_correction(
        self,
        stream: Stream,
        values: Dict[str, int],
        corrects_batch_id: str,
        note: str = "",
    ) -> MetricBatch:
        """在锁内构造并受理一个更正批次（编号由仓库单调发放）。"""
        with self._lock:
            if not any(b.batch_id == corrects_batch_id for b in self._batches):
                raise ReportError(f"被更正批次不存在: {corrects_batch_id}")
            target = next(b for b in self._batches if b.batch_id == corrects_batch_id)
            if target.stream != stream:
                raise ReportError("更正批次与被更正批次不属于同一条数据流")
            seq = self._next_seq[stream]
            batch = MetricBatch(
                batch_id=f"batch-{uuid.uuid4().hex[:12]}",
                stream=stream,
                seq=seq,
                values=dict(values),
                is_correction=True,
                corrects_batch_id=corrects_batch_id,
                note=note,
            )
            accept_batch(self._states, batch, self.segment_ids, seq)
            self._next_seq[stream] = seq + 1
            self._batches.append(batch)
            return batch

    # ---- 完整性 ---------------------------------------------------------
    def incomplete_segments(self) -> List[str]:
        """缺报判定：显式零值算报齐；任一流缺失即未报齐。"""
        return [
            r.segment_id
            for r in self.all_segment_reports()
            if not r.is_complete
        ]

    def is_complete(self) -> bool:
        return not self.incomplete_segments()

    # ---- 封存 -----------------------------------------------------------
    def seal(self, note: str = "", triggered_by_batch: Optional[str] = None) -> SealedSnapshot:
        """封存当前已提交数据。与任何补报/更正互斥，快照不可变。

        任一段未报齐 -> 生成 status=PENDING 的留痕并暂缓结论（不产出指标）；
        全部报齐 -> 生成 status=SEALED 的可分析快照。
        """
        with self._lock:
            seg_payload: List[Dict[str, Any]] = []
            incomplete: List[str] = []
            for sid in self.segment_ids:
                report = self.segment_report(sid)
                entry: Dict[str, Any] = {
                    "segment_id": sid,
                    "complete": report.is_complete,
                    "missing_streams": report.missing_streams(),
                    "impressions": report.impressions,
                    "clicks": report.clicks,
                    "impressions_batch_id": report.impressions_state.latest_batch_id,
                    "clicks_batch_id": report.clicks_state.latest_batch_id,
                    "impressions_status": report.impressions_state.status,
                    "clicks_status": report.clicks_state.status,
                }
                seg_payload.append(entry)
                if not report.is_complete:
                    incomplete.append(sid)

            status = PENDING if incomplete else SEALED
            fingerprint = self._fingerprint(seg_payload)
            snapshot = SealedSnapshot(
                seal_id=f"seal-{uuid.uuid4().hex[:12]}",
                version=len(self._snapshots) + 1,
                created_at=_utc_now_iso(),
                status=status,
                segments=seg_payload,
                incomplete_segments=incomplete,
                data_fingerprint=fingerprint,
                triggered_by_batch=triggered_by_batch,
                note=note,
            )
            # 历次封存全部保留，绝不覆盖
            self._snapshots.append(snapshot)
            return snapshot

    @staticmethod
    def _fingerprint(seg_payload: List[Dict[str, Any]]) -> str:
        import hashlib

        canonical = json.dumps(seg_payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # ---- 同批一致性校验 -------------------------------------------------
    def snapshot_metrics(self, snapshot: SealedSnapshot) -> Dict[str, Dict[str, int]]:
        """从封存快照取逐段计数；只在同批、完整数据上成立。

        返回 {segment_id: {"impressions": n, "clicks": k}}。
        PENDING 快照直接拒绝，防止用未报齐数据计算分子/分母。
        """
        if snapshot.status != SEALED:
            raise ReportError(
                f"快照 {snapshot.seal_id} 为暂缓状态，缺失段: "
                f"{snapshot.incomplete_segments}，不得产出指标"
            )
        out: Dict[str, Dict[str, int]] = {}
        for entry in snapshot.segments:
            if not entry["complete"]:
                # 理论上 SEALED 不会进入此分支，双保险
                raise ReportError(f"段 {entry['segment_id']} 数据未报齐")
            out[entry["segment_id"]] = {
                "impressions": int(entry["impressions"]),
                "clicks": int(entry["clicks"]),
            }
        return out
