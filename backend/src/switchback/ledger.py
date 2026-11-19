"""不可变台账与页面渲染：原分派与历次结论永久保留。

- Ledger 只追加：每次封存（含暂缓）与每条结论都按版本保存，更正重算
  产生新版本，旧版本原样可查。
- render_html 输出自包含 HTML：顶部锁定分派（含对内先后与缓冲），
  中部逐段上报状态（缺报/零值区分），底部历次结论时间线。
"""
from __future__ import annotations

import html
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.switchback.analysis import ConclusionRecord, SwapResult
from src.switchback.design import ExperimentDesign
from src.switchback.store import PENDING, SEALED, ExperimentStore, SealedSnapshot


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Ledger:
    design: ExperimentDesign
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _entries: List[Dict[str, Any]] = field(default_factory=list)

    def record_seal(self, snapshot: SealedSnapshot) -> None:
        with self._lock:
            self._entries.append(
                {
                    "kind": "seal",
                    "at": _now(),
                    "seal_id": snapshot.seal_id,
                    "version": snapshot.version,
                    "status": snapshot.status,
                    "fingerprint": snapshot.data_fingerprint,
                    "incomplete_segments": list(snapshot.incomplete_segments),
                    "note": snapshot.note,
                }
            )

    def record_conclusion(self, record: ConclusionRecord) -> None:
        with self._lock:
            self._entries.append(
                {
                    "kind": "conclusion",
                    "at": _now(),
                    "conclusion_id": record.conclusion_id,
                    "seal_id": record.seal_id,
                    "version": record.version,
                    "fingerprint": record.data_fingerprint,
                    "result": record.result,
                }
            )

    def entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)

    def conclusions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [e for e in self._entries if e["kind"] == "conclusion"]

    # ---- HTML 页面 ------------------------------------------------------
    def render_html(self, store: ExperimentStore) -> str:
        d = self.design
        esc = html.escape

        rows = []
        for s in d.segments:
            rep = store.segment_report(s.segment_id)
            imp = "缺报" if rep.impressions is None else rep.impressions
            clk = "缺报" if rep.clicks is None else rep.clicks
            badge = "报齐" if rep.is_complete else "未报齐"
            rows.append(
                "<tr>"
                f"<td>{esc(s.segment_id)}</td>"
                f"<td>{esc(s.pair_id)}-{s.pair_index + 1}</td>"
                f"<td>[{s.start_offset}, {s.end_offset})</td>"
                f"<td>缓冲 {s.buffer_length}</td>"
                f"<td><b>{esc(s.assigned_version)}</b></td>"
                f"<td>{esc(str(imp))}</td>"
                f"<td>{esc(str(clk))}</td>"
                f"<td>{badge}</td>"
                "</tr>"
            )

        timeline = []
        for e in self._entries:
            if e["kind"] == "seal":
                color = "#7a1d1d" if e["status"] == SEALED else "#8a6d1d"
                timeline.append(
                    f"<li><b>v{e['version']} 封存</b> "
                    f"<span style='color:{color}'>{esc(e['status'])}</span> "
                    f"<code>{esc(e['seal_id'])}</code><br>"
                    f"<small>指纹 {esc(e['fingerprint'][:16])}… "
                    f"缺失段 {esc(str(e['incomplete_segments']))} · {esc(e['at'])}</small></li>"
                )
            else:
                r = e["result"]
                p = "暂缓" if r["deferred"] else f"p={r['p_value']:.4f}"
                mean = "—" if r["mean_diff"] is None else f"{r['mean_diff']:.6f}"
                timeline.append(
                    f"<li><b>v{e['version']} 结论</b> "
                    f"<code>{esc(e['conclusion_id'])}</code><br>"
                    f"对内平均差 CTR(B)-CTR(A)={mean} · {p} · {esc(r['method'])}<br>"
                    f"<small>{esc(r['interpretation'])}</small></li>"
                )

        return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>轮流投放配对分析 · {esc(d.experiment_id)}</title>
<style>
 body {{ font-family: 'Noto Serif SC', serif; margin: 2rem; color: #2b2018; background: #fbf6ec; }}
 code, .mono {{ font-family: 'IBM Plex Mono', monospace; }}
 table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
 th, td {{ border: 1px solid #d8c9b2; padding: 6px 10px; text-align: left; font-size: .92rem; }}
 th {{ background: #f0e4cf; }}
 .lock {{ background: #f6ecda; border-left: 4px solid #7a1d1d; padding: .8rem 1rem; }}
 ul.tl {{ list-style: none; padding-left: 0; }} ul.tl li {{ margin: .8rem 0; border-bottom: 1px dashed #d8c9b2; padding-bottom: .6rem; }}
 small {{ color: #6b5c4a; }}
</style></head><body>
<h1>同入口轮流投放 · 配对互换分析</h1>
<div class="lock">
 <b>锁定分派（不可改派）</b><br>
 实验 {esc(d.experiment_id)} · 段长 {d.segment_length} · 段数 {d.segment_count} ·
 每段开头缓冲 {d.buffer_length} · 校验和 <code>{esc(d.lock_checksum or '')[:16]}…</code>
</div>
<h2>逐段上报状态（零值与缺报分开；错播归原分派；缓冲内不计）</h2>
<table><thead><tr><th>时段</th><th>配对</th><th>区间</th><th>缓冲</th><th>原分派</th><th>触达</th><th>点击</th><th>状态</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<h2>历次封存与结论（追加保留，更正只产生新版本）</h2>
<ul class="tl">{''.join(timeline) or '<li>尚无封存</li>'}</ul>
</body></html>"""
