"""缓冲池页 LSN 可恢复性验证器。

模拟崩溃恢复前的页版本检查：后台刷盘线程与 WAL 写入线程并发推进，
崩溃时数据页 LSN 可能领先于 WAL 持久化位置，导致恢复时错误跳过必要重放。

本模块只处理调用方传入的内存数据结构，不读取任何真实数据库文件。

核心不变量：数据页 LSN 不得高于 WAL 持久化点（wal_persisted_lsn）。
"""

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class PageStatus(Enum):
    ACCEPTABLE = "acceptable"      # 可接受页：页版本与持久化 WAL 一致
    NEEDS_REPLAY = "needs_replay"  # 需要重放页：页落后于 WAL，需 redo
    CORRUPTED = "corrupted"        # 损坏页：页领先于 WAL 或撕裂写无法修复


class FlushAnomaly(Enum):
    REPEATED_FLUSH = "repeated_flush"    # 重复刷盘（同 LSN 幂等，无害）
    STALE_FLUSH = "stale_flush"          # 过期刷盘：旧版本覆盖新版本
    PARTIAL_FLUSH = "partial_flush"      # 部分页写（撕裂页）


@dataclass
class BufferedPage:
    """缓冲池中的页镜像。"""
    page_id: int
    page_lsn: int          # 缓冲页当前 LSN（内存中最新修改）
    dirty: bool = True


@dataclass
class DiskPage:
    """数据文件中落盘的页。"""
    page_id: int
    file_lsn: int          # 数据文件页头记录的 LSN
    torn: bool = False     # 是否检测到部分页写（如校验和不匹配）


@dataclass
class FlushEvent:
    """一次后台刷盘事件。"""
    page_id: int
    lsn: int               # 刷盘时页的 LSN
    complete: bool = True  # False 表示部分页写（撕裂）
    sequence: int = 0      # 事件全局序号，用于排序竞争


@dataclass
class DoubleWriteCopy:
    """双写缓冲区中的页副本。"""
    page_id: int
    page_lsn: int
    intact: bool = True    # 副本本身是否完整


@dataclass
class RecoverySnapshot:
    """崩溃时刻的一致性快照（验证器输入）。"""
    wal_persisted_lsn: int                       # WAL 持久化点
    checkpoint_lsn: int                          # 最近一次 checkpoint 的 LSN
    buffered_pages: List[BufferedPage] = field(default_factory=list)
    disk_pages: List[DiskPage] = field(default_factory=list)
    flush_events: List[FlushEvent] = field(default_factory=list)
    double_write_copies: List[DoubleWriteCopy] = field(default_factory=list)


@dataclass
class PageVerdict:
    """单个数据文件页的判定结果。"""
    page_id: int
    status: PageStatus
    file_lsn: int
    redo_from_lsn: Optional[int] = None   # NEEDS_REPLAY 时的重放起点
    reason: str = ""
    via_double_write: bool = False        # 是否依赖双写副本修复


@dataclass
class FlushIssue:
    """刷盘事件分析发现的异常。"""
    page_id: int
    anomaly: FlushAnomaly
    detail: str


@dataclass
class ValidationReport:
    """验证器输出：三类页 + 刷盘异常列表。"""
    acceptable: List[PageVerdict] = field(default_factory=list)
    needs_replay: List[PageVerdict] = field(default_factory=list)
    corrupted: List[PageVerdict] = field(default_factory=list)
    flush_issues: List[FlushIssue] = field(default_factory=list)

    def verdicts(self) -> List[PageVerdict]:
        return self.acceptable + self.needs_replay + self.corrupted


def analyze_flush_events(events: List[FlushEvent]) -> List[FlushIssue]:
    """分析刷盘事件序列，区分重复刷盘、过期刷盘与部分页写。"""
    issues = []
    by_page = defaultdict(list)
    for event in events:
        by_page[event.page_id].append(event)

    for page_id in sorted(by_page):
        page_events = sorted(by_page[page_id], key=lambda e: e.sequence)
        max_flushed_lsn = None
        prev_event = None
        for event in page_events:
            if not event.complete:
                issues.append(FlushIssue(
                    page_id=page_id,
                    anomaly=FlushAnomaly.PARTIAL_FLUSH,
                    detail="flush at lsn %d incomplete (torn page)" % event.lsn,
                ))
            if max_flushed_lsn is not None and event.lsn < max_flushed_lsn:
                issues.append(FlushIssue(
                    page_id=page_id,
                    anomaly=FlushAnomaly.STALE_FLUSH,
                    detail="stale flush at lsn %d after lsn %d reached disk"
                           % (event.lsn, max_flushed_lsn),
                ))
            elif (prev_event is not None
                  and prev_event.complete
                  and event.complete
                  and event.lsn == prev_event.lsn):
                issues.append(FlushIssue(
                    page_id=page_id,
                    anomaly=FlushAnomaly.REPEATED_FLUSH,
                    detail="idempotent repeat flush at lsn %d" % event.lsn,
                ))
            if event.complete:
                if max_flushed_lsn is None or event.lsn > max_flushed_lsn:
                    max_flushed_lsn = event.lsn
            prev_event = event
    return issues


class PageLSNValidator:
    """按页判定崩溃后的可恢复性。"""

    def validate(self, snapshot: RecoverySnapshot) -> ValidationReport:
        if snapshot.checkpoint_lsn > snapshot.wal_persisted_lsn:
            raise ValueError(
                "checkpoint_lsn (%d) 不能高于 wal_persisted_lsn (%d)"
                % (snapshot.checkpoint_lsn, snapshot.wal_persisted_lsn)
            )

        buffered = {p.page_id: p for p in snapshot.buffered_pages}
        copies = {c.page_id: c for c in snapshot.double_write_copies}
        report = ValidationReport()
        report.flush_issues = analyze_flush_events(snapshot.flush_events)

        for disk_page in sorted(snapshot.disk_pages, key=lambda p: p.page_id):
            verdict = self._classify_page(disk_page, buffered, copies, snapshot)
            if verdict.status is PageStatus.ACCEPTABLE:
                report.acceptable.append(verdict)
            elif verdict.status is PageStatus.NEEDS_REPLAY:
                report.needs_replay.append(verdict)
            else:
                report.corrupted.append(verdict)
        return report

    def _classify_page(self, disk_page, buffered, copies, snapshot):
        wal_lsn = snapshot.wal_persisted_lsn
        copy = copies.get(disk_page.page_id)

        if disk_page.torn:
            return self._classify_torn_page(disk_page, copy, wal_lsn)

        if disk_page.file_lsn > wal_lsn:
            # 数据页领先于持久化 WAL：WAL 尾部丢失，无法重放该页的新版本。
            return PageVerdict(
                page_id=disk_page.page_id,
                status=PageStatus.CORRUPTED,
                file_lsn=disk_page.file_lsn,
                reason="page lsn %d ahead of persisted wal %d (wal tail lost)"
                       % (disk_page.file_lsn, wal_lsn),
            )

        buffer_page = buffered.get(disk_page.page_id)
        newer_in_buffer = (buffer_page is not None
                           and buffer_page.dirty
                           and buffer_page.page_lsn > disk_page.file_lsn)
        if disk_page.file_lsn < wal_lsn and newer_in_buffer:
            return PageVerdict(
                page_id=disk_page.page_id,
                status=PageStatus.NEEDS_REPLAY,
                file_lsn=disk_page.file_lsn,
                redo_from_lsn=disk_page.file_lsn,
                reason="page behind persisted wal; redo from lsn %d"
                       % disk_page.file_lsn,
            )

        return PageVerdict(
            page_id=disk_page.page_id,
            status=PageStatus.ACCEPTABLE,
            file_lsn=disk_page.file_lsn,
            reason="page consistent with persisted wal",
        )

    def _classify_torn_page(self, disk_page, copy, wal_lsn):
        if copy is not None and copy.intact:
            if copy.page_lsn > wal_lsn:
                return PageVerdict(
                    page_id=disk_page.page_id,
                    status=PageStatus.CORRUPTED,
                    file_lsn=disk_page.file_lsn,
                    via_double_write=True,
                    reason="double-write copy lsn %d ahead of persisted wal %d"
                           % (copy.page_lsn, wal_lsn),
                )
            return PageVerdict(
                page_id=disk_page.page_id,
                status=PageStatus.NEEDS_REPLAY,
                file_lsn=disk_page.file_lsn,
                redo_from_lsn=copy.page_lsn,
                via_double_write=True,
                reason="torn page restored from double-write copy; "
                       "redo from lsn %d" % copy.page_lsn,
            )
        return PageVerdict(
            page_id=disk_page.page_id,
            status=PageStatus.CORRUPTED,
            file_lsn=disk_page.file_lsn,
            reason="torn page without intact double-write copy",
        )
