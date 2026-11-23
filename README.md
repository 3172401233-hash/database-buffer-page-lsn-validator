# database-buffer-page-lsn-validator

验证缓冲页 LSN、脏页和恢复顺序一致性。

## 背景

缓冲池中的页被后台线程刷盘，WAL 写入线程又同时推进 LSN。崩溃后，
数据页 LSN 可能高于 WAL 持久化位置，系统可能错误地跳过必要重放。
本工具在崩溃快照上验证核心不变量：**数据页 LSN 不得领先于持久化 WAL**。

## 重要说明

本工具**不读取任何真实数据库文件**。所有输入（缓冲页、页 LSN、
数据文件 LSN、WAL 持久化点、checkpoint、脏页、刷盘事件、双写副本）
均由调用方以内存数据结构（`RecoverySnapshot`）传入，输出为页分类报告。

## 输入

- `wal_persisted_lsn`：WAL 持久化点
- `checkpoint_lsn`：最近一次 checkpoint 的 LSN（不得高于持久化点）
- `buffered_pages`：缓冲池页镜像（页 ID、页 LSN、脏页标记）
- `disk_pages`：数据文件页（页 ID、文件 LSN、是否撕裂写）
- `flush_events`：后台刷盘事件（含序号，区分完整写与部分页写）
- `double_write_copies`：双写缓冲区副本

## 输出

- `acceptable`：可接受页，页版本与持久化 WAL 一致
- `needs_replay`：需要重放页，页落后于 WAL，给出 `redo_from_lsn` 重放起点
- `corrupted`：损坏页，页 LSN 领先于持久化 WAL（WAL 尾部丢失），
  或撕裂写且无完整双写副本可修复
- `flush_issues`：刷盘异常，区分重复刷盘（幂等）、过期刷盘（旧版本
  覆盖新版本）与部分页写（撕裂页）

## 用法

```python
from page_lsn_validator import (
    BufferedPage, DiskPage, PageLSNValidator, RecoverySnapshot,
)

snapshot = RecoverySnapshot(
    wal_persisted_lsn=1000,
    checkpoint_lsn=500,
    disk_pages=[DiskPage(page_id=1, file_lsn=1200)],
    buffered_pages=[BufferedPage(page_id=1, page_lsn=1200, dirty=False)],
)
report = PageLSNValidator().validate(snapshot)
# 页 1 的 file_lsn=1200 领先于 wal_persisted_lsn=1000 -> corrupted
```

## 测试

```sh
python -m unittest
```

测试覆盖页刷新竞争、WAL 尾部丢失、双写副本修复，以及重复刷盘与
部分页写的区分。代码兼容 Python 3.8+。
