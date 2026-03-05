# Ceph OSD 元数据 WAL 旁路与回放方案（精简执行版）

更新时间：2026-03-04

---

## 1. 目标与边界

### 目标
- 在不干扰 RocksDB 主写路径前提下，持续旁路 OSD 元数据 WAL 到独立介质。
- 灾后基于“空骨架 RocksDB（mkfs）+ WAL 全量回放”恢复元数据。
- 优先级：**主写无感 > 顺序正确 > 复杂度可控**。

### 非目标（本期不做，快照阶段实现）
- 不引入 RocksDB Checkpoint/快照链（→ PR-10 实现）。
- 不实现自动归档/自动截断策略（→ PR-10 实现 WAL GC）。
- 不改变 Ceph 现有 OSD 业务语义。

> **注意**：当前 MVP 阶段的代码设计必须为快照阶段预留改造空间，参见第 10 节。

---

## 2. 核心方案（纯 WAL）

### A. 在线旁路（OSD 进程内）
- 拦截点：`BlueRocksWritableFile::Append`，仅处理 WAL（`.log`）。
- 前台线程：只做内存追加，不等待独立盘 I/O。
- 后台线程：串行刷盘，保证单文件内顺序。
- 双缓冲：`active_buffer` / `flush_buffer`。
- 轮转：大小阈值或时间阈值触发（默认建议 1GiB 或 24h）。
- 文件命名：`ceph_wal_<seq>.log`，序号严格递增且持久化。
- 退出：`drain + flush + fsync + close`。

### B. 离线回放（独立工具）
- 前置：目标机先 `ceph-osd --mkfs` 生成合法空 DB。
- 回放：按 `file_seq` 升序扫描 WAL，解析 WriteBatch 后写入 DB。
- 关键参数：`disableWAL=true`（回放写入不再产生二次 WAL）。
- 断点续跑：checkpoint 保存 `file_seq/offset/seqno`。
- 可选终止：`--stop-ts` 或 `--stop-seqno`（时间旅行恢复）。

---

## 3. MVP 验收标准

MVP 必须同时满足：
1. `.log` WAL 可持续旁路，支持 size/time 轮转。
2. 轮转序号连续，重启不回退。
3. 回放工具可从空骨架 DB 成功恢复并可启动 OSD。
4. 回放支持中断续跑。
5. 压测下主写路径无明显阻塞（可解释的轻微开销可接受）。

---

## 4. 关键配置与观测项

### 配置项（已落地）
- `bluerocks_wal_bypass_enable`
- `bluerocks_wal_bypass_dir`
- `bluerocks_wal_rotate_size_mb`
- `bluerocks_wal_rotate_interval_sec`
- `bluerocks_wal_flush_trigger_kb`
- `bluerocks_wal_flush_interval_ms`
- `bluerocks_wal_max_backlog_mb`（防 OOM，超限触发丢弃）

### 监控项（已落地）
- `wal_bypass_bytes_total`
- `wal_bypass_files_total`
- `wal_bypass_flush_latency`
- `wal_bypass_backlog_bytes`
- `wal_bypass_write_errors_total`
- `wal_bypass_drops_total`

---

## 5. 风险与策略（当前结论）

- **RTO 长**：纯 WAL 全量回放可能天级；后续需快照机制降低恢复时长。
- **容量高**：WAL 持续增长，需独立大容量盘 + 告警 + 离线归档策略。
- **独立盘异常**：默认不阻塞主写，必须暴露错误与失效窗口。
- **慢盘积压**：通过 `max_backlog_mb` 控制内存上限，超限丢弃并计数。

---

## 6. PR 路线图（状态）

### 已完成
- **PR-1** 配置与开关骨架：完成并验证。
- **PR-2** 旁路最小链路：完成，`.log` 可持续旁路。
- **PR-3** 轮转与序号持久化：完成；修复并发崩溃问题。
- **PR-4** 可观测性：完成；新增 perf 指标并在线验证。
- **PR-5** 回放工具 MVP：完成（scan/verify/replay/checkpoint/stop）。

### 待完成
- **PR-6** 测试与演练脚本闭环：`mkfs + replay + OSD + health`。
- **PR-7** 回放审计增强：`--verify-only` 深化、断裂定位、审计输出。
- **PR-8** 运维手册/SOP：值班可执行恢复流程与故障处置。

---

## 7. 当前实现快照（关键事实）

### 已完成的稳定性修复（审查后）
- 修复 `flush_loop` 持锁 I/O：改为“锁内取数据、锁外 I/O、锁内更新状态”。
- 修复 `m_enabled/m_failed` 数据竞争：改为原子变量。
- 增加积压上限：防独立盘慢导致内存无上限增长。
- 将旁路器实例提升到 `BlueRocksEnv` 级共享，避免 WAL 轮转频繁建线程。
- 强化持久化：状态文件与旁路文件落盘路径增加 fsync 保障。
- 收敛匹配逻辑：WAL 文件识别更严格（数字前缀 + `.log`）。

### 验证摘要
- `ceph-osd` 增量构建通过（`ninja -j3 ceph-osd`）。
- 3 OSD 在线短压：集群健康、指标可见、错误计数为 0。
- 冒烟脚本通过：文件序号连续，state 与最后序号对齐。
- 回放脚本通过：verify-only、全量回放、断点续跑、序号断裂检测、幂等回放。

---

## 8. 关键文件锚点

- 旁路接入：`src/os/bluestore/BlueRocksEnv.cc`
- 旁路核心：`src/os/bluestore/WalBypassCapture.hpp/.cpp`
- 观测注册：`src/os/bluestore/BlueStore.h/.cc`
- 配置定义：`src/common/options/global.yaml.in`
- 回放工具：`src/tools/ceph_bluestore_wal_replay.cc`
- 回放共享工具：`src/os/bluestore/WalBypassUtil.h`

---

## 9. 文档留档

- `doc/wal/pr1-config-validation.rst`
- `doc/wal/pr2-bypass-validation.rst`
- `doc/wal/pr3-rotation-validation.rst`
- `doc/wal/pr4-observability-validation.rst`
- `doc/wal/pr5-replay-tool.rst`

---

## 10. 快照 + 增量 WAL 演进规划

当前为纯 WAL 全量回放方案，RTO 可能达天级。后续必须引入**定期 RocksDB 快照 + 增量 WAL 回放**，将恢复时间从 O(全量 WAL) 降低到 O(最近快照后的增量 WAL)。

### 技术路径
1. **在线快照**：OSD 运行期间定期触发 RocksDB Checkpoint（硬链接快照），将快照目录持久化到旁路介质。
2. **快照标记**：快照完成后，在旁路目录写入锚定文件 `ceph_wal_snapshot_<id>.json`，记录 `{snapshot_id, wal_file_seq, rocksdb_seqno, timestamp}`。
3. **WAL 截断**：快照成功并验证后，安全删除快照点之前的旧 WAL 旁路文件，释放磁盘空间。
4. **增量回放**：恢复时先拷贝最近快照为目标 DB，再仅回放快照点之后的 WAL 文件。

### 当前实现的前置改造点（快照就绪要求）

| # | 改造项 | 涉及文件 | 说明 |
|---|--------|----------|------|
| 1 | 回放工具支持 `--start-seq` | `ceph_bluestore_wal_replay.cc` | 允许跳过已截断的旧 WAL，从指定序号开始回放；`validate_continuity()` 需放松为"从 start-seq 开始连续" |
| 2 | 回放工具支持 `--snapshot-dir` | `ceph_bluestore_wal_replay.cc` | 指定快照目录作为基础 DB，回放前自动拷贝快照到 db-path |
| 3 | `WalBypassCapture` 增加快照标记接口 | `WalBypassCapture.hpp/.cpp` | 新增 `record_snapshot_marker()` 方法，在旁路目录落盘快照锚定元数据 |
| 4 | WAL 截断/GC 机制 | `WalBypassCapture.cpp` / 管理工具 | `WalBypassSeqState` 增加 `truncate_before(seq)` 接口，安全删除过期文件 |
| 5 | 快照调度策略（配置项） | `global.yaml.in` | 新增 `bluerocks_wal_snapshot_interval_sec` / `bluerocks_wal_snapshot_retain_count` 等配置 |

> **原则**：上述 1-2 项可在当前 PR-6~PR-8 期间顺带完成，不依赖快照机制本身，属于无风险的前瞻性改造。3-5 项在正式快照 PR 中实现。

---

## 11. 下一步（建议执行顺序）

- **PR-6** 测试与演练脚本闭环：`mkfs + replay + OSD + health`。
- **PR-7** 回放审计增强：`--verify-only` 深化、断裂定位、审计输出。
- **PR-8** 运维手册/SOP：值班可执行恢复流程与故障处置。
4. **PR-9**（前置改造）：回放工具增加 `--start-seq` / `--snapshot-dir` 参数，放松 `validate_continuity` 约束。
5. **PR-10**（快照核心）：实现在线 RocksDB Checkpoint + 快照标记 + WAL 截断。
6. **PR-11**（增量回放）：回放工具支持"快照 + 增量 WAL"完整恢复流程。

---

## 12. DoD（交付完成定义）

### MVP 阶段（PR-1 ~ PR-8）
1. WAL 旁路持续可用，序号连续且可审计。
2. 独立盘异常不阻断主写，且告警/指标明确。
3. 回放工具支持完整回放、断点续跑、校验模式。
4. 至少完成一次破坏性恢复演练并产出 RTO 报告。
5. 文档覆盖配置、恢复流程、审计项与已知限制。
6. 回放工具接口预留快照扩展点（`--start-seq` 参数已实现）。

### 快照阶段（PR-9 ~ PR-11）
7. 支持定期 RocksDB Checkpoint 输出到旁路介质。
8. 快照锚定文件可关联对应 WAL 序号，可用于截断判定。
9. WAL 截断安全执行，不破坏增量回放连续性。
10. "快照 + 增量 WAL"端到端恢复演练通过，RTO 降至小时级以内。
