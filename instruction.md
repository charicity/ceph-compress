# Ceph OSD 元数据 WAL 旁路备份与全量回放方案（纯 WAL 精简版）

## 0. 目标与边界

### 0.1 目标
- 在不打扰 RocksDB 正常写路径的前提下，将 OSD 元数据 WAL 持续旁路到独立存储介质。
- 灾难后基于“空骨架 RocksDB + WAL 全量重放”恢复 OSD 元数据状态。
- 方案优先保证：**写入路径无感**、**回放顺序绝对正确**、**实现复杂度可控**。

### 0.2 非目标
- 不引入 RocksDB Checkpoint/快照链。
- 不在本阶段实现增量截断、WAL 归档清理策略自动化（可后续扩展）。
- 不改变 Ceph 现有 OSD 业务语义与上层协议行为。

---

## 1. 核心架构：纯 WAL 旁路 + 全量重放

系统只做两件事：
1) 无感偷录（Bypass Capture）
2) 从头回放（Full Replay）

### 1.1 模块 A：实时拦截与无限轮转（OSD 进程内）

#### 1.1.1 拦截点
- 在 `BlueRocksWritableFile::Append` 中识别 RocksDB WAL 写入（`.log`）。
- 仅复制 WAL 字节流，不改写 RocksDB 原始写入逻辑。
- 原写路径必须保持原有时延特征，旁路逻辑不应引入同步 I/O 阻塞。

#### 1.1.2 双缓冲异步刷盘
- 使用两个内存缓冲区：`active_buffer`（前台追加）与 `flush_buffer`（后台刷盘）。
- 典型流程：
  - 前台线程写入 `active_buffer`。
  - 达到触发阈值（字节阈值/时间阈值）后做原子交换。
  - 后台线程消费 `flush_buffer`，顺序写入独立磁盘文件。
- 约束：
  - 前台线程只做内存拷贝 + 轻量同步，不等待磁盘完成。
  - 后台线程串行写，避免文件内顺序错乱。
  - 进程退出时执行 `drain + flush + fsync + close`。

#### 1.1.3 严格轮转机制（无限增长场景）
- 轮转触发条件（建议“或”关系）：
  - 单文件累计写入 >= `1 GiB`；
  - 文件打开时长 >= `24h`。
- 文件命名建议：
  - `ceph_wal_0000000001.log`
  - `ceph_wal_0000000002.log`
- 强制要求：
  - 序号严格单调递增、无回退。
  - `close old -> fsync metadata -> open new` 顺序可审计。
  - 轮转窗口内内存数据不丢失，不跨文件乱序。

#### 1.1.4 顺序与完整性元信息
- 每个旁路文件建议写入文件头（header）：
  - `magic/version`、`file_seq`、`create_ts`、`start_lsn(可选)`。
- 文件尾或 sidecar 建议记录：
  - `bytes_written`、`crc32/xxhash`（可选）、`end_seqno_hint(可选)`。
- 目的：回放前快速校验、发现断裂、定位损坏区间。

#### 1.1.5 关键失败场景处理
- 独立盘写慢：
  - 指标告警（backlog bytes、flush latency、drop count）。
  - 可配置“背压策略/告警策略”，默认不阻塞主写路径。
- 独立盘不可写（ENOSPC/EIO）：
  - 记录高优先级错误日志并打点。
  - 根据策略选择：继续服务但标记“备份失效窗口”。
- 进程崩溃：
  - 接受最后极短窗口未落盘风险；
  - 通过小批次 flush 与更短触发周期降低窗口。

### 1.2 模块 B：异地复原与全量回放（独立 C++ 工具）

#### 1.2.1 恢复前置
- 在目标机先运行：`ceph-osd --mkfs`。
- 目的：生成合法空 RocksDB（正确 `MANIFEST`、CF ID、元数据布局）。

#### 1.2.2 回放主流程
1. 扫描 WAL 目录，按 `file_seq` 升序排序。
2. 逐文件读取并解析 RocksDB WAL `WriteBatch`。
3. 对空库执行 `db->Write(write_opts, &batch)`。
4. 关键参数：`write_opts.disableWAL = true`。

#### 1.2.3 回放中断续跑（必须）
- 工具维护恢复检查点（checkpoint），建议每 N 条 batch 或每 M 秒落盘：
  - `last_file_seq`
  - `last_file_offset`
  - `last_applied_seqno`（若可提取）
- 重启后从检查点继续，避免“3 天回放中断后从头再来”。

#### 1.2.4 回放终止条件（时间旅行能力）
- 支持可选停止条件：
  - 截止时间戳 `--stop-ts`
  - 截止序号 `--stop-seqno`
- 以实现“恢复到历史某一时刻”的定点复原。

#### 1.2.5 数据一致性校验
- 回放后至少校验：
  - RocksDB 可正常打开；
  - 关键 CF 存在且元数据读路径可工作；
  - OSD 启动后可加入集群并通过基础健康检查。

---

## 2. 纯 WAL 方案权衡（必须提前评估）

| 评估维度 | 纯 WAL 方案表现 | 应对策略 / 现实考量 |
|---|---|---|
| 系统侵入性 | 极低 | 不调用 Checkpoint、不干预 Compaction，主链路改动最小 |
| 存储空间成本 | 极高（持续增长） | 规划独立低成本大容量存储；建立容量告警与离线归档策略 |
| 灾难恢复时间（RTO） | 极长（可能天级） | `disableWAL` 加速仅降低部分 I/O；仍受 MemTable flush + SST compaction 物理上限约束 |
| 数据灵活性 | 高（支持时间旅行） | 借助 stop 条件实现任意历史点回放 |

容量粗算示例：若单 OSD 每日 10GB WAL，1 年约 `3.65TB`（未压缩，不含冗余）。

---

## 3. 实施路线图

### Phase 1：旁路与轮转引擎
- 在 `BlueRocksEnv` / `BlueRocksWritableFile` 植入 WAL 拦截与双缓冲。
- 实现独立盘写线程与轮转状态机。
- 验收重点：
  - 切换窗口无丢失、无乱序；
  - 序号连续；
  - OSD 前台写延迟无显著回归。

### Phase 2：压力与稳定性
- 用 `fio`/`rados bench` 施加高强度随机小写入。
- 观测项：
  - OSD RSS 曲线（双缓冲是否泄漏）；
  - 后台 flush 吞吐是否长期追平 WAL 产生速率；
  - backlog 是否可控。
- 故障注入：
  - 独立盘限速/满盘/短暂断开；
  - OSD 重启与异常退出。

### Phase 3：全量回放工具
- 开发独立 C++ 回放程序（scan/sort/parse/apply）。
- 优先完成：
  - `disableWAL` 写入模式；
  - 可恢复 checkpoint；
  - stop 条件；
  - 进度与速率统计。

### Phase 4：破坏性复原演练
- 模拟元数据盘损坏（测试环境）。
- 执行 `mkfs + replay` 端到端重建。
- 启动 OSD 并验证：
  - 守护进程状态正常；
  - 集群健康与数据路径可用；
  - 演练报告记录 RTO、失败点、人工介入步骤。

---

## 4. 最小可交付（MVP）定义

MVP 必须同时满足：
1. 拦截 `.log` WAL 并持续旁路写入，支持按大小/时间轮转。
2. 文件序号严格连续，重启后不倒退。
3. 回放工具可从 0 开始恢复并成功启动 OSD。
4. 回放工具支持中断续跑。
5. 在压力测试下，OSD 主写路径无明显阻塞（仅允许可解释的轻微开销）。

---

## 5. 配置与运维建议（首版）

建议增加可配置项（命名示例）：
- `bluerocks_wal_bypass_enable`
- `bluerocks_wal_bypass_dir`
- `bluerocks_wal_rotate_size_mb`
- `bluerocks_wal_rotate_interval_sec`
- `bluerocks_wal_flush_trigger_kb`
- `bluerocks_wal_flush_interval_ms`

建议暴露监控指标：
- `wal_bypass_bytes_total`
- `wal_bypass_files_total`
- `wal_bypass_flush_latency_ms`
- `wal_bypass_backlog_bytes`
- `wal_bypass_write_errors_total`
- `wal_replay_batches_total`
- `wal_replay_bytes_total`
- `wal_replay_progress_file_seq`

---

## 6. 风险清单与缓解

- 回放时间不可接受：
  - 缓解：分层存储、并行预处理（解析与校验）、离线演练确定 SLA。
- 旁路链路静默失效：
  - 缓解：强告警 + 健康探针 + 周期性恢复演练。
- 超长周期日志管理失控：
  - 缓解：明确保留策略、归档流程、容量阈值与人工升级路径。

---

## 7. 里程碑验收标准

- M1（旁路引擎）：连续 72h 压测无崩溃、无内存泄漏、无序号断裂。
- M2（回放工具）：在测试集可完整回放并通过 OSD 启动与基础健康检查。
- M3（灾备演练）：完成至少 3 次端到端破坏性恢复，RTO 统计稳定。

---

## 8. 后续增强（非本期）

- WAL 文件压缩与去重归档。
- 回放并行化（按 CF/分片策略，需验证顺序语义）。
- 与对象级/快照级备份机制融合，缩短超长 RTO。

---

## 9. 最小实现任务分解（按 Ceph 代码目录/符号）

本节用于直接指导编码，优先采用“小步提交、可回滚”的 PR 切分。

### 9.1 PR-1：配置项与开关骨架（不改数据路径）

**目标**：先把功能开关、参数与运行期可见性建好，但不做真实旁路写入。

**改动文件（建议）**：
- `src/common/options/global.yaml.in`
  - 新增配置项：
    - `bluerocks_wal_bypass_enable`（bool，默认 false）
    - `bluerocks_wal_bypass_dir`（str）
    - `bluerocks_wal_rotate_size_mb`（uint）
    - `bluerocks_wal_rotate_interval_sec`（uint）
    - `bluerocks_wal_flush_trigger_kb`（uint）
    - `bluerocks_wal_flush_interval_ms`（uint）
- `src/os/bluestore/BlueStore.cc`
  - 在 `BlueStore::get_tracked_keys()` 注册以上键。
  - 在 `BlueStore::handle_conf_change()` 增加配置变化处理入口（首版允许仅更新内存参数，不做热切换重建）。

**验收标准**：
- `ceph config get osd.<id> <key>` 可见新增参数。
- OSD 启动/重启不报未知配置项。

### 9.2 PR-2：WAL 旁路器内核（双缓冲 + 后台线程）

**目标**：在不影响 RocksDB 正常写入的前提下，复制 `.log` 数据到独立目录。

**改动文件（建议）**：
- `src/os/bluestore/BlueRocksEnv.h`
  - 新增旁路器类声明（建议独立类，如 `BlueRocksWalBypass`）。
  - 保存必要状态：启停标记、双缓冲、互斥/条件变量、后台线程、当前轮转文件句柄、序号。
- `src/os/bluestore/BlueRocksEnv.cc`
  - 在 `class BlueRocksWritableFile` 中引入旁路器引用。
  - 在 `BlueRocksWritableFile::Append(const rocksdb::Slice&)` 中：
    1) 先调用现有 `fs->append_try_flush(...)`；
    2) 再以“无阻塞主路径”为前提复制数据到旁路缓冲；
    3) 若文件非 `.log` 直接跳过。
  - 在析构/关闭路径补齐 `drain + flush + fsync + close`。

**关键实现约束**：
- 旁路失败不得导致 RocksDB 主写失败（仅记录错误和计数）。
- 后台线程串行落盘，保证旁路文件内顺序。
- 任何时候都不在 `Append()` 里执行独立盘同步 I/O。

**验收标准**：
- 开关关闭时行为与基线一致。
- 开关开启后能在旁路目录持续看到 WAL 文件增长。
- 压测下 OSD 前台延迟无明显突刺（以基线对比）。

### 9.3 PR-3：严格轮转与序号持久化

**目标**：实现可审计的 `size/time` 轮转，并保证重启后序号连续。

**改动文件（建议）**：
- `src/os/bluestore/BlueRocksEnv.cc`
  - 轮转状态机：`maybe_rotate()` / `open_next_file()` / `finalize_current_file()`。
  - 命名规则固定为 `ceph_wal_%010llu.log`。
  - 文件头最小字段：`magic/version/file_seq/create_ts`。
  - 启动时扫描旁路目录求最大序号，下一文件从 `max+1` 开始。

**验收标准**：
- 达到阈值后自动切换新文件。
- 重启 OSD 后序号不回退、不重复。
- 人工注入轮转点（高频切换）无数据丢失/乱序证据。

### 9.4 PR-4：监控与告警可观测性

**目标**：把旁路链路运行状态暴露给运维与压测。

**改动文件（建议）**：
- `src/os/bluestore/BlueStore.h`
  - 在 `l_bluestore_*` 计数枚举中新增 WAL 旁路计数项。
- `src/os/bluestore/BlueStore.cc`
  - 在 `_init_logger()` 通过 `PerfCountersBuilder` 注册指标。
  - 在旁路关键路径更新计数：总字节、轮转次数、flush 延迟、写失败次数、backlog。

**验收标准**：
- `ceph daemon osd.<id> perf dump` 可见新增字段。
- 限速/满盘故障注入时，错误指标与日志同步增长。

### 9.5 PR-5：全量回放工具（MVP）

**目标**：提供独立工具完成“目录扫描 -> WAL 解析 -> disableWAL 写入 -> 断点续跑”。

**改动文件（建议）**：
- `src/tools/CMakeLists.txt`
  - 新增可执行目标（建议名：`ceph-bluestore-wal-replay`）。
- `src/tools/ceph_bluestore_wal_replay.cc`（新文件）
  - CLI 参数：
    - `--db-path`
    - `--wal-dir`
    - `--checkpoint-path`
    - `--stop-ts`（可选）
    - `--stop-seqno`（可选）
  - 扫描并按文件名序号排序。
  - 使用 RocksDB WAL 读取接口解析 `WriteBatch`。
  - 对目标 DB 执行 `db->Write(write_opts, &batch)` 且 `write_opts.disableWAL=true`。
  - 周期落盘 checkpoint：`file_seq + file_offset + last_applied_seqno`。

**验收标准**：
- 从空骨架 DB 回放到可打开状态。
- 中断后可从 checkpoint 继续，不从头重放。
- 指定 `--stop-*` 可稳定停在预期边界。

### 9.6 PR-6：测试与演练脚本（最小闭环）

**目标**：形成可重复的回归与灾备演练流程。

**改动文件（建议）**：
- `src/test/objectstore/` 下新增或扩展单测（优先围绕 BlueFS/BlueStore 测试框架）。
  - 覆盖：
    - 轮转边界；
    - 序号连续性；
    - 异常退出后的恢复可读性。
- `qa/` 或 `src/script/` 增加最小演练脚本（可选，若本期范围允许）。

**验收标准**：
- 单测可稳定复现并通过。
- 至少 1 次端到端“mkfs + replay + 启动 OSD”演练记录。

---

## 10. 开发顺序与时间估算（建议）

- 周 1：PR-1 + PR-2（功能开关 + 双缓冲核心）
- 周 2：PR-3 + PR-4（轮转可靠性 + 可观测性）
- 周 3：PR-5（回放工具 MVP）
- 周 4：PR-6（测试与演练，修正缺陷）

并行建议：
- 一名开发负责 OSD 内旁路链路（PR-1~4）。
- 一名开发负责回放工具（PR-5）与演练脚本（PR-6）。

---

## 11. 交付清单（Definition of Done）

满足以下全部条件才算本方案完成：

1. OSD 开启旁路后，WAL 文件持续落到独立目录，且轮转序号严格连续。
2. 独立盘异常时，OSD 主写路径不被阻断，并有明确告警与指标。
3. 回放工具可对空骨架 DB 完整重放，并支持 checkpoint 续跑。
4. 至少一次破坏性演练完成并产出 RTO 报告。
5. 文档/运维手册包含配置说明、恢复步骤与已知限制。
