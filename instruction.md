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
  - 在下一阶段改造中引入快照机制
- 旁路链路静默失效：
  - 缓解：强告警 + 健康探针 + 周期性恢复演练。

---

## 7. 里程碑验收标准

- M1（旁路引擎）：连续 10 min 压测无崩溃、无内存泄漏、无序号断裂。
- M2（回放工具）：在测试集可完整回放并通过 OSD 启动与基础健康检查。
- M3（灾备演练）：完成至少 2 次端到端破坏性恢复，RTO 统计稳定。

---

## 8. 后续增强（非本期）

- WAL 文件压缩与去重归档。
- 回放并行化（按 CF/分片策略，需验证顺序语义）。
- 与对象级/快照级备份机制融合，缩短超长 RTO。


---

## 9. 最小实现任务分解（按 Ceph 代码目录/符号）

### 9.1 PR-1：配置项与开关骨架（不改数据路径）

**目标**：先落地配置开关与参数，不改变数据路径行为。

**改动文件（建议）**：
- `src/common/options/global.yaml.in`
- `src/os/bluestore/BlueStore.cc`（`get_tracked_keys()` / `handle_conf_change()`）

**验收标准**：
- 新增配置项可读可写，OSD 启停无未知键报错。

### 9.2 PR-2：WAL 旁路器内核（双缓冲 + 后台线程）

**目标**：在不阻塞 RocksDB 主写入线程前提下旁路 `.log` 数据。

**改动文件（建议）**：
- `src/os/bluestore/BlueRocksEnv.h`
- `src/os/bluestore/BlueRocksEnv.cc`（`BlueRocksWritableFile::Append` 挂载点）
- `src/os/bluestore/WalBypassCapture.hpp`
- `src/os/bluestore/WalBypassCapture.cpp`

**验收标准**：
- 开关关闭时行为与基线一致；开启后旁路目录持续增长。

### 9.3 PR-3：严格轮转与序号持久化

**目标**：实现 size/time 轮转与重启后序号连续。

**改动文件（建议）**：
- `src/os/bluestore/WalBypassCapture.hpp`
- `src/os/bluestore/WalBypassCapture.cpp`
- `src/os/bluestore/BlueRocksEnv.cc`（引用/接入点）

**验收标准**：
- 高并发与频繁轮转下不丢失、不乱序、不重复序号。

### 9.4 PR-4：监控与告警可观测性

**目标**：暴露旁路链路核心指标。

**改动文件（建议）**：
- `src/os/bluestore/BlueStore.h`（新增 `l_bluestore_*` 计数项）
- `src/os/bluestore/BlueStore.cc`（`_init_logger()` 注册与更新）

**验收标准**：
- `ceph daemon osd.<id> perf dump` 可见新增字段。

### 9.5 PR-5：全量回放工具（MVP）

**目标**：独立工具支持 scan/sort/replay/checkpoint/stop 条件。

**改动文件（建议）**：
- `src/tools/CMakeLists.txt`
- `src/tools/ceph_bluestore_wal_replay.cc`（新文件）

**验收标准**：
- 从空骨架 DB 回放可成功打开；中断可续跑。

### 9.6 PR-6：测试与演练脚本（最小闭环）

**目标**：建立可重复回归与灾备演练流程。

**改动文件（建议）**：
- `src/test/objectstore/`（新增/扩展单测）
- `qa/` 或 `src/script/`（演练脚本，可选）

**验收标准**：
- 单测稳定通过；至少一次端到端 `mkfs + replay + OSD` 演练记录。

### 9.7 PR-7：恢复一致性校验与回放审计

**目标**：让回放结果“可证明正确”，而非仅“看起来可启动”。

**改动文件（建议）**：
- `src/tools/ceph_bluestore_wal_replay.cc`
- `src/tools/` 下新增简易审计输出（可同文件实现）

**实现要点**：
- 回放时输出并持久化统计：处理文件数、batch 数、总字节、最后序号。
- 增加 `--verify-only`（只扫描与校验，不写 DB）模式。
- 检测序号断裂/文件缺失并返回非 0 退出码。

**验收标准**：
- 故意删掉中间 WAL 文件时，工具能明确报错并定位到文件序号。
- 正常链路回放后，审计结果可重复。

### 9.8 PR-8：运维手册与值班操作流程

**目标**：形成值班可执行 SOP，降低恢复过程中人为错误率。

**改动文件（建议）**：
- `instruction.md`（本文件）
- `doc/` 下新增或补充操作文档（如 `doc/rados/operations/`，按仓库规范放置）

**实现要点**：
- 明确“触发恢复”的判定条件。
- 给出标准流程：`mkfs -> replay -> 启动 OSD -> 健康检查 -> 回写报告`。
- 列出常见故障与处理：ENOSPC、回放中断、序号断裂、RTO 超时。

**验收标准**：
- 新值班同学按文档可独立完成一次演练。
- 演练报告包含耗时分解与失败重试记录。

---

## 10. 开发顺序与时间估算（建议）

- 周 1：PR-1 + PR-2
- 周 2：PR-3 + PR-4
- 周 3：PR-5 + PR-7
- 周 4：PR-6 + PR-8

---

## 11. 交付清单（Definition of Done）

1. 旁路 WAL 持续写入，轮转序号严格连续。
2. 独立盘异常时主写路径不阻断，且有明确告警与指标。
3. 回放工具支持完整重放、checkpoint 续跑与 `--verify-only` 校验。
4. 至少一次破坏性恢复演练并产出 RTO 报告。
5. 文档包含配置说明、恢复步骤、审计项与已知限制。

---

## 12. 当前进度摘要（2026-03-03）

### 12.1 PR-1 完成状态

已完成 **PR-1：配置项与开关骨架（不改数据路径）**，并通过构建与运行时测试验证。

**已落地内容：**
- 新增配置项（6 个）：
  - `bluerocks_wal_bypass_enable`
  - `bluerocks_wal_bypass_dir`
  - `bluerocks_wal_rotate_size_mb`
  - `bluerocks_wal_rotate_interval_sec`
  - `bluerocks_wal_flush_trigger_kb`
  - `bluerocks_wal_flush_interval_ms`
- `BlueStore::get_tracked_keys()` 已接入上述配置键。
- `BlueStore::handle_conf_change()` 已接入对应变更分支（骨架处理，不改变数据路径语义）。

### 12.2 测试结论（PR-1）

- `ceph-osd --show-config-value` 可读取 6 个新键默认值。
- 使用临时配置文件覆盖后，可读回覆盖值。
- 在运行中的 `osd.0` 执行 `ceph config set` 后，
  `ceph daemon osd.0 config get/config diff` 可确认值已生效。
- 执行 `ceph config rm` 后可回滚至默认值。

### 12.3 文档留档

- 已新增测试留档文档：`doc/wal/pr1-config-validation.rst`
  - 记录了测试方法、关键命令、结果与回滚验证。

### 12.4 下一步建议

- 进入 PR-2：在 `BlueRocksEnv` / `BlueRocksWritableFile::Append` 实现 WAL 旁路双缓冲主干，
  并保持开关关闭时路径零影响。

### 12.5 PR-2 当前进度（2026-03-03）

已完成 PR-2 最小实现与在线验证。

**已落地内容：**
- 当前实现位于 `src/os/bluestore/WalBypassCapture.cpp`
  （由 `src/os/bluestore/BlueRocksEnv.cc` 的 `BlueRocksWritableFile::Append` 挂载调用）。
- 在 `BlueRocksWritableFile::Append` 中实现 `.log` 文件旁路写入。
- 保持主写路径不变：原 `append_try_flush` 逻辑未改语义。
- 完成运行验证：旁路目录出现 `.bypass` 文件，压测期间字节持续增长。

**关键设置注意事项（必须遵守）：**
- 旁路器在 WAL 文件打开时读取开关，先前是“集群已开机后才 set 配置”，旧 WAL writer 不会自动切换。我会重启 OSD 进程（保留 mon 配置）后再跑一次写入验证。

**留档位置：**
- `doc/wal/pr2-bypass-validation.rst`

### 12.6 PR-3 当前进度（2026-03-04）

已完成 PR-3 的核心能力与补充稳定性回归。

**已落地内容：**
- 在 `src/os/bluestore/WalBypassCapture.cpp`
  （由 `src/os/bluestore/BlueRocksEnv.cc` 引用）完成严格轮转与序号持久化链路验证：
  - 旁路文件按 `ceph_wal_<seq>.log` 严格递增；
  - `ceph_wal_seq.state` 与最大文件序号保持 `+1` 一致；
  - 支持按大小/时间触发轮转（本次重点验证 2 秒时间轮转）。
- 在 3 OSD 场景下复现并定位一次 `osd.0` 段错误：
  - 崩溃线程：`bstore_kv_sync`；
  - 关键栈：`BlueRocksWritableFile::Append` / `WalBypassCapture` 路径；
  - 根因：`flush_loop()` 对 `m_current_stream` 的写入/轮转与前台追加存在并发竞争。
- 已完成并发修复：
  - 调整 `flush_loop()`，确保对 `m_current_stream` 的关键访问在同一把锁下完成，
    消除 `rotate_stream()` 与前台 `append()` 的生命周期竞争。

**测试结论（PR-3）：**
- 短压验证通过：序号连续、`state` 对齐、按时轮转可观测。
- 修复后 3 OSD 长压（600 秒）通过：
  - `rados bench` 正常结束（exit code 0）；
  - `osd tree` 保持 `3 up / 3 in`；
  - `out/osd.0.log/out/osd.1.log/out/osd.2.log` 中 `Segmentation fault` 计数均为 0；
  - 旁路文件序号持续连续（样例：`1..1269`），`state=1270` 匹配。

**观测备注：**
- 压测后出现 `BlueFS spillover` 告警，属于容量/布局信号，
  不等同于 WAL 旁路功能失败，后续需在容量治理中处理。

**留档位置：**
- `doc/wal/pr3-rotation-validation.rst`

### 12.7 下一步建议（更新）

- 进入 PR-4：补齐可观测性与告警指标（旁路吞吐、积压、错误计数、回放进度）。

### 12.8 PR-4 当前进度（2026-03-04）

已完成 PR-4：监控与告警可观测性。

**已落地内容：**
- 在 `src/os/bluestore/BlueStore.h` 新增 WAL 旁路相关 perf 项：
  - `l_bluestore_wal_bypass_bytes_total`
  - `l_bluestore_wal_bypass_files_total`
  - `l_bluestore_wal_bypass_flush_latency`
  - `l_bluestore_wal_bypass_backlog_bytes`
  - `l_bluestore_wal_bypass_write_errors_total`
- 在 `src/os/bluestore/BlueStore.cc::_init_logger()` 注册对应字段：
  - `wal_bypass_bytes_total`
  - `wal_bypass_files_total`
  - `wal_bypass_flush_latency`
  - `wal_bypass_backlog_bytes`
  - `wal_bypass_write_errors_total`
- 在 `src/os/bluestore/BlueRocksEnv.{h,cc}` 透传 `BlueStore::logger` 到 `WalBypassCapture`。
- 在 `src/os/bluestore/WalBypassCapture.{hpp,cpp}` 增加实时打点：
  - 成功 flush 后累加 bytes；
  - 新旁路文件打开时累加 files；
  - flush 写入路径记录 latency；
  - 前台/后台缓冲变化时更新 backlog；
  - 写入/轮转失败时累加 errors。

**构建验证：**
- 增量构建通过：`cd build && ninja -j3 ceph-osd`。

**留档位置：**
- `doc/wal/pr4-observability-validation.rst`

### 12.9 PR-4 在线验证结果（2026-03-04，3 OSD 短压）

已按 3 OSD 场景完成在线验证（不含长压）。

**执行形态：**
- 冷启动后以 `MON=1 OSD=3 MGR=1` 拉起集群；
- 启动时注入 `bluerocks_wal_bypass_*` 配置；
- `rados bench 12s write -b 4096` 触发 WAL 写入；
- 分别读取 `osd.0~2` 的 `perf dump` 新增字段。

**关键结果：**
- 集群状态：`HEALTH_OK`，`3 up / 3 in`；
- 压测结果：`Bandwidth=3.47814 MB/s`，`IOPS=890`，`Avg Latency=0.0179568 s`；
- 三个 OSD 上新增指标均可见且行为符合预期：
  - `wal_bypass_bytes_total` 均约 75MB 并持续增长；
  - `wal_bypass_files_total` 为 16~17；
  - `wal_bypass_flush_latency` 有效（`avgcount` 约 1.4 万，`avgtime` 约 `2e-05`）；
  - `wal_bypass_backlog_bytes=0`（采样点空闲）；
  - `wal_bypass_write_errors_total=0`；
- `/tmp/wal_bypass_pr4` 下可见连续命名的 `ceph_wal_*.log` 文件。

**结论：**
- PR-4 可观测性链路在 3 OSD 在线场景验证通过。
