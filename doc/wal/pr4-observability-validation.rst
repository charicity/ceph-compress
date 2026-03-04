PR-4：WAL 旁路可观测性验证
============================

范围
----

PR-4 为 BlueStore 的 WAL 旁路捕获链路新增了 perf 计数器，
并在 ``WalBypassCapture`` 中接入了实时更新。

新增 perf 字段
-------------

在 ``bluestore`` 下新增并暴露以下字段：

- ``wal_bypass_bytes_total``
- ``wal_bypass_files_total``
- ``wal_bypass_flush_latency``
- ``wal_bypass_backlog_bytes``
- ``wal_bypass_write_errors_total``

验证步骤
--------

1. 确认已开启 WAL 旁路，并重启 OSD，确保当前 WAL writer 重新读取配置。
2. 施加元数据写入压力（例如 ``rados bench``）。
3. 观察 perf 计数器：

.. code-block:: bash

   ceph daemon osd.0 perf dump | jq '.bluestore | {
     wal_bypass_bytes_total,
     wal_bypass_files_total,
     wal_bypass_flush_latency,
     wal_bypass_backlog_bytes,
     wal_bypass_write_errors_total
   }'

预期结果
--------

- ``wal_bypass_bytes_total`` 在 WAL 流量存在时持续增长。
- ``wal_bypass_files_total`` 在轮转打开新旁路文件时递增。
- ``wal_bypass_flush_latency`` 在有负载时存在且为非零值。
- ``wal_bypass_backlog_bytes`` 会随 active/flush 缓冲变化而波动，空闲时回落到接近 0。
- ``wal_bypass_write_errors_total`` 在健康路径下保持 0；发生旁路写入/轮转失败时递增。

构建验证
--------

已完成 ``ceph-osd`` 目标的增量构建验证：

.. code-block:: bash

   cd build
   ninja -j3 ceph-osd

在线验证结果（3 OSD，短压）
--------------------------

测试环境采用 ``MON=1 OSD=3 MGR=1``，并在启动时注入 WAL bypass 配置。
本次仅执行短压（12s），不执行长压。

.. code-block:: text

    cluster: HEALTH_OK
    osd: 3 up / 3 in

    rados bench 12s:
    Bandwidth (MB/sec): 3.47814
    Average IOPS: 890
    Average Latency(s): 0.0179568

    osd.0:
       wal_bypass_bytes_total: 75539645
       wal_bypass_files_total: 17
       wal_bypass_backlog_bytes: 0
       wal_bypass_write_errors_total: 0
       wal_bypass_flush_latency.avgcount: 14004
       wal_bypass_flush_latency.avgtime: 2.0468e-05

    osd.1:
       wal_bypass_bytes_total: 75369728
       wal_bypass_files_total: 17
       wal_bypass_backlog_bytes: 0
       wal_bypass_write_errors_total: 0
       wal_bypass_flush_latency.avgcount: 13963
       wal_bypass_flush_latency.avgtime: 2.0401e-05

    osd.2:
       wal_bypass_bytes_total: 75415475
       wal_bypass_files_total: 16
       wal_bypass_backlog_bytes: 0
       wal_bypass_write_errors_total: 0
       wal_bypass_flush_latency.avgcount: 13973
       wal_bypass_flush_latency.avgtime: 2.2393e-05

判定
----

- 新增 perf 字段在 3 个 OSD 上均可见；
- ``wal_bypass_bytes_total`` 与 ``wal_bypass_files_total`` 随负载增长；
- ``wal_bypass_flush_latency`` 统计有效；
- ``wal_bypass_write_errors_total`` 保持 0；
- 旁路目录持续生成 ``ceph_wal_<seq>.log`` 文件。

附录：代码审查后加固（2026-03-04）
===================================

对 PR-1 至 PR-4 进行全量代码审查后，完成以下加固修复。

P0 修复
-------

1. **flush_loop 持锁做 I/O → 释放锁后执行磁盘操作**

   原 ``flush_loop()`` 在整个循环中持有 ``m_lock``，
   导致前台 ``append()`` 在旁路盘 I/O 期间被阻塞。
   修复后：dequeue 数据 → 释放锁 → 写入 / 轮转 → 重新加锁更新状态。

2. **m_enabled / m_failed 无锁读取 → 改为 std::atomic<bool>**

   消除了 ``append()`` 路径上的 data race (UB)。

3. **backlog 无上限 → 新增 bluerocks_wal_max_backlog_mb**

   积压超限时丢弃数据并累加 ``wal_bypass_drops_total`` 计数器，
   防止旁路盘慢时导致 OOM。

P1 修复
-------

4. **每个 WAL 文件一个旁路器实例 → 提升到 BlueRocksEnv 级别共享**

   ``WalBypassCapture`` 生命周期绑定到 ``BlueRocksEnv``，
   所有 ``BlueRocksWritableFile`` 共享同一实例，
   避免 RocksDB WAL 轮转时频繁创建/销毁后台线程。

5. **state 文件和旁路文件缺少 fsync → 新增 POSIX fsync 辅助函数**

   - ``persist_state_file()``：rename 后 ``fsync`` 目录。
   - ``WalBypassCaptureStream``：改用 POSIX fd 写入，
     ``sync_and_close()`` 调用 ``fdatasync`` + ``fsync`` 目录。

6. **配置项 runtime 标志无效 → 移除 flags: runtime**

   ``handle_conf_change`` 改为输出"需重启 OSD 生效"日志。

P2 修复
-------

7. ``is_wal_file()`` 增加数字前缀校验，排除 ``OPTIONS-*.log`` 等。
8. ``scan_max_bypass_seq()`` 改用 ``it.increment(ec)`` 显式迭代。
9. 移除无用的 ``#define dout_context nullptr``。
10. 移除未使用的 ``fsync_fd()`` 消除编译警告。

新增配置与计数器
----------------

- ``bluerocks_wal_max_backlog_mb``（uint，默认 256，最小 1）
- ``wal_bypass_drops_total``（uint64 counter）

冒烟测试脚本
------------

新增 ``qa/standalone/test-wal-bypass.sh``：

.. code-block:: bash

   cd build
   bash ../qa/standalone/test-wal-bypass.sh [BENCH_SECONDS]

自动执行：停集群 → 清数据 → 拉起 1 OSD → 压测 → 关集群 → 验证旁路文件。

验证结果（1 OSD，10s 短压）：

.. code-block:: text

   [PASS]  WAL bypass log files found (27 files)
   [PASS]  Sequence state file exists
   [PASS]  Bypass files contain data (74123735 bytes)
   [PASS]  Perf counter wal_bypass_bytes_total > 0 (74118324)
   [PASS]  No write errors reported
   [PASS]  No data drops reported
   [PASS]  Sequence numbers continuous: 1..27 (27 files)
   [PASS]  State file value (28) == last_seq+1 (28)

   ========== ALL CHECKS PASSED ==========
