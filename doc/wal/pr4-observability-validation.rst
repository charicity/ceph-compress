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
