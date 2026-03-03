PR-1 配置骨架测试留档（WAL bypass）
=====================================

范围
----

本次仅验证 PR-1（配置与开关骨架）是否满足“可设置、可读取、可回滚”，
不包含 WAL 旁路数据路径实现与性能验证。

涉及配置项：

- ``bluerocks_wal_bypass_enable``
- ``bluerocks_wal_bypass_dir``
- ``bluerocks_wal_rotate_size_mb``
- ``bluerocks_wal_rotate_interval_sec``
- ``bluerocks_wal_flush_trigger_kb``
- ``bluerocks_wal_flush_interval_ms``

测试环境
--------

- 仓库：``/home/lj/ceph``
- 构建目录：``/home/lj/ceph/build``
- 二进制：``build/bin/ceph-conf``、``build/bin/ceph-osd``
- 集群：本地 ``vstart`` 三副本开发集群

测试方法
--------

1. 离线默认值读取（二进制识别）
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

使用 ``ceph-osd --show-config-value`` 读取 6 个新键默认值，预期返回码均为 0。

示例：

.. code-block:: bash

   cd /home/lj/ceph
   build/bin/ceph-osd -i 0 --no-mon-config -c build/ceph.conf \
     --show-config-value bluerocks_wal_bypass_enable

2. 离线覆盖值读取（可设置）
^^^^^^^^^^^^^^^^^^^^^^^^^

创建临时配置文件并覆盖 6 个键，再次读取，预期读到覆盖值且返回码为 0。

示例配置：

.. code-block:: ini

   [global]
   bluerocks_wal_bypass_enable = true
   bluerocks_wal_bypass_dir = /var/lib/ceph/wal_bypass_test
   bluerocks_wal_rotate_size_mb = 64
   bluerocks_wal_rotate_interval_sec = 300
   bluerocks_wal_flush_trigger_kb = 256
   bluerocks_wal_flush_interval_ms = 20

3. 在线动态下发与 daemon 侧读取（运行时可配置）
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

在运行中的 ``osd.0`` 上执行：

- ``ceph config set osd.0 <key> <value>``
- ``ceph daemon osd.0 config get <key>``
- ``ceph daemon osd.0 config diff``

预期：

- ``config get`` 返回下发值；
- ``config diff`` 中可见 ``bluerocks_wal_*`` 条目。

4. 回滚验证
^^^^^^^^^^^

执行 ``ceph config rm osd.0 <key>`` 后，使用 ``ceph daemon osd.0 config get``
确认值恢复到默认值。

主要结果
--------

1) 默认值读取通过（``ceph-osd``）：

- ``bluerocks_wal_bypass_enable = false``
- ``bluerocks_wal_bypass_dir = ""``（空值）
- ``bluerocks_wal_rotate_size_mb = 1024``
- ``bluerocks_wal_rotate_interval_sec = 86400``
- ``bluerocks_wal_flush_trigger_kb = 1024``
- ``bluerocks_wal_flush_interval_ms = 100``

2) 覆盖值读取通过：

- ``true``、``/var/lib/ceph/wal_bypass_test``、``64``、``300``、``256``、``20``
  均可被 ``ceph-osd --show-config-value`` 正确读回。

3) 在线动态配置通过：

- ``ceph config set osd.0 ...`` 可成功下发；
- ``ceph daemon osd.0 config get`` 可读到下发值（示例：
  ``bluerocks_wal_bypass_enable=true``、``bluerocks_wal_flush_interval_ms=55``）；
- ``ceph daemon osd.0 config diff`` 中出现全部 6 个 ``bluerocks_wal_*`` 项。

4) 回滚通过：

- 删除 ``osd.0`` 覆盖配置后，
  ``bluerocks_wal_bypass_enable`` 恢复为 ``false``，
  ``bluerocks_wal_flush_interval_ms`` 恢复为 ``100``。

结论
----

PR-1 在“配置骨架”目标上已达成：

- 新增配置项已被 OSD 进程识别；
- 支持离线配置读取与在线动态设置；
- 支持回滚到默认值；
- 未引入数据路径行为变更（仅配置面生效）。
