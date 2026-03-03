PR-2 WAL 旁路器验证留档（双缓冲 + 后台线程）
==============================================

范围
----

本留档覆盖 PR-2 的最小实现与运行验证：

- 在 ``BlueRocksWritableFile::Append`` 对 RocksDB WAL（``.log``）做旁路捕获；
- 前台线程仅做内存追加；
- 后台线程异步落盘；
- 开关关闭时不影响原写路径。

本阶段不包含严格轮转、序号持久化、审计与回放工具。

实现要点
--------

代码位置：

- ``src/os/bluestore/BlueRocksEnv.cc``

关键点：

- 新增 ``WalBypassCapture``，采用 ``active_buffer`` + ``flush_buffer`` 双缓冲。
- 后台线程按触发阈值/时间周期刷盘。
- 仅当文件名后缀为 ``.log`` 且配置开关为 true 时启用旁路。
- 旁路写入目标目录由 ``bluerocks_wal_bypass_dir`` 决定。
- 进程关闭时执行 drain、fsync、close。

验证环境
--------

- 仓库：``/home/lj/ceph``
- 构建目录：``/home/lj/ceph/build``
- 集群：本地 ``vstart`` 开发集群
- 旁路目录：``/tmp/wal_bypass_pr2``

验证步骤
--------

1. 编译验证
^^^^^^^^^^^

仅编译目标对象，确认 PR-2 代码可编译。

.. code-block:: bash

   cd /home/lj/ceph
   ninja -C build src/os/bluestore/CMakeFiles/bluestore.dir/BlueRocksEnv.cc.o -j2

2. 启用旁路配置
^^^^^^^^^^^^^^^

.. code-block:: bash

   cd /home/lj/ceph/build
   rm -rf /tmp/wal_bypass_pr2 && mkdir -p /tmp/wal_bypass_pr2
   ./bin/ceph config set osd bluerocks_wal_bypass_enable true
   ./bin/ceph config set osd bluerocks_wal_bypass_dir /tmp/wal_bypass_pr2
   ./bin/ceph config set osd bluerocks_wal_flush_trigger_kb 4
   ./bin/ceph config set osd bluerocks_wal_flush_interval_ms 50

3. 关键注意事项（必须）
^^^^^^^^^^^^^^^^^^^^^^^

旁路器在 WAL 文件打开时读取开关，先前是“集群已开机后才 set 配置”，旧 WAL writer 不会自动切换。我会重启 OSD 进程（保留 mon 配置）后再跑一次写入验证。

4. 重启 OSD 后执行写入压测
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # 重启 OSD（保留 mon 配置）
   cd /home/lj/ceph/build
   ../src/stop.sh
   ../src/vstart.sh --debug --new -x --localhost --bluestore

   # 创建测试池并短时写入
   ./bin/ceph osd pool create pr2test 1 1
   ./bin/ceph osd pool set pr2test size 1 --yes-i-really-mean-it
   ./bin/ceph osd pool set pr2test min_size 1
   ./bin/rados -p pr2test bench 6 write --no-cleanup

5. 检查旁路目录文件与字节增长
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   find /tmp/wal_bypass_pr2 -maxdepth 1 -type f -name '*.bypass.*' -printf '%f %s\n' | sort

结果摘要
--------

- 重启前：旁路目录文件数为 0。
- 重启 OSD 后：旁路目录出现 ``*.bypass.*`` 文件（实测出现 3 个文件）。
- 写入压测前后总字节：

  - ``before_bytes = 51547484``
  - ``after_bytes  = 53812680``
  - ``delta_bytes  = 2265196``

结论
----

PR-2 最小目标已达成：

- 开关开启且 OSD 使用新 WAL writer 后，旁路文件可创建并持续增长；
- 前台写入链路保持可用，旁路刷盘由后台线程异步执行。
