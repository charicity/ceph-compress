PR-6：端到端恢复与演练脚本闭环
=================================

范围
----

PR-6 为 ``ceph-bluestore-wal-replay`` 工具新增 ``--recover`` 模式，
实现 "自动 mkfs + WAL 全量回放" 一键恢复流程。同时提供 3-OSD 端到端
恢复演练脚本，验证 ``mkfs → replay → OSD 启动 → 数据校验`` 完整路径。

设计决策
--------

1. **一键恢复**：``--recover`` 标志下工具自动执行
   ``BlueStore::mkfs()`` 重建骨架 DB，再全量回放 WAL 旁路文件。
   用户无需手动执行 ``ceph-osd --mkfs``。

2. **实例隔离**：mkfs 和 replay 使用两个独立的 ``BlueStore`` 实例。
   mkfs 实例在完成后立即销毁，避免内部状态（BlueFS、Allocator 等）
   泄漏到后续的 open_db_environment 调用中。

3. **原盘恢复**：mkfs 重建仅影响 BlueFS 区域和 RocksDB 元数据，
   不覆盖 block 设备上的数据对象区。WAL 全量回放后，
   ``PREFIX_SUPER``、``PREFIX_COLL``、``PREFIX_OBJ`` 等元数据从
   旁路 WAL 中恢复，OSD 可重新挂载并访问原有数据对象。

4. **测试场景**：使用 3-OSD 集群验证 replicated pool 恢复。
   写入测试对象 → 记录 MD5 → 破坏 OSD 0 → 恢复 → 比对校验和。

新增/修改文件清单
-----------------

.. list-table::
   :header-rows: 1

   * - 文件
     - 操作
     - 说明
   * - ``src/tools/ceph_bluestore_wal_replay.cc``
     - 修改
     - 新增 ``--recover`` 选项和 ``recover_bluestore_db()`` 函数
   * - ``qa/standalone/test-wal-recovery.sh``
     - 新增
     - 3-OSD 端到端恢复演练脚本

核心实现细节
------------

recover_bluestore_db() 恢复函数
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

该函数实现三阶段恢复：

**阶段 1：移除 mkfs_done 标记**

.. code-block:: cpp

   fs::path mkfs_done = fs::path(opt.osd_path) / "mkfs_done";
   fs::remove(mkfs_done, ec);

``BlueStore::mkfs()`` 检测到 ``mkfs_done`` 存在时会跳过执行（幂等保护）。
移除此文件允许 mkfs 重新执行，重建 BlueFS 和 RocksDB 骨架。

**阶段 2：执行 BlueStore::mkfs()**

.. code-block:: cpp

   auto mkfs_store = std::make_unique<BlueStore>(g_ceph_context, opt.osd_path);
   int r = mkfs_store->mkfs();
   // mkfs_store 析构，释放所有内部资源

mkfs 的关键行为：
- 读取已有 FSID（``<osd-path>/fsid``），不生成新的
- 重建 BlueFS 超级块和 RocksDB 空数据库
- 通过 ``bluestore_rocksdb_cfs`` 配置创建匹配的 Column Family 布局
- 初始化 ``PREFIX_SUPER`` 基础元数据（``nid_max=0``、``blobid_max=0``）
- 写入 ``mkfs_done`` 标记完成

**阶段 3：打开 DB 供回放**

.. code-block:: cpp

   return open_bluestore_db(opt, sharding_text, bluestore, raw_db, max_cf_id);

复用已有的 ``open_bluestore_db()`` 函数，通过 ``open_db_environment()``
获取 RocksDB 句柄。后续回放逻辑（scan → replay → flush）与常规模式完全一致。

WAL 回放恢复原理
~~~~~~~~~~~~~~~~~

回放将旁路捕获的所有 ``WriteBatch`` 按序重新应用到新建的骨架 DB 中：

1. ``PREFIX_SUPER`` 条目（``nid_max``、``blobid_max`` 等高水位标记）
   随 WAL 更新到正确的值
2. ``PREFIX_COLL`` 条目恢复所有 collection 元数据
3. ``PREFIX_OBJ`` 条目恢复所有 onode 到 block 设备偏移的映射
4. ``PREFIX_ALLOC`` 条目恢复 FreelistManager 位图（但不可直接使用，见下文）
5. ``PREFIX_DEFERRED`` 条目恢复（但必须清除，见下文）

回放完成后 OSD 即可正常 mount，通过已恢复的元数据访问 block 设备上的
数据对象。

关键：回放后修复步骤（Step 5b）
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**端到端测试中发现三个必须在回放后、OSD 启动前完成的修复操作。**
若缺少任何一步，OSD 将在 mount 时 assert 崩溃。

.. warning::

   以下三项修复是 ``--recover`` 模式能正常工作的关键。这些问题在
   纯单元测试（POSIX 模式回放）中不会暴露，只有在完整的
   ``mkfs → replay → OSD mount`` 链路中才会触发。

**修复 1：清除 BitmapFreelistManager 位图（PREFIX_ALLOC_BITMAP "b"）**

问题根因：``mkfs()`` 创建空的位图分配表（块设备全部标记为已用），
而 WAL 回放写入的 ONode 引用了真实的磁盘偏移。两者的位图状态不一致，
导致 OSD mount 时 ``BitmapFreelistManager::enumerate_next()`` 触发：

.. code-block:: text

   BitmapFreelistManager.cc: 387: FAILED
       ceph_assert(get_next_set_bit(enumerate_bl, 0) == 0)

解决方案：清除所有 ``PREFIX_ALLOC_BITMAP`` 条目，并将
``freelist_type`` 从 ``"bitmap"`` 改为 ``"null"``。OSD 启动时将使用
``NullFreelistManager``，通过 ``read_allocation_from_drive_on_startup()``
从 ONode extent 元数据自动重建分配表。此路径由配置项
``bluestore_allocation_from_file``（默认 ``true``）控制。

**修复 2：清除延迟事务（PREFIX_DEFERRED "L"）**

问题根因：WAL 中包含旁路捕获的延迟写入条目。OSD mount 时
``_deferred_replay()`` 会处理这些条目并释放临时分配的块。但由于分配表已被
重建（NullFM 从 ONode 推导），这些临时块已经在 free tree 中，再次释放导致
``AvlAllocator::_add_to_tree`` 断言失败（double-free）：

.. code-block:: text

   AvlAllocator.cc: 115: FAILED
       ceph_assert(rs_before == range_tree.end() || rs_before->end <= start)

解决方案：清除所有 ``PREFIX_DEFERRED`` 条目。块设备上的延迟写入
在原始 OSD 崩溃前已经完成物理写入（``_deferred_replay`` 处理的是
RocksDB 层面的清理操作），因此清除这些条目不会导致数据丢失。

**修复 3：设置 freelist_type = "null"（PREFIX_SUPER "S"）**

配合修复 1，将 ``freelist_type`` 设为 ``"null"`` 告知 BlueStore 不依赖
RocksDB 中的位图表，而是从磁盘上的对象元数据重建分配信息。

实现代码：

.. code-block:: cpp

   // Step 5b: 回放后修复——在 kv_db 上提交一个清理事务
   if (kv_db && opt.recover && r >= 0) {
     KeyValueDB::Transaction t = kv_db->get_transaction();
     t->rmkeys_by_prefix("b");  // PREFIX_ALLOC_BITMAP
     t->rmkeys_by_prefix("L");  // PREFIX_DEFERRED
     bufferlist bl;
     bl.append("null");
     t->set("S", "freelist_type", bl);  // PREFIX_SUPER
     kv_db->submit_transaction_sync(t);
   }

端到端恢复测试脚本
~~~~~~~~~~~~~~~~~~

``test-wal-recovery.sh`` 使用 6 个阶段验证完整恢复路径：

.. list-table::
   :header-rows: 1

   * - 阶段
     - 操作
     - 验证项
   * - PHASE 1: SETUP
     - 启动 3-OSD 集群，所有 OSD 启用 WAL bypass（per-OSD 隔离目录）
     - 集群 HEALTH_OK
   * - PHASE 2: WRITE DATA
     - 创建 replicated pool，写入 N 个对象并记录 MD5
     - 所有对象可读且校验和一致
   * - PHASE 3: CAPTURE & STOP
     - 停止集群，验证 per-OSD bypass 文件
     - 每个 OSD 目录下 WAL 文件存在且序号连续
   * - PHASE 4: SIMULATE CORRUPTION
     - 零化 OSD 0 block 设备前 64MB（BlueFS 区域）
     - OSD 0 无法正常启动
   * - PHASE 5: RECOVERY
     - ``ceph-bluestore-wal-replay --recover`` + Step 5b 后处理
     - 工具报告 SUCCESS，batches > 0
   * - PHASE 6: VERIFICATION
     - 集群启动 + 等待 HEALTH_OK + 数据读取
     - HEALTH_OK，3 OSD 全部 up，所有对象 MD5 一致

**Per-OSD 旁路目录隔离**

.. important::

   所有 OSD 必须使用各自独立的旁路目录。使用 Ceph 配置的 ``$id``
   元变量实现自动隔离：

   .. code-block:: ini

      bluerocks_wal_bypass_dir = /tmp/ceph-wal-bypass-test/osd$id

   如果多个 OSD 共用同一旁路目录，WAL 文件会混合交错。恢复时
   将混合文件回放到单个 OSD 会导致 Column Family 不匹配或元数据损坏，
   PG 永久卡在 peering/unknown 状态。

   在 ``vstart.sh`` 启动参数中传递时需转义 ``$`` 符号：
   ``-o 'bluerocks_wal_bypass_dir=/path/osd\$id'``

测试结果
~~~~~~~~

.. list-table::
   :header-rows: 1

   * - 测试规模
     - 结果
     - 关键指标
   * - 5 个对象
     - **ALL CHECKS PASSED**
     - HEALTH_OK, 3 OSD up, 全部 MD5 匹配
   * - 50 个对象
     - **ALL CHECKS PASSED**
     - HEALTH_OK, 3 OSD up, 全部 MD5 匹配

运行方法
--------

.. code-block:: bash

   cd build
   ninja ceph-bluestore-wal-replay

   # 运行端到端恢复测试
   bash ../qa/standalone/test-wal-recovery.sh [NUM_OBJECTS]

   # 运行 --recover 模式（独立使用）
   bin/ceph-bluestore-wal-replay \
     --wal-dir /path/to/bypass \
     --mode bluestore \
     --osd-path /var/lib/ceph/osd/ceph-0 \
     --recover \
     --conf /etc/ceph/ceph.conf

前置条件
--------

- OSD block 设备完好（数据区未损坏）
- 旁路 WAL 文件目录可访问且序号连续
- ``ceph.conf`` 可用（需提供与原集群匹配的 ``bluestore_rocksdb_cfs`` 配置）
- 已有 ``<osd-path>/fsid`` 文件（提供 OSD 身份标识）

已知限制
--------

1. **RTO**：纯 WAL 全量回放，恢复时间与 WAL 总量成正比。
   大规模集群可能达天级。待 PR-10 快照机制实现后可降至小时级。

2. **空间要求**：旁路 WAL 文件需持续保留直到恢复完成。
   需为旁路介质预留足够空间。

3. **配置一致性**：恢复时的 ``ceph.conf`` 必须包含与原集群一致的
   ``bluestore_rocksdb_cfs`` 配置，否则 Column Family 布局不匹配
   会导致回放失败。

4. **仅限 BlueFS 区域损坏**：``--recover`` 模式假设 block 设备上的
   数据区完好。如果数据区也损坏，恢复的元数据虽然正确，但对象数据
   本身不可读。需依赖副本或纠删码恢复。

调试经验与注意事项
------------------

开发过程中遇到的典型问题及解决思路，供后续维护参考。

BitmapFreelistManager assert 崩溃
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**症状**：OSD 启动时立即 coredump，日志中出现：

.. code-block:: text

   BitmapFreelistManager.cc: 387: FAILED
       ceph_assert(get_next_set_bit(enumerate_bl, 0) == 0)

**根因**：``mkfs()`` 创建的空位图与 WAL 回放写入的 ONode extend 映射不一致。
``_init_alloc()`` 中 ``BitmapFreelistManager`` 枚举位图时发现预期为空的
区域有已分配标记。

**排查路径**：
1. 查看 OSD 日志中 ``_init_alloc`` 附近的输出
2. 确认 ``freelist_type`` 是 ``"bitmap"`` 还是 ``"null"``
3. 检查 ``read_allocation_from_drive_on_startup`` 是否被调用

**修复**：在工具的 Step 5b 中清除 ``PREFIX_ALLOC_BITMAP`` 并设置
``freelist_type = "null"``。

AvlAllocator double-free 崩溃
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**症状**：OSD 启动后正常运行数秒，处理 PG peering 时 coredump：

.. code-block:: text

   AvlAllocator.cc: 115: FAILED
       ceph_assert(rs_before == range_tree.end() || rs_before->end <= start)

**根因**：``_deferred_replay()`` 处理 WAL 中残留的 ``PREFIX_DEFERRED``
条目，释放临时块。但因使用 ``NullFreelistManager`` 从 ONode 重建分配表，
这些临时块已经在 free tree 中 → 释放到已 free 的区间 → assert。

**排查路径**：
1. 在 OSD 日志中搜索 ``_deferred_replay`` 和 ``_txc_release_alloc``
2. 查看释放地址范围是否与 ``read_allocation_from_drive_on_startup``
   重建的 free tree 重叠
3. 检查 ``PREFIX_DEFERRED`` 条目数量

**修复**：在 Step 5b 中清除 ``PREFIX_DEFERRED`` 条目。

PG peering 卡死（per-OSD 旁路隔离问题）
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**症状**：恢复后 OSD 全部 up，但 PG 永久卡在 ``peering`` / ``unknown``
状态，超时后仍无法恢复：

.. code-block:: text

   23 peering, 12 unknown

**根因**：多个 OSD 共用同一旁路目录，WAL 文件来自不同 OSD 的交错写入。
恢复时将这些混合文件全部回放到 OSD 0，导致其 RocksDB 中包含
不属于自己的 PG 元数据，PG map 混乱。

**排查路径**：
1. 检查旁路目录下 ``state`` 文件内容，确认序号是否合理
2. 确认 ``bluerocks_wal_bypass_dir`` 配置是否包含 ``$id`` 变量
3. 检查旁路目录下是否有明显来自多个 OSD 的文件

**修复**：使用 ``bluerocks_wal_bypass_dir = /path/osd$id`` 配置，
确保每个 OSD 实例使用独立目录。

Bash 测试脚本中 ``set -e`` 与 ``(( ))`` 的陷阱
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**症状**：测试脚本在某些表达式求值为 0 时意外退出。

**根因**：Bash 的 ``set -e`` 模式下，``(( expr ))`` 当 ``expr``
求值为 0 时返回退出码 1，触发 ``set -e`` 终止脚本。

**修复**：使用 ``[[ $((expr)) ]]`` 或 ``(( expr )) || true`` 替代
独立的 ``(( expr ))``。测试脚本中的所有算术操作均应注意此问题。

NullFreelistManager 与 bluestore_allocation_from_file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

恢复流程依赖 ``NullFreelistManager`` 的 ONode 重建路径。该路径受配置项
``bluestore_allocation_from_file``（默认 ``true``）控制。

工作原理：OSD mount 时检测到 ``freelist_type = "null"``，调用
``read_allocation_from_drive_on_startup()``，遍历所有 ONode 的
extent 元数据来重建磁盘分配表。此过程不依赖 RocksDB 中的位图记录，
而是直接从对象元数据推导。

.. note::

   如果 ``bluestore_allocation_from_file`` 被显式设为 ``false``，
   OSD 将无法通过 ONode 重建分配表，导致所有块标记为"已使用"。
   生产环境中应确保此配置保持默认值。
