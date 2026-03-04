PR-3 严格轮转与序号持久化测试留档
===================================

范围
----

本留档覆盖 PR-3 的运行验证：

- 旁路文件按 ``ceph_wal_<seq>.log`` 严格递增命名；
- 轮转支持“大小/时间”``or`` 触发，本次重点验证“按时轮转”；
- ``ceph_wal_seq.state`` 持久化 ``next_seq``，与文件序号一致。
- 在 ``MON=1 OSD=3 MGR=1`` 场景下，验证长时间写压稳定性（无 OSD 段错误）。

本次不覆盖回放工具（PR-5）与回放审计（PR-7）。

实现对应
--------

代码位置：

- ``src/os/bluestore/BlueRocksEnv.cc``

本次验证关注点：

- ``WalBypassCapture`` 的轮转状态机；
- ``ceph_wal_seq.state`` 的持久化与续号逻辑；
- ``rotate_interval_sec`` 极小值下的稳定行为。
- 修复后在 3 OSD 场景的并发稳定性（重点关注 ``bstore_kv_sync`` 路径）。

验证环境
--------

- 仓库：``/home/lj/ceph``
- 构建目录：``/home/lj/ceph/build``
- 集群：本地 ``vstart``，``MON=1 OSD=3 MGR=1``
- 旁路目录：``/tmp/wal_bypass_pr3``
- 关键配置：

  - ``bluerocks_wal_bypass_enable=true``
  - ``bluerocks_wal_bypass_dir=/tmp/wal_bypass_pr3``
  - ``bluerocks_wal_rotate_size_mb=1024``
  - ``bluerocks_wal_rotate_interval_sec=2``
  - ``bluerocks_wal_flush_trigger_kb=4``
  - ``bluerocks_wal_flush_interval_ms=50``

验证步骤与命令
--------------

1. 启动最小集群并注入极小时间轮转
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   cd /home/lj/ceph/build
   rm -rf /tmp/wal_bypass_pr3 && mkdir -p /tmp/wal_bypass_pr3

   MON=1 OSD=3 MGR=1 MDS=0 RGW=0 ../src/vstart.sh --new --localhost --bluestore -x \
     -o bluerocks_wal_bypass_enable=true \
     -o bluerocks_wal_bypass_dir=/tmp/wal_bypass_pr3 \
     -o bluerocks_wal_rotate_size_mb=1024 \
     -o bluerocks_wal_rotate_interval_sec=2 \
     -o bluerocks_wal_flush_trigger_kb=4 \
     -o bluerocks_wal_flush_interval_ms=50

   ./bin/ceph -s

2. 施加写入负载触发 WAL 持续产生
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   cd /home/lj/ceph/build
   ./bin/ceph osd pool create benchpool 8 8 || true
   ./bin/ceph osd pool application enable benchpool rados || true
   ./bin/rados -p benchpool bench 12 write --no-cleanup
   sleep 3

3. 检查旁路文件列表
^^^^^^^^^^^^^^^^^

.. code-block:: bash

   ls -l /tmp/wal_bypass_pr3

样例输出（节选）：

.. code-block:: text

   -rw-r--r-- 1 lj lj      307 ... ceph_wal_0000000001.log
   -rw-r--r-- 1 lj lj     5097 ... ceph_wal_0000000002.log
   -rw-r--r-- 1 lj lj   231603 ... ceph_wal_0000000003.log
   ...
   -rw-r--r-- 1 lj lj   206926 ... ceph_wal_0000000013.log
   -rw-r--r-- 1 lj lj       19 ... ceph_wal_0000000014.log
   -rw-r--r-- 1 lj lj        3 ... ceph_wal_seq.state

4. 自动校验序号连续与按时轮转证据
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   python3 - <<'PY'
   import os,re,glob
   p='/tmp/wal_bypass_pr3'
   files=sorted(glob.glob(os.path.join(p,'ceph_wal_*.log')))
   pat=re.compile(r'ceph_wal_(\d+)\.log$')
   seq=[]
   for f in files:
       m=pat.search(os.path.basename(f))
       if m:
           seq.append((int(m.group(1)),f))

   nums=[n for n,_ in seq]
   start,end=nums[0],nums[-1]
   missing=[n for n in range(start,end+1) if n not in set(nums)]

   state_path=os.path.join(p,'ceph_wal_seq.state')
   with open(state_path) as fp:
       state=int(fp.read().strip())

   rows=[]
   for n,f in seq:
       st=os.stat(f)
       rows.append((n,st.st_size,st.st_mtime))
   diffs=[rows[i][2]-rows[i-1][2] for i in range(1,len(rows))]
   within=[d for d in diffs if 1.5<=d<=3.5]

   print('FILE_COUNT',len(seq))
   print('SEQ_START',start)
   print('SEQ_END',end)
   print('SEQ_CONTIGUOUS', 'YES' if not missing else 'NO')
   print('STATE_NEXT_SEQ',state)
   print('STATE_MATCH', 'YES' if state==end+1 else 'NO')
   print('ROTATE_DIFF_COUNT',len(diffs))
   print('DIFF_IN_2S_WINDOW',len(within))
   print('MIN_DIFF',min(diffs))
   print('MAX_DIFF',max(diffs))
   print('AVG_DIFF',sum(diffs)/len(diffs))
   PY

样例输出（本次实测）：

.. code-block:: text

   FILE_COUNT 24
   SEQ_START 1
   SEQ_END 24
   SEQ_CONTIGUOUS YES
   STATE_NEXT_SEQ 25
   STATE_MATCH YES
   ROTATE_DIFF_COUNT 23
   DIFF_IN_2S_WINDOW 19
   MIN_DIFF 0.5279932022094727
   MAX_DIFF 17.43177628517151
   AVG_DIFF 3.3590873531673267

5. （补充）稳定性回归：长时间写压
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

在一次 3 OSD 验证中，曾观察到 ``osd.0`` 进程段错误（``Segmentation fault``），
栈落在 ``BlueRocksWritableFile::Append`` / ``WalBypassCapture`` 路径。

定位后确认是并发访问问题：

- worker 线程在 ``flush_loop()`` 中未持锁执行 ``rotate_stream()``，会改写
  ``m_current_stream``；
- 前台写线程在 ``append()`` 中持锁访问同一对象；
- 造成 ``m_current_stream`` 生命周期并发竞争，触发段错误。

修复方式：

- 在 ``src/os/bluestore/BlueRocksEnv.cc`` 中，调整 ``flush_loop()``，
  保证对 ``m_current_stream`` 的写入与轮转操作在同一把锁保护下进行。

修复后执行 600 秒写压：

.. code-block:: bash

   cd /home/lj/ceph/build
   ./bin/rados -p pr3test bench 600 write -b 4096 --no-cleanup

并复核：

.. code-block:: bash

   ./bin/ceph -s
   ./bin/ceph osd tree
   grep -c "Caught signal (Segmentation fault)" out/osd.0.log
   grep -c "Caught signal (Segmentation fault)" out/osd.1.log
   grep -c "Caught signal (Segmentation fault)" out/osd.2.log

本次实测（修复后）摘要：

.. code-block:: text

   rados bench 600s: exit code 0
   Bandwidth (MB/sec): 2.93947
   Average IOPS: 752
   Average Latency(s): 0.0212585

   osd tree: 3 up / 3 in
   segfault count: osd.0=0, osd.1=0, osd.2=0

   FILE_COUNT 1269
   SEQ_START 1
   SEQ_END 1269
   SEQ_CONTIGUOUS YES
   STATE_NEXT_SEQ 1270
   STATE_MATCH YES

判定标准
--------

通过条件：

1. 序号连续
^^^^^^^^^^^

- 目录内 ``ceph_wal_<seq>.log`` 序号无缺口、无回退；
- 满足 ``SEQ_CONTIGUOUS=YES``。

2. 序号持久化一致
^^^^^^^^^^^^^^^^^

- ``ceph_wal_seq.state`` 值等于当前最大文件序号 + 1；
- 满足 ``STATE_MATCH=YES``。

3. 按时轮转生效
^^^^^^^^^^^^^^^

- 在 ``rotate_size_mb`` 设置为大值（1GiB）时，单文件大小远小于阈值；
- 仍出现持续新增文件，并且相邻文件 mtime 大量落在配置时间窗口附近（本次为约 2s）。

4. 并发稳定性（补充）
^^^^^^^^^^^^^^^^^^^^^^

- 在 3 OSD 场景下执行中/长时写压（建议至少 10 分钟）期间：

   - 不出现 ``osd`` 进程段错误；
   - 集群维持 ``3 up / 3 in``（允许短时恢复过程，但最终应回到 ``active+clean``）。

说明：

- 个别间隔可能偏离 2s，通常由负载突发、调度抖动或停写阶段导致；
- 只要多数间隔命中时间窗口且序号连续，即可判定“按时轮转有效”。
- ``DIFF_IN_2S_WINDOW`` 不应作为唯一硬阈值，应结合连续写入阶段、
   文件持续增长和序号连续性综合判断。
- 压测中若出现 ``BlueFS spillover`` 告警，属于容量/布局信号，
   不等同于本项 WAL 旁路功能失败，但需在结论中显式记录。

结果结论
--------

本次 PR-3 验证通过（含补充稳定性回归）：

- 文件序号严格连续（短压样例 1..24；长压样例 1..1269）；
- ``state`` 与最大序号关系正确（短压 25 = 24 + 1；长压 1270 = 1269 + 1）；
- 在 2 秒轮转配置下，持续观察到时间驱动轮转行为；
- 3 OSD、600 秒写压后未再复现 ``osd.0`` 段错误，集群保持 ``3 up / 3 in``。

过程与心得摘要
--------------

1. 先修“崩溃根因”再做长压
^^^^^^^^^^^^^^^^^^^^^^^^^^

- 若仅做短压通过，无法覆盖并发窗口；
- 先通过崩溃栈定位并修复并发竞争，再做长压更能反映真实稳定性。

2. 轮转验证要分层看
^^^^^^^^^^^^^^^^^^^

- 一层看功能正确性：``SEQ_CONTIGUOUS``、``STATE_MATCH``；
- 一层看行为特征：时间窗口内命中比例、文件持续新增；
- 一层看系统稳定性：OSD 进程是否稳定、集群是否回到 ``active+clean``。

3. 判定标准避免单指标化
^^^^^^^^^^^^^^^^^^^^^^^

- ``mtime`` 间隔会受调度和负载影响，不能用单一阈值一票否决；
- 结合序号连续性、状态文件一致性和长压稳定性更稳妥。

收尾
----

如需释放环境，执行：

.. code-block:: bash

   cd /home/lj/ceph/build
   ../src/stop.sh

本次留档更新时，集群仍在运行用于后续回归；如不再继续测试，
建议按上面命令手动停止。
