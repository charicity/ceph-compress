PR-3 严格轮转与序号持久化测试留档
===================================

范围
----

本留档只保留本次纠正后的 3 OSD 实测流程与结果，不再沿用旧的错误测试路径。
本留档对应拆分后的 WAL bypass 实现路径，测试结论以该实现形态为准。

实现对应：``src/os/bluestore/WalBypassCapture.hpp``、
``src/os/bluestore/WalBypassCapture.cpp``（由
``src/os/bluestore/BlueRocksEnv.cc`` 引用）。

环境与配置
----------

- 仓库：``/home/lj/ceph``
- 构建目录：``/home/lj/ceph/build``
- 集群：``MON=1 OSD=3 MGR=1``
- 旁路目录：``/tmp/wal_bypass_pr3``
- 核心配置：

  - ``bluerocks_wal_bypass_enable=true``
  - ``bluerocks_wal_bypass_dir=/tmp/wal_bypass_pr3``
  - ``bluerocks_wal_rotate_size_mb=1024``
  - ``bluerocks_wal_rotate_interval_sec=2``
  - ``bluerocks_wal_flush_trigger_kb=4``
  - ``bluerocks_wal_flush_interval_ms=50``

最小复现流程
------------

1) 冷启动（必须）
^^^^^^^^^^^^^^^^^

.. code-block:: bash

   cd /home/lj/ceph/build
   ../src/stop.sh || true
   rm -rf dev out asok run
   rm -f ceph.conf keyring
   rm -rf /tmp/wal_bypass_pr3 && mkdir -p /tmp/wal_bypass_pr3

2) 启动时注入配置（不采用运行期临时下发）
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   MON=1 OSD=3 MGR=1 MDS=0 RGW=0 ../src/vstart.sh --new --localhost --bluestore -x \
     -o bluerocks_wal_bypass_enable=true \
     -o bluerocks_wal_bypass_dir=/tmp/wal_bypass_pr3 \
     -o bluerocks_wal_rotate_size_mb=1024 \
     -o bluerocks_wal_rotate_interval_sec=2 \
     -o bluerocks_wal_flush_trigger_kb=4 \
     -o bluerocks_wal_flush_interval_ms=50

3) 短压 + 长压
^^^^^^^^^^^^^

.. code-block:: bash

   ./bin/ceph osd pool create pr3test 8 8 || true
   ./bin/ceph osd pool application enable pr3test rados || true
   ./bin/rados -p pr3test bench 12 write -b 4096 --no-cleanup
   ./bin/rados -p pr3test bench 600 write -b 4096 --no-cleanup

4) 校验
^^^^^^^

.. code-block:: bash

   # 序号/state
   python3 - <<'PY'
   import os,re,glob
   p='/tmp/wal_bypass_pr3'
   files=sorted(glob.glob(os.path.join(p,'ceph_wal_*.log')))
   pat=re.compile(r'ceph_wal_(\d+)\.log$')
   seq=[int(m.group(1)) for f in files for m in [pat.search(os.path.basename(f))] if m]
   start,end=seq[0],seq[-1]
   missing=[n for n in range(start,end+1) if n not in set(seq)]
   with open(os.path.join(p,'ceph_wal_seq.state')) as fp:
       state=int(fp.read().strip())
   print('FILE_COUNT',len(seq))
   print('SEQ_START',start)
   print('SEQ_END',end)
   print('SEQ_CONTIGUOUS','YES' if not missing else 'NO')
   print('STATE_NEXT_SEQ',state)
   print('STATE_MATCH','YES' if state==end+1 else 'NO')
   PY

   # 稳定性
   ./bin/ceph -s
   ./bin/ceph osd tree
   grep -c "Caught signal (Segmentation fault)" out/osd.0.log
   grep -c "Caught signal (Segmentation fault)" out/osd.1.log
   grep -c "Caught signal (Segmentation fault)" out/osd.2.log

关键结果（本次）
----------------

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

判定
----

- ``SEQ_CONTIGUOUS=YES``；
- ``STATE_MATCH=YES``；
- 轮转持续发生且行为与时间窗一致（允许调度抖动）；
- 长压期间无 OSD 段错误，集群维持 ``3 up / 3 in`` 并回到 ``active+clean``。

经验摘要
--------

- 每轮必须冷启动，禁止复用脏环境；
- 配置必须在启动时注入，不以运行期临时下发为主路径；
- 验证要同时覆盖：功能正确 + 行为正确 + 稳定性正确。

收尾
----

.. code-block:: bash

   cd /home/lj/ceph/build
   ../src/stop.sh
