#!/bin/bash

# ============================================================
# HRAC 压缩算法实数据测试脚本
# 用法: ./real_test.sh [选项]
#   -s, --start-ceph    启动 Ceph 集群（默认关闭）
#   -t, --tmpdir        临时目录路径（默认 ./real_test_tmp）
#   -o, --object-name   写入对象名（默认 real-data.fits）
#   -h, --help          显示帮助信息
# ============================================================

set -e

# ============================================================
# 默认配置
# ============================================================
START_CEPH=false
TMPDIR="./real_test_tmp"
DATA_FILE="real-data.fits"       # 位于 ../build 下
OBJECT_NAME="real-data.fits"

# ============================================================
# 第一部分：参数解析函数
# ============================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -s|--start-ceph)
                START_CEPH=true
                shift
                ;;
            -t|--tmpdir)
                TMPDIR="$2"
                shift 2
                ;;
            -o|--object-name)
                OBJECT_NAME="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    echo "HRAC 压缩算法实数据测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -s, --start-ceph    启动 Ceph 集群（默认关闭）"
    echo "  -t, --tmpdir        临时目录路径（默认 ./real_test_tmp）"
    echo "  -o, --object-name   写入对象名（默认 real-data.fits）"
    echo "  -h, --help          显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                          # 使用默认配置"
    echo "  $0 -s -o myfits             # 启动Ceph，对象名为 myfits"
}

# ============================================================
# 工具函数
# ============================================================
log_info() {
    echo "[INFO] $1"
}

log_section() {
    echo ""
    echo "============================================================"
    echo " $1"
    echo "============================================================"
}

wait_confirm() {
    # read -p "[WAIT] $1，按 Enter 继续..."
    true
}

# ============================================================
# 第二部分：创建临时目录
# ============================================================
create_tmpdir() {
    log_section "创建临时目录"
    mkdir -p "$TMPDIR"
    log_info "临时目录: $TMPDIR"
    wait_confirm "临时目录创建完成"
}

# ============================================================
# 第三部分：启动 Ceph 集群（可选）
# ============================================================
create_config() {
  if [ "$START_CEPH" = true ]; then
    log_section "创建 Ceph 配置文件"
    # 在 build 目录内创建 ceph.conf
    cat <<EOF > ./ceph.conf
[global]
plugin dir = lib
erasure code dir = lib
EOF
    export CEPH_CONF="$(pwd)/ceph.conf"
    log_info "配置文件已创建: $CEPH_CONF"
  else
    log_info "跳过 创建 Ceph 配置文件（使用 -s 参数启用）"
  fi
}

start_ceph_cluster() {
    log_section "重启 Ceph 集群"
    
    if [ "$START_CEPH" = true ]; then
        log_info "正在关闭 Ceph 集群..."
        ../src/stop.sh
        create_config
        log_info "正在启动 Ceph 集群..."
        ../src/vstart.sh -d -n -x --localhost --bluestore
        wait_confirm "Ceph 集群重启完成"
        configure_compression
    else
        log_info "跳过 Ceph 集群重启（使用 -s 参数启用）"
    fi
}

# ============================================================
# 第四部分：配置 HRAC 压缩
# ============================================================
configure_compression() {
    log_section "配置 HRAC 压缩"
    log_info "设置 bluestore 压缩模式..."
    ./bin/ceph tell osd.* injectargs --bluestore_compression_mode=force
    log_info "设置 bluestore 压缩算法..."
    ./bin/ceph tell osd.* injectargs --bluestore_compression_algorithm=hrac
    # key: 8 KB -> 64 KB
    log_info "强制设置压缩块大小为 64KB..."
    ./bin/ceph tell osd.* injectargs --bluestore_compression_min_blob_size=65536
    ./bin/ceph tell osd.* injectargs --bluestore_compression_min_blob_size_hdd=65536
    ./bin/ceph tell osd.* injectargs --bluestore_compression_max_blob_size_hdd=65536

    wait_confirm "压缩配置完成"
}

configure_osd_limits() {
    log_section "配置 OSD 限制"
    log_info "设置 osd_max_object_size = 512MB ..."
    ./bin/ceph tell osd.* injectargs --osd_max_object_size=536870912
    wait_confirm "OSD 限制配置完成"
}

# ============================================================
# 第五部分：创建测试池
# ============================================================
create_test_pool() {
    log_section "创建测试池"
    log_info "删除已存在的 testpool（如果有）..."
    ./bin/ceph osd pool delete testpool testpool --yes-i-really-really-mean-it || true
    
    log_info "创建新的 testpool..."
    ./bin/ceph osd pool create testpool 16 16
    
    log_info "启用 rados 应用..."
    ./bin/ceph osd pool application enable testpool rados
    
    log_info "设置池压缩算法..."
    ./bin/ceph osd pool set testpool compression_algorithm hrac
    
    log_info "设置池压缩模式..."
    ./bin/ceph osd pool set testpool compression_mode force
    
    wait_confirm "测试池创建完成"
}

# ============================================================
# 第六部分：上传/下载/校验（单文件）
# ============================================================
upload_real_file() {
    if [ -f "$DATA_FILE" ]; then
        log_section "上传文件到 testpool"
        log_info "上传: $OBJECT_NAME <- $DATA_FILE"
        ./bin/rados -p testpool put "$OBJECT_NAME" "$DATA_FILE"
        log_info "上传完成"
        wait_confirm "文件上传完成"
    else
        echo "[ERROR] 未找到数据文件: $DATA_FILE"
        exit 1
    fi
}

download_real_file() {
    log_section "从 testpool 下载文件"
    log_info "下载: $OBJECT_NAME -> $TMPDIR/retrieved_$OBJECT_NAME"
    ./bin/rados -p testpool get "$OBJECT_NAME" "$TMPDIR/retrieved_$OBJECT_NAME"
    log_info "下载完成"
    wait_confirm "文件下载完成"
}

verify_integrity() {
    log_section "数据完整性校验"
    echo -n "  $OBJECT_NAME: "
    if diff -q "$DATA_FILE" "$TMPDIR/retrieved_$OBJECT_NAME" > /dev/null 2>&1; then
        echo "✓ PASS"
        log_info "所有文件校验通过！"
    else
        echo "✗ FAIL"
        log_info "警告: 完整性校验失败"
    fi
    wait_confirm "完整性校验完成"
}

# ============================================================
# 第七部分：压缩率与日志
# ============================================================
check_compression_ratio() {
    log_section "压缩率检测"
    
    log_info "等待数据刷盘..."
    sleep 2
    ./bin/ceph osd pool stats testpool 2>/dev/null || true
    
    echo ""
    local logpath="../hrac-script/test_logs"
    local logname="real_$(date +%Y%m%d_%H%M%S).txt"
    mkdir -p "$logpath"
    ./bin/ceph df detail > "$logpath/$logname"
    cat "$logpath/$logname"
    echo "log path: $logpath/$logname"
}

show_compression_logs() {
    local count=10
    log_section "HRAC 压缩日志"
    
    echo ""
    echo "=== 压缩日志 (后 $count 条) ==="
    grep -a -h "HRAC_DEBUG" out/osd.*.log 2>/dev/null | grep "\bcompress" | sort | tail -"$count" || echo "未找到压缩日志"
    
    echo ""
    echo "=== 解压日志 (后 $count 条) ==="
    grep -a -h "HRAC_DEBUG" out/osd.*.log 2>/dev/null | grep "\bdecompress" | sort | tail -"$count" || echo "未找到解压日志"
    
    echo ""
    echo "=== OSD.0 日志尾部 ==="
    tail out/osd.0.log 2>/dev/null -n $count || echo "未找到 OSD 日志"
}

# ============================================================
# 主函数
# ============================================================
main() {
    parse_args "$@"

    cd ../build
    
    log_section "HRAC 压缩算法实数据测试"
    log_info "配置参数:"
    log_info "  - 启动 Ceph: $START_CEPH"
    log_info "  - 临时目录: $TMPDIR"
    log_info "  - 数据文件: $DATA_FILE (相对于 build 目录)"
    log_info "  - 对象名称: $OBJECT_NAME"
    echo ""
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "[ERROR] 数据文件不存在: $(pwd)/$DATA_FILE"
        exit 1
    fi
    
    create_tmpdir
    start_ceph_cluster
    configure_osd_limits
    create_test_pool
    upload_real_file
    download_real_file
    verify_integrity
    check_compression_ratio
    show_compression_logs
    
    log_section "测试完成"
}

# 执行主函数
main "$@"
