#!/bin/bash

# ============================================================
# HRAC 压缩算法测试脚本
# 用法: ./test_hrac.sh [选项]
#   -s, --start-ceph    启动 Ceph 集群（默认关闭）
#   -d, --data-size     数据大小，如 4M, 16M, 1G（默认 4M）
#   -t, --tmpdir        临时目录路径（默认 ./tmp）
#   -h, --help          显示帮助信息
# ============================================================

set -e

# ============================================================
# 默认配置
# ============================================================
START_CEPH=false
DATA_SIZE="4M"
TMPDIR="./test_gen_data"

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
            -d|--data-size)
                DATA_SIZE="$2"
                shift 2
                ;;
            -t|--tmpdir)
                TMPDIR="$2"
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
    echo "HRAC 压缩算法测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -s, --start-ceph    启动 Ceph 集群（默认关闭）"
    echo "  -d, --data-size     数据大小，如 4M, 16M, 1G（默认 4M）"
    echo "  -t, --tmpdir        临时目录路径（默认 ./tmp）"
    echo "  -h, --help          显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                          # 使用默认配置"
    echo "  $0 -s -d 16M                # 启动Ceph，使用16M数据"
    echo "  $0 --data-size 1G           # 使用1G数据量测试"
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

# 将大小字符串转换为字节数（用于计算）
size_to_bytes() {
    local size="$1"
    local num="${size%[KMGkmg]*}"
    local unit="${size##*[0-9]}"
    
    case "$unit" in
        K|k) echo $((num * 1024)) ;;
        M|m) echo $((num * 1024 * 1024)) ;;
        G|g) echo $((num * 1024 * 1024 * 1024)) ;;
        *)   echo "$num" ;;
    esac
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
    cat <<EOF > ../build/ceph.conf
[global]
plugin dir = lib
erasure code dir = lib
EOF
    export CEPH_CONF=../build/ceph.conf
    log_info "配置文件已创建: ../build/ceph.conf"
    log_info "CEPH_CONF=$CEPH_CONF"
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
    
    wait_confirm "压缩配置完成"
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
# 第六部分：生成测试数据
# ============================================================
generate_high_compressible() {
    log_info "生成高度可压缩文件 (全A): high_A_${DATA_SIZE}"
    yes A | head -c "$DATA_SIZE" > "$TMPDIR/high_A_${DATA_SIZE}"
}

generate_random_data() {
    log_info "生成随机不可压缩文件: random_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local mb=$((bytes / 1024 / 1024))
    if [ "$mb" -eq 0 ]; then mb=1; fi
    dd if=/dev/urandom of="$TMPDIR/random_${DATA_SIZE}" bs=1M count="$mb" status=progress 2>/dev/null
    truncate -s "$DATA_SIZE" "$TMPDIR/random_${DATA_SIZE}"
}

generate_small_files_tar() {
    log_info "生成小文件打包: many_small.tar"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local file_count=$((bytes / 512))
    
    mkdir -p "$TMPDIR/many_small"
    seq 1 "$file_count" | xargs -P4 -I{} sh -c 'head -c 512 </dev/zero > "'"$TMPDIR"'/many_small/f{}"'
    tar -C "$TMPDIR" -cf "$TMPDIR/many_small.tar" many_small
    rm -rf "$TMPDIR/many_small"
}

generate_csv_data() {
    log_info "生成 CSV 结构化文本: repeat_csv_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local lines=$((bytes / 40))
    
    awk -v n="$lines" 'BEGIN{for(i=1;i<=n;i++) print "user"i",email"i"@example.com,info"}' > "$TMPDIR/repeat_csv_${DATA_SIZE}"
    truncate -s "$DATA_SIZE" "$TMPDIR/repeat_csv_${DATA_SIZE}"
}

generate_alternating_pattern() {
    log_info "生成交替 01 模式: alt_01_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local pairs=$((bytes / 2))
    
    awk -v n="$pairs" 'BEGIN{for(i=0;i<n;i++) printf "01"}' > "$TMPDIR/alt_01_${DATA_SIZE}"
}

generate_binary_pattern() {
    log_info "生成模式二进制: pattern_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local repeats=$((bytes / 256))
    
    perl -e 'print pack("C*", (0..255)) x '"$repeats" > "$TMPDIR/pattern_${DATA_SIZE}"
}

generate_zeros() {
    log_info "生成全零文件: zeros_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local mb=$((bytes / 1024 / 1024))
    if [ "$mb" -eq 0 ]; then mb=1; fi
    dd if=/dev/zero of="$TMPDIR/zeros_${DATA_SIZE}" bs=1M count="$mb" status=progress 2>/dev/null
    truncate -s "$DATA_SIZE" "$TMPDIR/zeros_${DATA_SIZE}"
}

generate_mixed_data() {
    log_info "生成混合内容文件: mixed_${DATA_SIZE}"
    local bytes
    bytes=$(size_to_bytes "$DATA_SIZE")
    local half=$((bytes / 2))
    local half_mb=$((half / 1024 / 1024))
    if [ "$half_mb" -eq 0 ]; then half_mb=1; fi
    
    yes B | head -c "$half" > "$TMPDIR/mixed_${DATA_SIZE}"
    dd if=/dev/urandom bs=1M count="$half_mb" status=none >> "$TMPDIR/mixed_${DATA_SIZE}"
    truncate -s "$DATA_SIZE" "$TMPDIR/mixed_${DATA_SIZE}"
}

generate_test_data() {
    log_section "生成测试数据 (大小: $DATA_SIZE)"
    
    generate_high_compressible
    generate_random_data
    # generate_small_files_tar
    generate_csv_data
    generate_alternating_pattern
    generate_binary_pattern
    generate_zeros
    generate_mixed_data
    
    log_info ""
    log_info "=== 生成的测试文件 ==="
    ls -lh "$TMPDIR"
    
    wait_confirm "测试数据生成完成"
}

# ============================================================
# 第七部分：上传文件到池
# ============================================================
upload_file() {
    local filename="$1"
    if [ -f "$TMPDIR/$filename" ]; then
        log_info "上传: $filename"
        ./bin/rados -p testpool put "$filename" "$TMPDIR/$filename"
    fi
}

upload_files() {
    log_section "上传文件到 testpool"
    
    upload_file "high_A_${DATA_SIZE}"
    upload_file "random_${DATA_SIZE}"
    # upload_file "many_small.tar"
    upload_file "repeat_csv_${DATA_SIZE}"
    upload_file "alt_01_${DATA_SIZE}"
    upload_file "pattern_${DATA_SIZE}"
    upload_file "zeros_${DATA_SIZE}"
    upload_file "mixed_${DATA_SIZE}"
    
    log_info "上传完成"
    wait_confirm "文件上传完成"
}

# ============================================================
# 第八部分：下载文件验证
# ============================================================
download_file() {
    local filename="$1"
    log_info "下载: $filename"
    echo "./bin/rados -p testpool get \"$filename\" \"$TMPDIR/retrieved_$filename\""
    ./bin/rados -p testpool get "$filename" "$TMPDIR/retrieved_$filename"
}

download_files() {
    log_section "从 testpool 下载文件"
    
    download_file "high_A_${DATA_SIZE}"
    download_file "random_${DATA_SIZE}"
    # download_file "many_small.tar"
    download_file "repeat_csv_${DATA_SIZE}"
    download_file "alt_01_${DATA_SIZE}"
    download_file "pattern_${DATA_SIZE}"
    download_file "zeros_${DATA_SIZE}"
    download_file "mixed_${DATA_SIZE}"
    
    log_info "下载完成"
    wait_confirm "文件下载完成"
}

# ============================================================
# 第九部分：数据完整性校验
# ============================================================
verify_file() {
    local filename="$1"
    echo -n "  $filename: "
    if diff -q "$TMPDIR/$filename" "$TMPDIR/retrieved_$filename" > /dev/null 2>&1; then
        echo "✓ PASS"
        return 0
    else
        echo "✗ FAIL"
        return 1
    fi
}

verify_integrity() {
    log_section "数据完整性校验"
    
    local failed=0
    
    verify_file "high_A_${DATA_SIZE}" || ((failed++))
    verify_file "random_${DATA_SIZE}" || ((failed++))
    # verify_file "many_small.tar" || ((failed++))
    verify_file "repeat_csv_${DATA_SIZE}" || ((failed++))
    verify_file "alt_01_${DATA_SIZE}" || ((failed++))
    verify_file "pattern_${DATA_SIZE}" || ((failed++))
    verify_file "zeros_${DATA_SIZE}" || ((failed++))
    verify_file "mixed_${DATA_SIZE}" || ((failed++))
    
    echo ""
    if [ "$failed" -eq 0 ]; then
        log_info "所有文件校验通过！"
    else
        log_info "警告: $failed 个文件校验失败"
    fi
    
    wait_confirm "完整性校验完成"
}


# ============================================================
# 第十一部分：查看压缩日志
# ============================================================
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
# 第十二部分：压缩率检测
# ============================================================

# 主压缩率检测函数
check_compression_ratio() {
    log_section "压缩率检测"
    
    log_info "等待数据刷盘..."
    sleep 2
    ./bin/ceph osd pool stats testpool 2>/dev/null || true
    
    echo ""
    local logpath="../hrac-script/test_logs"
    local logname="gene_$(date +%Y%m%d_%H%M%S).txt"
    mkdir -p "$logpath"
    ./bin/ceph df detail > "$logpath/$logname"
    cat "$logpath/$logname"
    echo "log path: $logpath/$logname"
}

# ============================================================
# 主函数
# ============================================================
main() {
    parse_args "$@"

    cd ../build
    
    log_section "HRAC 压缩算法测试"
    log_info "配置参数:"
    log_info "  - 启动 Ceph: $START_CEPH"
    log_info "  - 数据大小: $DATA_SIZE"
    log_info "  - 临时目录: $TMPDIR"
    echo ""
    
    create_tmpdir
    start_ceph_cluster
    create_test_pool
    generate_test_data
    upload_files
    # check_compression_ratio
    download_files
    verify_integrity
    check_compression_ratio
    # generate_compression_report
    show_compression_logs
    # cleanup
    
    log_section "测试完成"
}

# 执行主函数
main "$@"
