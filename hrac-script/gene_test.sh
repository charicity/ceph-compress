#!/bin/bash

# ============================================================
# 压缩算法测试脚本 (调用中间件版)
# ============================================================

set -e

# 获取当前脚本所在目录，以便定位 rw_check.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RW_TOOL="$SCRIPT_DIR/../rw-script/rw_check.sh"

# ============================================================
# 默认配置
# ============================================================
START_CEPH=false
TMPDIR=$SCRIPT_DIR/logs/tmpdata
DATA_SIZE="4M"
BLOCK_SIZE=16777216  # 16MB
VERBOSE=false
SIMPLE_CLUSTER=false
RUN_LOG_FILENAME=""
RUN_LOG_FILE=""
START_CONFIGS=""
COMPRESSOR="hrac"
POOL_NAME="test_compressor_pool"

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
            -b|--block)
                BLOCK_SIZE="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            --simple-cluster)
                SIMPLE_CLUSTER=true
                shift
                ;;
            -com|--compressor)
                COMPRESSOR="$2"
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

    START_CONFIGS=(
        -o "osd_max_object_size = 536870912"
        -o "bluestore_max_blob_size = $BLOCK_SIZE"
    )
}

show_help() {
    echo "HRAC 压缩算法测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -s, --start-ceph    启动 Ceph 集群（默认关闭）"
    echo "  -d, --data-size     数据大小，如 4M, 16M, 1G（默认 4M）"
    echo "  -t, --tmpdir        临时目录路径（默认 ./test_gen_data）"
    echo "  -b, --block         设置压缩块大小（默认 16MB，单位字节）"
    echo "  -v, --verbose       实时显示输出到终端（默认关闭）"
    echo "  --simple-cluster    设置使用简易集群启动（默认关闭）"
    echo "  -com, --compressor  设置压缩算法（默认 hrac）"
    echo "  -h, --help          显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                          # 使用默认配置"
    echo "  $0 -s -d 16M                # 启动Ceph，使用16M数据"
    echo "  $0 --data-size 1G -v        # 使用1G数据量测试，实时显示输出"
}

# ============================================================
# 工具函数
# ============================================================
log_output() {
    if [ "$VERBOSE" = true ]; then
        tee -a "$RUN_LOG_FILE"
    else
        cat >> "$RUN_LOG_FILE"
    fi
}

log_info() {
    echo "[INFO] $1" | tee -a "$RUN_LOG_FILE"
}

log_section() {
    {
        echo ""
        echo "============================================================"
        echo " $1"
        echo "============================================================"
    } | tee -a "$RUN_LOG_FILE"
}

log_result() {
    local result_log_path="$SCRIPT_DIR/logs/result"
    mkdir -p "$result_log_path"
    if [ -n "$1" ]; then
        echo "$1" | tee -a "$result_log_path/$RUN_LOG_FILENAME" | log_output
    else
        tee -a "$result_log_path/$RUN_LOG_FILENAME" | log_output
    fi
}

wait_confirm() {
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

start_ceph_cluster() {
    log_section "重启 Ceph 集群"
    if [ "$START_CEPH" = true ]; then
        log_info "正在关闭 Ceph 集群..."
        ../src/stop.sh 2>&1 | log_output || true
        
        log_info "正在启动 Ceph 集群..."
        if [ "$SIMPLE_CLUSTER" = true ]; then
            MON=1 OSD=1 MDS=0 MGR=1 ../src/vstart.sh -d -n -x --without-dashboard \
            "${START_CONFIGS[@]}" 2>&1 | log_output
        else
            ../src/vstart.sh -d -n -x --without-dashboard \
            "${START_CONFIGS[@]}" 2>&1 | log_output
        fi
        configure_compression
    else
        log_info "跳过 Ceph 集群重启"
    fi
}

# ============================================================
# 第四部分：配置 HRAC 压缩
# ============================================================
configure_compression() {
    log_info "验证配置..."
    ./bin/ceph daemon osd.0 config show | grep -E "bluestore_compression|blob_size" 2>&1 | log_output
}

# ============================================================
# 第五部分：创建测试池
# ============================================================
create_test_pool() {
    log_section "创建测试池"
    ./bin/ceph osd pool delete $POOL_NAME $POOL_NAME --yes-i-really-really-mean-it 2>&1 | log_output || true
    ./bin/ceph osd pool create $POOL_NAME 16 16 2>&1 | log_output
    ./bin/ceph osd pool application enable $POOL_NAME rados 2>&1 | log_output
    
    log_section "设置池的压缩参数"
    ./bin/ceph osd pool set $POOL_NAME compression_algorithm $COMPRESSOR 2>&1 | log_output
    ./bin/ceph osd pool set $POOL_NAME compression_mode force 2>&1 | log_output
    ./bin/ceph osd pool set $POOL_NAME compression_min_blob_size "$BLOCK_SIZE" 2>&1 | log_output
    ./bin/ceph osd pool set $POOL_NAME compression_max_blob_size "$BLOCK_SIZE" 2>&1 | log_output
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
# 第七部分：使用中间件进行读写与校验
# ============================================================
run_rw_check() {
    local mode="$1" # --put 或 --get
    local filename="$2"
    local msg="$3"
    
    log_section "$msg"
    
    # 构造命令参数
    local cmd=(
        "$RW_TOOL"
        "$mode"
        --pool "$POOL_NAME"
        --block "$BLOCK_SIZE"
        --object "$filename"
        --input "$TMPDIR/$filename"
        --tmp "$TMPDIR"
        --bin-dir "./bin"
    )

    if [ "$VERBOSE" = true ]; then
        cmd+=(--verbose)
    fi

    # 执行并记录日志
    log_info "执行指令: ${cmd[*]}"
    "${cmd[@]}" 2>&1 | log_output; local exit_code=$?
    
    # 检查返回值
    if [ $exit_code -eq 0 ]; then
        if [ "$mode" == "--get" ]; then
            log_result "$filename: ✓ PASS (Integrity Verified)"
        fi
    else
        log_result "$filename: ✗ FAIL (Operation Failed)"
        return 1
    fi
}

upload_file() {
    local filename="$1"
    if [ -f "$TMPDIR/$filename" ]; then
        run_rw_check "--put" "$filename" "上传文件: $filename"
    fi
}

upload_files() {
    log_section "上传文件到 $POOL_NAME"
    
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
    run_rw_check "--get" "$filename" "下载并校验文件: $filename"
}

download_files() {
    log_section "从 $POOL_NAME 下载文件并校验"
    
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
# 注意：完整性校验已在 run_rw_check 的 --get 模式中完成


# ============================================================
# 第十一部分：查看压缩日志
# ============================================================
show_compression_logs() {
    local count=20
    log_section "HRAC 压缩日志"
    
    {
        echo ""
        echo "=== 压缩日志 (后 $count 条) ==="
        grep -a -h "HRAC_DEBUG" out/osd.*.log 2>/dev/null | grep -E "\bcompress\(\)" | sort | tail -"$count" || echo "未找到压缩日志"
        
        echo ""
        echo "=== 解压日志 (后 $count 条) ==="
        grep -a -h "HRAC_DEBUG" out/osd.*.log 2>/dev/null | grep -E "\bdecompress\(\)" | sort | tail -"$count" || echo "未找到解压日志"

        echo ""
        echo "=== Hrac codec ERROR 日志 (后 $count 条) ==="
        grep -a -h -F "HRAC_ERROR" out/osd.*.log 2>/dev/null | grep -E "\bfits_kcomp_u8\(\)" | sort | tail -"$count" || echo "未找到 Hrac 日志"

        echo ""
        echo "=== Hrac codec DEBUG 日志 (后 $count 条) ==="
        grep -a -h -F "HRAC_DEBUG" out/osd.*.log 2>/dev/null | grep -E "fits_kcomp_u8\(\)\bcompleted" | sort | tail -"$count" || echo "未找到 Hrac 日志"

        echo ""
        echo "=== OSD.0 日志尾部 ==="
        tail out/osd.0.log 2>/dev/null -n $count || echo "未找到 OSD 日志"
    } | log_output
}

n() {
    local cmd
    cmd="$(printf '%q ' "$0" "$@")"
    log_result "CMD: ${cmd% }"
}

clean_tmp_dir() {
    log_section "清理临时目录"
    rm -rf "$TMPDIR"
    log_info "已删除临时目录: $TMPDIR"
}

# ============================================================
# 第十二部分：压缩率检测
# ============================================================
check_compression_ratio() {
    log_section "压缩率检测"
    
    # 等待统计
    sleep 5

    ./bin/ceph osd pool stats $POOL_NAME 2>&1 | log_output || true
    echo "" >> "$RUN_LOG_FILE"
    ./bin/ceph df detail 2>&1 | log_result
}

# ============================================================
# 主函数
# ============================================================
main() {
    parse_args "$@"

    # 切换到 build 目录，不然 vstart 和 ceph 调用都会报错
    cd ../build
    
    # 初始化日志
    local run_log_path="../hrac-script/logs/run"
    mkdir -p "$run_log_path"
    RUN_LOG_FILENAME="gene_$(date +%Y%m%d_%H%M%S).txt"
    RUN_LOG_FILE="$run_log_path/$RUN_LOG_FILENAME"
    
    log_section "HRAC 压缩算法测试"
    log_info "配置参数:"
    log_info "  - 启动 Ceph: $START_CEPH"
    log_info "  - 数据大小: $DATA_SIZE"
    log_info "  - 临时目录: $TMPDIR"
    log_info "  - 压缩块大小: $BLOCK_SIZE"
    log_info "  - 压缩算法: $COMPRESSOR"
    log_info "  - 详细输出: $VERBOSE"
    log_info "  - 运行日志: $RUN_LOG_FILE"
    echo ""
    
    n "$@"
    create_tmpdir
    start_ceph_cluster
    create_test_pool
    generate_test_data
    upload_files
    download_files
    check_compression_ratio
    show_compression_logs
    clean_tmp_dir
    
    log_section "测试完成"
}

# 执行主函数
main "$@"