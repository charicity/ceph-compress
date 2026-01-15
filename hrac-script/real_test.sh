#!/bin/bash

# ============================================================
# 压缩算法实数据测试脚本 (调用中间件版)
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
DATA_FILE="$(realpath "$SCRIPT_DIR/data/real-data.fits")"
OBJECT_NAME="test_object"
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
            -d|--data-file)
                DATA_FILE="$SCRIPT_DIR/data/$2"
                shift 2
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
    echo "压缩算法实数据测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -s, --start-ceph    启动 Ceph 集群（默认关闭）"
    echo "  -t, --tmpdir        临时目录路径（默认 ./real_test_tmp）"
    echo "  -o, --object-name   写入对象名（默认 real-data.fits）"
    echo "  -h, --help          显示帮助信息"
    echo "  -b, --block         设置压缩块大小（默认 64KB，单位字节）"
    echo "  -v, --verbose       实时显示输出到终端（默认关闭）"
    echo "  --simple-cluster    设置使用简易集群启动（默认关闭）"
    echo "  -com, --compressor  设置压缩算法（默认 hrac）"
    echo ""
    echo "示例:"
    echo "  $0                          # 使用默认配置"
    echo "  $0 -s -o myfits             # 启动Ceph，对象名为 myfits"
    echo "  $0 -v                       # 实时显示输出到终端"
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

# ============================================================
# 环境与集群设置函数 (保持不变)
# ============================================================
create_tmpdir() {
    log_section "创建临时目录"
    mkdir -p "$TMPDIR"
    log_info "临时目录: $TMPDIR"
}

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
        configure_verification
    else
        log_info "跳过 Ceph 集群重启"
    fi
}

configure_verification() {
    log_info "验证配置..."
    ./bin/ceph daemon osd.0 config show | grep -E "bluestore_compression|blob_size" 2>&1 | log_output
}

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
# 核心修改：调用中间件进行读写与校验
# ============================================================
run_rw_check() {
    local mode="$1" # --put 或 --get
    local msg="$2"
    
    log_section "$msg"
    
    # 构造命令参数

    local cmd=(
        "$RW_TOOL"
        "$mode"
        --pool "$POOL_NAME"
        --block "$BLOCK_SIZE"
        --object "$OBJECT_NAME"
        --input "$DATA_FILE"
        --tmp "$TMPDIR"
        --bin-dir "./bin"
    )

    if [ "$VERBOSE" = true ]; then
        cmd+=(--verbose)
    fi

    # 执行并记录日志
    log_info "执行指令: ${cmd[*]}"
    "${cmd[@]}" 2>&1 | log_output
    
    # 检查返回值
    if [ $? -eq 0 ]; then
        if [ "$mode" == "--get" ]; then
            log_result "$OBJECT_NAME: ✓ PASS (Integrity Verified)"
        fi
    else
        log_result "$OBJECT_NAME: ✗ FAIL (Operation Failed)"
        exit 1
    fi
}

# ============================================================
# 统计与日志函数 (保持不变)
# ============================================================
check_compression_ratio() {
    log_section "压缩率检测"

    # 等待统计
    sleep 5

    ./bin/ceph osd pool stats $POOL_NAME 2>&1 | log_output || true
    echo "" >> "$RUN_LOG_FILE"
    ./bin/ceph df detail 2>&1 | log_result
}

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
# 主函数
# ============================================================
main() {
    parse_args "$@"

    # 切换到 build 目录，不然 vstart 和 ceph 调用都会报错
    cd ../build
    
    # 初始化日志
    local run_log_path="../hrac-script/logs/run"
    mkdir -p "$run_log_path"
    RUN_LOG_FILENAME="real_$(date +%Y%m%d_%H%M%S).txt"
    RUN_LOG_FILE="$run_log_path/$RUN_LOG_FILENAME"
    
    log_section "HRAC 压缩算法真实数据测试"
    log_info "配置参数:"
    log_info "  - 启动 Ceph: $START_CEPH"
    log_info "  - 输入文件名: $DATA_FILE"
    log_info "  - 临时目录: $TMPDIR"
    log_info "  - 压缩块大小: $BLOCK_SIZE"
    log_info "  - 压缩算法: $COMPRESSOR"
    log_info "  - 详细输出: $VERBOSE"
    log_info "  - 运行日志: $RUN_LOG_FILE"
    
    if [ ! -f "$DATA_FILE" ]; then
        echo "[ERROR] 数据文件不存在: $(pwd)/$DATA_FILE"
        exit 1
    fi
    
    n "$@"
    create_tmpdir
    start_ceph_cluster
    create_test_pool
    
    # --- 修改点：调用中间件 ---
    run_rw_check "--put" "上传文件到 $POOL_NAME"
    run_rw_check "--get" "下载文件并校验完整性"
    # ------------------------
    
    check_compression_ratio
    show_compression_logs

    clean_tmp_dir
    
    log_section "测试完成"
}

main "$@"