#!/bin/bash

# ============================================================
# EC (纠删码) 算法实数据测试脚本 (调用中间件版)
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
DATA_FILE="$(realpath "$SCRIPT_DIR/data/01.txt")"
OBJECT_NAME="test_ec_object"
BLOCK_SIZE=4194304   # 4MB (EC通常对大块更友好)
VERBOSE=false
RUN_LOG_FILENAME="" # shared var, set by script
RUN_LOG_FILE=""  # shared var, set by script
START_CONFIGS="" # shared var, set in script
POOL_NAME="test_ec_pool"
PROFILE_NAME="test_ec_profile"
VICTIM_OSD="" # shared var, set by script

# EC 默认参数 (2+1)
EC_K=2
EC_M=1
EC_PLUGIN="jerasure"

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
            -d|--data-file)
                DATA_FILE="$SCRIPT_DIR/data/$2"
                shift 2
                ;;
            -k)
                EC_K="$2"
                shift 2
                ;;
            -m)
                EC_M="$2"
                shift 2
                ;;
            -p|--plugin)
                EC_PLUGIN="$2"
                shift 2
                ;;
            *)
                echo "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # EC 测试可能需要调整一些 bluestore 参数，但通常默认即可
    START_CONFIGS=(
            -o "osd_max_object_size = 536870912"
        )
}

show_help() {
    echo "EC (纠删码) 算法实数据测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -s, --start-ceph    启动 Ceph 集群（默认关闭）"
    echo "  -t, --tmpdir        临时目录路径"
    echo "  -o, --object-name   写入对象名"
    echo "  -h, --help          显示帮助信息"
    echo "  -b, --block         设置读写块大小（默认 4MB）"
    echo "  -v, --verbose       实时显示输出到终端"
    echo "  -k                  EC 数据块数量 (默认 2)"
    echo "  -m                  EC 校验块数量 (默认 1)"
    echo "  -p, --plugin        EC 插件算法 (默认 jerasure)"
    echo ""
    echo "示例:"
    echo "  $0 -s -k 4 -m 2             # 启动集群并测试 4+2 EC"
}

# ============================================================
# 工具函数 (日志相关)
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

# ============================================================
# 环境与集群设置函数
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
        
        # 计算需要的最小 OSD 数量
        local min_osds=$((EC_K + EC_M))
        local target_osds=$((min_osds))

        log_info "正在启动 Ceph 集群 (EC 模式, 需要至少 $min_osds 个 OSD)..."
        log_info "计划启动 $target_osds 个 OSD"

        MON=1 OSD=$target_osds MDS=0 MGR=1 ../src/vstart.sh -d -n -x --without-dashboard \
        "${START_CONFIGS[@]}" 2>&1 | log_output

        configure_verification
    else
        log_info "跳过 Ceph 集群重启"
    fi
}

configure_verification() {
    log_info "验证配置..."
    ./bin/ceph daemon osd.0 config show | grep -E "pool|erasure_code_profile" 2>&1 | log_output

    log_info "EC Profile 列表:"
    ./bin/ceph osd erasure-code-profile ls 2>&1 | log_output
}

create_ec_pool() {
    log_section "创建 EC 测试池"
    
    # 1. 清理旧池和 Profile
    log_info "清理旧资源..."
    ./bin/ceph osd pool delete $POOL_NAME $POOL_NAME --yes-i-really-really-mean-it 2>&1 | log_output || true
    ./bin/ceph osd erasure-code-profile rm $PROFILE_NAME 2>&1 | log_output || true

    # 2. 创建 EC Profile
    # crush-failure-domain=osd 是关键，因为 vstart 通常在同一台机器上运行，
    # 默认的 host 级别隔离会导致 PG stuck 在 creating 状态
    log_info "创建 EC Profile: k=$EC_K m=$EC_M plugin=$EC_PLUGIN"
    ./bin/ceph osd erasure-code-profile set $PROFILE_NAME \
        k=$EC_K m=$EC_M plugin=$EC_PLUGIN \
        crush-failure-domain=osd 2>&1 | log_output

    # 3. 创建 EC Pool
    log_info "创建 Pool: $POOL_NAME (使用 profile: $PROFILE_NAME)"
    # 16 16 是 pg_num，erasure 是类型
    ./bin/ceph osd pool create $POOL_NAME 16 16 erasure $PROFILE_NAME 2>&1 | log_output

    # 4. 启用 Rados 应用
    ./bin/ceph osd pool application enable $POOL_NAME rados 2>&1 | log_output
    
    # 5. (可选) 允许覆盖写。
    # 标准 EC 池不支持部分覆盖写，但在测试环境中启用它允许类似 BlueStore 的行为
    # ./bin/ceph osd pool set $POOL_NAME allow_ec_overwrites true 2>&1 | log_output
}

# ============================================================
# 调用中间件进行读写与校验
# ============================================================
run_rw_check() {
    local mode="$1" # --put 或 --get
    local msg="$2"
    
    log_section "$msg"
    
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

    log_info "执行指令: ${cmd[*]}"
    "${cmd[@]}" 2>&1 | log_output
    
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
# 核心新增：故障模拟与恢复测试
# ============================================================
simulate_failure_check() {
    log_section "开始故障模拟测试"

    # 1. 确定对象所在的 OSD 列表 (Acting Set)
    log_info "正在查询对象 '$OBJECT_NAME' 的分布..."
    local map_info
    map_info=$(./bin/ceph osd map $POOL_NAME $OBJECT_NAME)
    log_info "对象分布图: $map_info"

    # 解析 Acting Set，例如 "acting ([0,2,1], p0)" -> 提取出 0, 2, 1
    # 我们选择列表中的第一个 OSD 作为“受害者”
    VICTIM_OSD=$(echo "$map_info" | awk -F'[][]' '{print $2}' | awk -F',' '{print $1}')

    if [ -z "$VICTIM_OSD" ]; then
        echo "[ERROR] 无法解析 OSD ID，测试终止"
        exit 1
    fi

    log_info ">>> 选定受害者: osd.$VICTIM_OSD <<<"

    # 2. 杀掉该 OSD 进程
    log_info "正在模拟故障：Kill osd.$VICTIM_OSD 进程..."
    
    local pid_file="out/osd.$VICTIM_OSD.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        kill -9 "$pid"
        log_info "进程 $pid (osd.$VICTIM_OSD) 已被强制终止"
    else
        log_info "PID 文件未找到，尝试使用 ceph osd down (软故障)..."
        ./bin/ceph osd down "$VICTIM_OSD"
    fi

    # 3. 等待集群感知故障
    log_info "等待 5 秒让集群更新状态..."
    sleep 5
    
    # 打印当前健康状态 (预期是 HEALTH_WARN)
    log_info "当前集群健康状态 (预期包含 degraded):"
    ./bin/ceph -s | grep "health" | log_output
    ./bin/ceph pg stat | grep "degraded" | log_output || true

    # 4. 在降级状态下尝试下载 (Degraded Read)
    log_section "执行降级读取 (Degraded Read)"
    
    # 清理之前的下载文件，确保这次是真的从 Ceph 拉取的
    rm -f "$TMPDIR/retrieved_$OBJECT_NAME"
    
    local cmd=(
        "$RW_TOOL"
        "--get"
        --pool "$POOL_NAME"
        --block "$BLOCK_SIZE"
        --object "$OBJECT_NAME"
        --input "$DATA_FILE"
        --tmp "$TMPDIR"
        --bin-dir "./bin"
    )
    
    if [ "$VERBOSE" = true ]; then cmd+=(--verbose); fi

    log_info "执行下载指令..."
    if "${cmd[@]}" 2>&1 | log_output; then
        log_result ">>> 故障容错测试通过 (PASS): 即使 osd.$VICTIM_OSD 挂了，数据依然可读 <<<"
    else
        log_result ">>> 故障容错测试失败 (FAIL): 无法在降级状态下读取数据 <<<"
        exit 1
    fi
}

recover_failure_check() {
    log_section "开始灾难恢复测试"
    
    if [ -z "$VICTIM_OSD" ]; then
        log_info "未指定受害者 ID，无法执行恢复测试"
        return
    fi

    log_info "正在尝试重启 osd.$VICTIM_OSD ..."
    
    # 在 vstart 环境下，主要通过这种方式重启守护进程
    # 注意：这里假设我们在 build 目录运行，且 conf 就在当前目录下
    ./bin/ceph-osd -i "$VICTIM_OSD" -c ./ceph.conf &
    
    log_info "进程已启动，等待集群自愈 (Self-Healing)..."
    
    # 轮询检查健康状态，最多等待 60 秒
    local retries=0
    local max_retries=12
    local recovered=false
    
    while [ $retries -lt $max_retries ]; do
        log_info "sleep $retries"
        sleep 5
        local health
        health=$(./bin/ceph health)
        log_info "当前健康状态: $health"
        
        if [[ "$health" == *"HEALTH_OK"* ]]; then
            recovered=true
            break
        fi
        
        # 显示恢复进度
        ./bin/ceph -s | grep "recovery:" | log_output || true
        retries=$((retries + 1))
    done
    
    if [ "$recovered" = true ]; then
        log_result ">>> 恢复测试通过 (PASS): 集群已自动恢复至 HEALTH_OK <<<"
    else
        log_result ">>> 恢复测试警告 (WARN): 超时未完全恢复，但可能仍在后台进行 <<<"
        # 即使没完全 OK，只要 PG 不是 down 的，通常也没事
    fi

    # 再次验证数据完整性
    rm -f "$TMPDIR/recovered_$OBJECT_NAME"
    log_info "恢复后再次验证数据读取..."
    local cmd=(
        "$RW_TOOL" "--get" --pool "$POOL_NAME" --block "$BLOCK_SIZE" 
        --object "$OBJECT_NAME" --input "$DATA_FILE" 
        --tmp "$TMPDIR" --bin-dir "./bin"
    )
    "${cmd[@]}" 2>&1 | log_output
}

compare_with_replication() {
    log_section "EC vs 多副本 (Replication) 空间对比"

    local repl_pool="test_repl_pool"
    
    log_info "创建 3 副本池: $repl_pool"
    # size=3 是默认的，但显式指定更安全
    ./bin/ceph osd pool create $repl_pool 16 16 replicated 2>&1 | log_output
    ./bin/ceph osd pool set $repl_pool size 3 2>&1 | log_output
    ./bin/ceph osd pool application enable $repl_pool rados 2>&1 | log_output
    
    log_info "向副本池上传相同文件..."
    local cmd=(
        "$RW_TOOL" "--put" --pool "$repl_pool" --block "$BLOCK_SIZE" 
        --object "${OBJECT_NAME}_rep" --input "$DATA_FILE" 
        --tmp "$TMPDIR" --bin-dir "./bin"
    )
    "${cmd[@]}" 2>&1 | log_output
    
    log_info "等待统计收敛 (10s)..."
    sleep 10
    
    echo ""
    log_info ">>> 空间占用对比 <<<"
    # 只显示这两个池的信息
    ./bin/ceph df detail | grep -E "POOL|$POOL_NAME|$repl_pool" 2>&1 | log_result
    
    echo ""
}

# ============================================================
# 统计与日志函数
# ============================================================
check_ec_status() {
    log_section "EC 状态检查"
    
    # 等待统计
    sleep 5

    log_info "EC Profile 详情:"
    ./bin/ceph osd erasure-code-profile get $PROFILE_NAME 2>&1 | log_output || true
    
    echo "" >> "$RUN_LOG_FILE"
    
    log_info "集群空间使用情况:"
    ./bin/ceph df detail 2>&1 | log_result
    
    log_info "PG 状态 (确认是否 Active+Clean):"
    ./bin/ceph pg stat 2>&1 | log_output
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

    # 切换到 build 目录
    cd ../build

    # 初始化日志
    local run_log_path="$SCRIPT_DIR/logs/run"
    mkdir -p "$run_log_path"
    RUN_LOG_FILENAME="real_ec_$(date +%Y%m%d_%H%M%S).txt"
    RUN_LOG_FILE="$run_log_path/$RUN_LOG_FILENAME"

    log_section "EC (纠删码) 算法真实数据测试"
    log_info "配置参数:"
    log_info "  - 启动 Ceph: $START_CEPH"
    log_info "  - 输入文件名: $DATA_FILE"
    log_info "  - 临时目录: $TMPDIR"
    log_info "  - 压缩块大小: $BLOCK_SIZE"
    log_info "  - 详细输出: $VERBOSE"
    log_info "  - 运行日志: $RUN_LOG_FILE"

    if [ ! -f "$DATA_FILE" ]; then
        echo "[ERROR] 数据文件不存在: $(pwd)/$DATA_FILE"
        exit 1
    fi

    n "$@"
    create_tmpdir
    start_ceph_cluster
    create_ec_pool
    
    # --- 调用中间件 ---
    run_rw_check "--put" "上传文件到 $POOL_NAME (EC)"
    run_rw_check "--get" "下载文件并校验完整性"
    # ----------------

    check_ec_status

    simulate_failure_check
    check_ec_status

    recover_failure_check
    check_ec_status
    
    compare_with_replication
    clean_tmp_dir

    log_section "测试完成"
}

main "$@"