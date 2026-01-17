#!/bin/bash

# ============================================================
# Ceph Rados 读写与校验中间件
# 用于执行对象存储的上传、下载及完整性比对
# ============================================================

set -e

# 默认参数
DO_PUT=false
DO_GET=false
POOL_NAME="testpool"
BLOCK_SIZE=4194304 # 4MB Default
OBJECT_NAME=""
LOCAL_FILE=""
TMP_DIR="/tmp"
BIN_DIR="./bin" # 默认 rados 命令所在目录
VERBOSE=false

# 帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo "选项:"
    echo "  --put               执行上传操作"
    echo "  --get               执行下载并校验操作"
    echo "  -p, --pool          存储池名称 (默认: testpool)"
    echo "  -b, --block         块大小 (字节)"
    echo "  -o, --object        对象名称 (必需)"
    echo "  -i, --input         本地源文件路径 (必需)"
    echo "  -t, --tmp           下载临时目录 (默认: /tmp)"
    echo "  --bin-dir           Ceph 二进制文件目录 (默认: ./bin)"
    echo "  -v, --verbose       显示详细输出"
    echo "  -h, --help          显示帮助"
}

# 参数解析
while [[ $# -gt 0 ]]; do
    case "$1" in
        --put)
            DO_PUT=true
            shift
            ;;
        --get)
            DO_GET=true
            shift
            ;;
        -p|--pool)
            POOL_NAME="$2"
            shift 2
            ;;
        -b|--block)
            BLOCK_SIZE="$2"
            shift 2
            ;;
        -o|--object)
            OBJECT_NAME="$2"
            shift 2
            ;;
        -i|--input)
            LOCAL_FILE="$2"
            shift 2
            ;;
        -t|--tmp)
            TMP_DIR="$2"
            shift 2
            ;;
        --bin-dir)
            BIN_DIR="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "[ERROR] 未知参数: $1"
            exit 1
            ;;
    esac
done

# 基础检查
if [ -z "$OBJECT_NAME" ] || [ -z "$LOCAL_FILE" ]; then
    echo "[ERROR] 必须指定对象名 (-o) 和本地文件 (-i)"
    exit 1
fi

RADOS_CMD="$BIN_DIR/rados"
if [ ! -x "$RADOS_CMD" ]; then
    echo "[ERROR] 找不到 rados 命令: $RADOS_CMD"
    exit 1
fi

# 日志辅助函数
log() {
    if [ "$VERBOSE" = true ]; then
        echo "[RW-CHECK] $1"
    fi
}

# ============================================================
# 核心逻辑
# ============================================================

# 1. 上传逻辑 (PUT)
if [ "$DO_PUT" = true ]; then
    if [ ! -f "$LOCAL_FILE" ]; then
        echo "[ERROR] 源文件不存在: $LOCAL_FILE"
        exit 1
    fi

    log "正在上传: $LOCAL_FILE -> $POOL_NAME/$OBJECT_NAME (Block: $BLOCK_SIZE)"
    "$RADOS_CMD" -p "$POOL_NAME" -b "$BLOCK_SIZE" put "$OBJECT_NAME" "$LOCAL_FILE"
    log "上传完成"
fi

# 2. 下载与校验逻辑 (GET & VERIFY)
if [ "$DO_GET" = true ]; then
    DOWNLOAD_PATH="$TMP_DIR/retrieved_$OBJECT_NAME"
    
    # 确保临时目录存在
    mkdir -p "$TMP_DIR"

    log "正在下载: $POOL_NAME/$OBJECT_NAME -> $DOWNLOAD_PATH"
    "$RADOS_CMD" -p "$POOL_NAME" -b "$BLOCK_SIZE" get "$OBJECT_NAME" "$DOWNLOAD_PATH"
    
    log "正在校验数据完整性..."
    if diff -q "$LOCAL_FILE" "$DOWNLOAD_PATH" > /dev/null 2>&1; then
        echo "[SUCCESS] 校验通过: $OBJECT_NAME"
        # 校验通过后可选择删除下载的临时文件以节省空间
        # rm -f "$DOWNLOAD_PATH"
    else
        echo "[FAIL] 校验失败: $OBJECT_NAME"
        echo "  源文件: $LOCAL_FILE"
        echo "  下载文件: $DOWNLOAD_PATH"
        exit 1
    fi
fi