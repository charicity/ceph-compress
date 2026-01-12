#!/usr/bin/env python3

import os
import re
import zipfile
import sys

def parse_size_to_bytes(value_str, unit_str):
    """
    将带有单位的大小转换为字节数，用于计算比率。
    """
    units = {
        'B': 1,
        'k': 1024, 'K': 1024, 'KiB': 1024,
        'm': 1024**2, 'M': 1024**2, 'MiB': 1024**2,
        'g': 1024**3, 'G': 1024**3, 'GiB': 1024**3,
        't': 1024**4, 'T': 1024**4, 'TiB': 1024**4
    }
    
    try:
        val = float(value_str)
        mult = units.get(unit_str, 1)
        return val * mult
    except ValueError:
        return 0.0

def analyze_log_content(content, filename):
    """
    解析单个日志文件的内容
    """
    result = {
        "filename": filename,
        "hrac_inner": "N/A",
        "block_size": "N/A",
        "simple_cluster": "No",
        "used_compr": "N/A",
        "under_compr": "N/A",
        "ratio": 0.0,
        "status": "FAIL"
    }

    # 1. 查找 HRAC_INNER
    # 匹配模式: const uint32_t HRAC_INNER = 16384;
    hrac_match = re.search(r'^\s*const\s+uint32_t\s+HRAC_INNER\s*=\s*(\d+);', content, re.MULTILINE)
    if hrac_match:
        result["hrac_inner"] = hrac_match.group(1)

    # 2. 查找 CMD 参数
    # 匹配模式: CMD: ./real_test.sh -b 16777216 -s
    cmd_match = re.search(r'CMD:.*real_test\.sh(.*?)\n', content)
    if cmd_match:
        args = cmd_match.group(1)
        # 查找 -b 参数
        b_match = re.search(r'-b\s+(\d+)', args)
        if b_match:
            result["block_size"] = b_match.group(1)
        
        # 查找 -s 参数
        if '-s' in args:
            result["simple_cluster"] = "Yes"

    # 3. 查找 Pool 统计信息
    # 目标行: testpool ... 468 MiB 720 MiB
    # 逻辑: 找到以 'testpool' 开头的行，取最后4个元素 (数值 单位 数值 单位)
    pool_match = re.search(r'^testpool\s+.*$', content, re.MULTILINE)
    if pool_match:
        line = pool_match.group(0).strip()
        parts = line.split()
        
        # Ceph osd pool stats 的最后两列通常是 USED COMPR 和 UNDER COMPR
        # 格式通常是: ... [Used Val] [Used Unit] [Under Val] [Under Unit]
        if len(parts) >= 4:
            under_unit = parts[-1]
            under_val = parts[-2]
            used_unit = parts[-3]
            used_val = parts[-4]
            
            result["used_compr"] = f"{used_val} {used_unit}"
            result["under_compr"] = f"{under_val} {under_unit}"
            
            used_bytes = parse_size_to_bytes(used_val, used_unit)
            under_bytes = parse_size_to_bytes(under_val, under_unit)
            
            if used_bytes > 0:
                result["ratio"] = under_bytes / used_bytes

    # 4. 检查状态
    if "PASS" in content or "所有文件校验通过" in content:
        if used_bytes == 0:
            result["status"] = "FAILED"
        else:
            result["status"] = "PASS"

    return result

def main():
    log_dir = './logs/result'
    
    if not os.path.exists(log_dir):
        print(f"错误: 找不到目录 {log_dir}")
        return

    results = []

    print(f"正在处理目录 {log_dir} ...")
    
    # 遍历目录中的文件
    try:
        files = [f for f in os.listdir(log_dir) if f.endswith('.txt')]
        files.sort()
        
        if not files:
            print("警告: 目录中没有找到 .txt 文件。")
            return

        for filename in files:
            filepath = os.path.join(log_dir, filename)
            
            # 跳过目录，只处理文件
            if not os.path.isfile(filepath):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    data = analyze_log_content(content, filename)
                    results.append(data)
            except Exception as e:
                print(f"无法读取文件 {filename}: {e}")

    except Exception as e:
        print(f"访问目录出错: {e}")
        return

    # 打印表格
    # 定义列宽
    col_fmt = "{:<25} | {:<10} | {:<12} | {:<8} | {:<12} | {:<12} | {:<8} | {:<6}"
    header = col_fmt.format("Filename", "HRAC_INNER", "Block Size", "Simple(-s)", "Used Compr", "Under Compr", "Ratio", "Status")
    
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for r in results:
        ratio_str = f"{r['ratio']:.3f}" if r['ratio'] > 0 else "0.000"
        print(col_fmt.format(
            r['filename'],
            r['hrac_inner'],
            r['block_size'],
            r['simple_cluster'],
            r['used_compr'],
            r['under_compr'],
            ratio_str,
            r['status']
        ))
    print("-" * len(header))

if __name__ == "__main__":
    main()
