# -*- coding: utf-8 -*-
"""Wrapper: 运行60m下载 + 每分钟打印文件数进度到stdout"""
import subprocess, time, sys, os
from pathlib import Path

TARGET_DIR = Path(r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeLab\data\cn\stocks_60m")
TOTAL = 5512

def count_files():
    return len(list(TARGET_DIR.glob("*.parquet")))

# 先启动下载子进程（继承stdout，用-u去缓冲）
proc = subprocess.Popen(
    [sys.executable, "-u", 
     r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeData\scripts\download_60min_simple.py"],
    stdout=sys.stdout,
    stderr=sys.stderr,
)

# 同时监控进度
last_count = count_files()
while proc.poll() is None:
    time.sleep(60)
    cur = count_files()
    if cur != last_count:
        pct = cur / TOTAL * 100
        print(f"\n[进度] {cur} / {TOTAL} 只股票 (约 {pct:.0f}%)", flush=True)
        last_count = cur

# 最终汇报
final = count_files()
pct = final / TOTAL * 100
print(f"\n=== 下载进程结束 ===", flush=True)
print(f"[最终] {final} / {TOTAL} 只股票 (约 {pct:.0f}%)", flush=True)
