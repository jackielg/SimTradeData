# -*- coding: utf-8 -*-
"""
BaoStock 60分钟数据下载工具

直接使用 baostock API 下载2025年A股全部60分钟(1小时)K线数据，
以parquet格式保存到 SimTradeLab/data/cn/stocks_60m/ 目录。

数据特征:
  - 频率: 60分钟 (frequency="60")
  - 复权: 不复权 (adjustflag="3")，与5分钟数据保持一致
  - 每日约4根K线: 10:30, 11:30, 14:00, 15:00

限流策略 (基于社区最佳实践):
  - 基础延迟: 0.08秒/请求
  - 批量暂停: 每500只暂停3秒
  - 自适应退避: 连续错误时指数退避 (max=5秒)
  - 断线重连: 连续失败10次自动重新登录

Usage:
    python scripts/download_60min_simple.py                    # 下载全部(断点续传)
    python scripts/download_60min_simple.py --check            # 仅检查数据完整性
    python scripts/download_60min_simple.py --codes 000001.SZ,600519.SS  # 指定股票
    python scripts/download_60min_simple.py --force             # 强制覆盖已存在文件
"""

import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import baostock as bs

# ============================================================
# 路径配置
# ============================================================
BASE_DIR = Path(r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeLab\data\cn")
STOCKS_DIR = BASE_DIR / "stocks"
MIN60_DIR = BASE_DIR / "stocks_60m"

STOCKS_DIR.mkdir(parents=True, exist_ok=True)
MIN60_DIR.mkdir(parents=True, exist_ok=True)

# 指数代码（从日线股票列表中排除）
INDEX_CODES = {
    "000300.SS", "000016.SS", "000905.SS", "000001.SS",
    "399001.SZ", "399006.SZ", "000688.SS",
}

# BaoStock 查询字段
BS_FIELDS = "date,time,open,high,low,close,volume,amount"

# 默认时间范围：2025全年至今
DEFAULT_START_DATE = "2025-01-01"

# ============================================================
# 限流参数
# ============================================================
BASE_DELAY = 0.08          # 基础请求间隔 (秒)
BATCH_SIZE = 500           # 每批数量
BATCH_PAUSE = 3.0          # 每批完成后暂停 (秒)
MAX_BACKOFF = 5.0          # 最大退避延迟 (秒)
CONSECUTIVE_FAIL_THRESHOLD = 10  # 连续失败阈值，触发重新登录


def get_stock_codes_from_daily():
    """从日线parquet目录获取所有A股股票代码"""
    all_files = list(STOCKS_DIR.glob("*.parquet"))
    codes = []
    for f in all_files:
        code = f.stem
        if code not in INDEX_CODES:
            codes.append(code)
    return sorted(codes)


def parse_args():
    """解析命令行参数"""
    args = sys.argv[1:]
    opts = {
        "check": False,
        "codes": None,
        "start_date": DEFAULT_START_DATE,
        "end_date": None,
        "resume": True,
    }
    i = 0
    while i < len(args):
        if args[i] == "--check":
            opts["check"] = True
        elif args[i] == "--codes" and i + 1 < len(args):
            opts["codes"] = [c.strip() for c in args[i + 1].split(",")]
            i += 1
        elif args[i] == "--start-date" and i + 1 < len(args):
            opts["start_date"] = args[i + 1]
            i += 1
        elif args[i] == "--end-date" and i + 1 < len(args):
            opts["end_date"] = args[i + 1]
            i += 1
        elif args[i] == "--force":
            opts["resume"] = False
        i += 1
    return opts


def code_to_baostock(code):
    """将 PTrade 格式代码转换为 BaoStock 格式"""
    if code.endswith(".SZ"):
        return f"sz.{code[:-3]}"
    elif code.endswith(".SS"):
        return f"sh.{code[:-3]}"
    else:
        return None


def download_60min_all(stock_codes=None, start_date=DEFAULT_START_DATE,
                       end_date=None, resume=True):
    """
    下载全部A股60分钟K线数据

    Args:
        stock_codes: 股票代码列表，None则从日线目录自动获取
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)，None表示今天
        resume: 是否跳过已有的文件

    Returns:
        dict: 统计结果 {"success", "no_data", "failed", "skipped"}
    """
    print("\n" + "=" * 60)
    print("下载60分钟K线数据 (全部A股, 不复权)")
    print("=" * 60)

    if stock_codes is None:
        stock_codes = get_stock_codes_from_daily()

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    total = len(stock_codes)
    print(f"共 {total} 只股票")
    print(f"目标目录: {MIN60_DIR}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print(f"复权方式: 不复权 (adjustflag=3)")
    print(f"断点续传: {'开启' if resume else '关闭'}")
    print()

    # 登录 BaoStock
    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return {"success": 0, "no_data": 0, "failed": 0, "skipped": 0}

    print("登录成功")
    print(f"  限流配置: 基础延迟={BASE_DELAY}s | 批量暂停={BATCH_PAUSE}s/{BATCH_SIZE}只 | "
          f"最大退避={MAX_BACKOFF}s")
    print()

    results = {"success": 0, "no_data": 0, "failed": 0, "skipped": 0}
    start_time = time.time()
    batch_start = time.time()
    consecutive_fails = 0
    current_delay = BASE_DELAY

    for i, code in enumerate(stock_codes):
        output_path = MIN60_DIR / f"{code}.parquet"

        # 断点续传：跳过已存在的文件
        if resume and output_path.exists():
            results["skipped"] += 1
            continue

        try:
            bs_code = code_to_baostock(code)
            if bs_code is None:
                results["failed"] += 1
                continue

            # 调用BaoStock API，frequency="60" 表示60分钟K线
            rs = bs.query_history_k_data_plus(
                bs_code,
                BS_FIELDS,
                start_date=start_date,
                end_date=end_date,
                frequency="60",       # <-- 60分钟
                adjustflag="3",       # <-- 不复权 (与5m保持一致)
            )

            if rs is None:
                results["no_data"] += 1
                consecutive_fails += 1
                time.sleep(current_delay)
                continue

            # 读取查询结果
            data = []
            while rs.next():
                data.append(rs.get_row_data())

            # 数据量太少视为无意义数据（不足10条）
            if len(data) > 10:
                df = pd.DataFrame(data, columns=rs.fields)
                # 数值列类型转换
                for col in df.columns:
                    if col not in ("date", "time"):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                # 保存为parquet
                df.to_parquet(output_path, index=False)
                results["success"] += 1
                # 成功后重置退避和连续失败计数
                consecutive_fails = 0
                current_delay = BASE_DELAY
            else:
                results["no_data"] += 1
                # 删除可能存在的空文件
                if output_path.exists():
                    output_path.unlink()

            # 控制请求频率 (自适应延迟)
            time.sleep(current_delay)

        except Exception as e:
            results["failed"] += 1
            consecutive_fails += 1
            if results["failed"] <= 5:
                print(f"  失败 {code}: {e}")

            # 自适应退避: 指数增加延迟
            current_delay = min(BASE_DELAY * (2 ** consecutive_fails), MAX_BACKOFF)

            # 连续失败过多时重新登录
            if consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
                print(f"  !! 连续失败{consecutive_fails}次，尝试重新登录...")
                try:
                    bs.logout()
                    time.sleep(2)
                    lg = bs.login()
                    if lg.error_code == "0":
                        print(f"  !! 重新登录成功")
                    else:
                        print(f"  !! 重新登录失败: {lg.error_msg}")
                except Exception:
                    pass
                consecutive_fails = 0
                current_delay = BASE_DELAY
                time.sleep(3)

        # 每 BATCH_SIZE 只暂停一下 + 打印进度
        if (i + 1) % BATCH_SIZE == 0:
            elapsed = time.time() - batch_start
            speed = BATCH_SIZE / elapsed * 60 if elapsed > 0 else 0
            eta = (total - i - 1) / speed if speed > 0 else 0
            done = i + 1
            pct = done / total * 100
            print(
                f"[进度] {done} / {total} 只股票 (约 {pct:.0f}%)\n"
                f"       成功={results['success']} 无数据={results['no_data']} "
                f"失败={results['failed']} 跳过={results['skipped']} | "
                f"速度={speed:.0f}只/分钟 | ETA={eta:.0f}分钟"
            )
            # 批量暂停 - 避免触发IP黑名单
            print(f"  >> 批量暂停 {BATCH_PAUSE}s (防限流)...")
            time.sleep(BATCH_PAUSE)
            batch_start = time.time()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"下载完成!")
    print(f"  成功:   {results['success']} 只")
    print(f"  无数据: {results['no_data']} 只")
    print(f"  失败:   {results['failed']} 只")
    print(f"  跳过(已存在): {results['skipped']} 只")
    print(f"  耗时: {total_time:.0f}秒 ({total_time/60:.1f}分钟)")
    print(f"{'='*60}")

    bs.logout()
    return results


def check_data():
    """检查60分钟数据的完整性"""
    print("\n" + "=" * 60)
    print("60分钟数据完整性检查")
    print("=" * 60)

    min60_files = list(MIN60_DIR.glob("*.parquet"))
    daily_files = list(STOCKS_DIR.glob("*.parquet"))

    daily_codes = {f.stem for f in daily_files if f.stem not in INDEX_CODES}
    min60_codes = {f.stem for f in min60_files}

    missing = sorted(daily_codes - min60_codes)
    extra = sorted(min60_codes - daily_codes)

    print(f"\n日线数据:     {len(daily_codes)} 只股票")
    print(f"60分钟数据:   {len(min60_codes)} 只股票")
    print(f"缺失:         {len(missing)} 只")
    print(f"多余:         {len(extra)} 只")

    if min60_files:
        total_size = sum(f.stat().st_size for f in min60_files) / (1024 * 1024)
        print(f"总大小:       {total_size:.1f} MB")

        # 抽样检查数据质量
        sample_file = min60_files[0]
        df = pd.read_parquet(sample_file)
        print(f"\n抽样: {sample_file.name}")
        print(f"  记录数:   {len(df)}")
        print(f"  字段:     {list(df.columns)}")
        print(f"  时间范围: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
        print(f"  复权:     不复权 (adjustflag=3)")

    if missing:
        print(f"\n缺失股票(前20只): {missing[:20]}")

    return {
        "daily_count": len(daily_codes),
        "min60_count": len(min60_codes),
        "missing_count": len(missing),
        "missing_codes": missing,
    }


def main():
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    opts = parse_args()

    if opts["check"]:
        check_data()
    else:
        download_60min_all(
            stock_codes=opts["codes"],
            start_date=opts["start_date"],
            end_date=opts["end_date"],
            resume=opts["resume"],
        )
        # 下载完成后自动执行检查
        check_data()

    print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
