# -*- coding: utf-8 -*-
"""
数据完整性校验与补齐工具

1. 交叉比对 stocks/ vs stocks_5m/ vs stocks_60m/，找出各目录缺失的股票
2. 检查日线数据连续性（对照交易日历）
3. 检查5分钟线数据连续性（每日应有48条bar）
4. 输出缺失报告

Usage:
    python scripts/validate_data.py                    # 完整校验
    python scripts/validate_data.py --quick            # 仅比对股票列表
    python scripts/validate_data.py --fix              # 校验并补齐
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "SimTradeLab" / "data" / "baostock"

# ── A 股过滤 ──
A_SHARE_PREFIXES = {
    "600", "601", "603", "605", "688", "689",  # 上海
    "000", "001", "002", "003", "300", "301",  # 深圳
}
INDEX_CODES = {
    "000300.SS", "000016.SS", "000905.SS", "000001.SS",
    "399001.SZ", "399006.SZ", "000688.SS",
}


def is_a_share(code: str) -> bool:
    prefix = code.split(".")[0][:3]
    return prefix in A_SHARE_PREFIXES and code not in INDEX_CODES


def get_parquet_stems(directory: Path) -> set:
    if not directory.exists():
        return set()
    return {f.stem for f in directory.glob("*.parquet")}


def cross_check():
    """交叉比对各目录的股票列表"""
    print("=" * 60)
    print("1. 交叉比对股票列表")
    print("=" * 60)

    stocks = get_parquet_stems(DATA_DIR / "stocks")
    stocks_5m = get_parquet_stems(DATA_DIR / "stocks_5m")
    stocks_60m = get_parquet_stems(DATA_DIR / "stocks_60m")

    # 过滤 A 股
    stocks = {s for s in stocks if is_a_share(s)}
    stocks_5m = {s for s in stocks_5m if is_a_share(s)}
    stocks_60m = {s for s in stocks_60m if is_a_share(s)}

    print("  日线: %d, 5分钟: %d, 60分钟: %d" % (len(stocks), len(stocks_5m), len(stocks_60m)))
    print()

    # 日线有但5分钟没有
    missing_5m = stocks - stocks_5m
    print("  日线有但5分钟缺失: %d 只" % len(missing_5m))
    if missing_5m and len(missing_5m) <= 20:
        print("    %s" % sorted(missing_5m))
    elif missing_5m:
        print("    前20: %s ..." % sorted(missing_5m)[:20])

    # 5分钟有但日线没有
    missing_daily = stocks_5m - stocks
    print("  5分钟有但日线缺失: %d 只" % len(missing_daily))
    if missing_daily:
        print("    %s" % sorted(missing_daily))

    # 日线有但60分钟没有
    missing_60m = stocks - stocks_60m
    print("  日线有但60分钟缺失: %d 只" % len(missing_60m))
    if missing_60m and len(missing_60m) <= 20:
        print("    %s" % sorted(missing_60m))
    elif missing_60m:
        print("    前20: %s ..." % sorted(missing_60m)[:20])

    print()
    return {
        "missing_5m": missing_5m,
        "missing_daily": missing_daily,
        "missing_60m": missing_60m,
    }


def load_trade_days() -> set:
    """加载交易日历"""
    cal_path = DATA_DIR / "metadata" / "trade_days.parquet"
    if not cal_path.exists():
        print("  警告: 交易日历不存在，使用粗略估算")
        return set()
    df = pd.read_parquet(cal_path)
    return set(pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d"))


def check_daily_continuity(trade_days: set):
    """检查日线数据连续性"""
    print("=" * 60)
    print("2. 日线数据连续性检查")
    print("=" * 60)

    stocks_dir = DATA_DIR / "stocks"
    if not stocks_dir.exists():
        print("  stocks/ 目录不存在")
        return {}

    files = sorted(stocks_dir.glob("*.parquet"))
    total = len(files)
    gaps = {}
    empty_files = []

    for i, fp in enumerate(files):
        if (i + 1) % 500 == 0:
            print("  检查进度: %d/%d ..." % (i + 1, total))

        sym = fp.stem
        if not is_a_share(sym):
            continue

        try:
            df = pd.read_parquet(fp)
        except Exception:
            empty_files.append(sym)
            continue

        if df.empty:
            empty_files.append(sym)
            continue

        if "date" in df.columns:
            dates = set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"))
        elif df.index.name == "date":
            dates = set(pd.to_datetime(df.index).strftime("%Y-%m-%d"))
        else:
            continue

        if not trade_days:
            # 没有交易日历，跳过
            continue

        # 只检查文件实际覆盖范围内的交易日
        if dates:
            min_d = min(dates)
            max_d = max(dates)
            expected = {d for d in trade_days if min_d <= d <= max_d}
            missing = expected - dates
            if missing:
                gaps[sym] = sorted(missing)[:10]  # 最多记录前10个

    print("  检查完成: %d 只" % total)
    print("  空文件: %d 只" % len(empty_files))
    print("  有缺口的: %d 只" % len(gaps))
    if gaps:
        total_gap_days = sum(len(v) for v in gaps.values())
        print("  总缺口天数: %d" % total_gap_days)
        # 显示前5个
        for sym in sorted(gaps.keys())[:5]:
            print("    %s: 缺 %d 天, 例如 %s" % (sym, len(gaps[sym]), gaps[sym][:3]))
    if empty_files:
        print("  空文件列表: %s" % empty_files[:20])

    print()
    return gaps


def check_5min_continuity():
    """检查5分钟线数据连续性"""
    print("=" * 60)
    print("3. 5分钟线数据连续性检查")
    print("=" * 60)

    stocks_5m_dir = DATA_DIR / "stocks_5m"
    if not stocks_5m_dir.exists():
        print("  stocks_5m/ 目录不存在")
        return {}

    files = sorted(stocks_5m_dir.glob("*.parquet"))
    total = len(files)
    incomplete_days = {}
    empty_files = []
    short_date_range = []

    for i, fp in enumerate(files):
        if (i + 1) % 500 == 0:
            print("  检查进度: %d/%d ..." % (i + 1, total))

        sym = fp.stem
        if not is_a_share(sym):
            continue

        try:
            df = pd.read_parquet(fp)
        except Exception:
            empty_files.append(sym)
            continue

        if df.empty:
            empty_files.append(sym)
            continue

        # 重建日期列
        if isinstance(df.index, pd.DatetimeIndex):
            dates = df.index.strftime("%Y-%m-%d")
        elif "datetime" in df.columns:
            dates = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")
        else:
            continue

        df_copy = df.copy()
        df_copy["_date"] = dates
        daily_counts = df_copy.groupby("_date").size()

        # 检查日期范围（应覆盖约 2024-01 到 2026-03）
        unique_dates = sorted(daily_counts.index)
        if unique_dates:
            first = unique_dates[0]
            last = unique_dates[-1]
            # 正常应从 2024 年初开始
            if first > "2024-04-01":
                short_date_range.append((sym, first, last))

        # 每个交易日应有 48 条 bar (9:30-11:30: 24条 + 13:00-15:00: 24条)
        # 也可能 40 条（某些老数据），但不应少于 40
        bad_days = daily_counts[daily_counts < 40]
        if len(bad_days) > 0:
            # 只记录样本
            sample = list(bad_days.items())[:5]
            incomplete_days[sym] = {
                "bad_day_count": len(bad_days),
                "sample": [(d, int(c)) for d, c in sample],
            }

    print("  检查完成: %d 只" % total)
    print("  空文件: %d 只" % len(empty_files))
    print("  日期范围偏短的: %d 只" % len(short_date_range))
    print("  有不完整交易日的: %d 只" % len(incomplete_days))

    if short_date_range:
        print("\n  --- 日期范围偏短 (前10) ---")
        for sym, first, last in sorted(short_date_range, key=lambda x: x[1])[:10]:
            print("    %s: %s ~ %s" % (sym, first, last))

    if incomplete_days:
        total_bad = sum(v["bad_day_count"] for v in incomplete_days.values())
        print("\n  --- 不完整交易日 (前10) ---")
        print("  总计 %d 只股票有不完整日, 共 %d 个不完整交易日" % (len(incomplete_days), total_bad))
        for sym in sorted(incomplete_days.keys())[:10]:
            info = incomplete_days[sym]
            print("    %s: %d 天不完整, 例如 %s" % (
                sym, info["bad_day_count"],
                [(d, c) for d, c in info["sample"][:3]]
            ))

    if empty_files:
        print("  空文件列表: %s" % empty_files[:20])

    print()
    return {"incomplete_days": incomplete_days, "short_date_range": short_date_range}


def main():
    parser = argparse.ArgumentParser(description="数据完整性校验与补齐")
    parser.add_argument("--quick", action="store_true", help="仅比对股票列表")
    parser.add_argument("--fix", action="store_true", help="校验并补齐缺失数据")
    args = parser.parse_args()

    # 1. 交叉比对
    cross_result = cross_check()

    if args.quick:
        return

    # 2. 日线连续性
    trade_days = load_trade_days()
    print("  交易日历: %d 天" % len(trade_days))
    print()
    daily_gaps = check_daily_continuity(trade_days)

    # 3. 5分钟连续性
    min_result = check_5min_continuity()

    # 4. 汇总
    print("=" * 60)
    print("汇总")
    print("=" * 60)
    print("  5分钟线缺失股票: %d 只" % len(cross_result["missing_5m"]))
    print("  60分钟线缺失股票: %d 只" % len(cross_result["missing_60m"]))
    print("  日线有缺口: %d 只" % len(daily_gaps))
    print("  5分钟线日期偏短: %d 只" % len(min_result.get("short_date_range", [])))
    print("  5分钟线不完整交易日: %d 只" % len(min_result.get("incomplete_days", [])))

    # 保存报告
    report_path = DATA_DIR / "validation_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("数据完整性校验报告\n")
        f.write("生成时间: %s\n\n" % pd.Timestamp.now())
        f.write("=== 5分钟线缺失股票 (%d) ===\n" % len(cross_result["missing_5m"]))
        for s in sorted(cross_result["missing_5m"]):
            f.write(s + "\n")
        f.write("\n=== 60分钟线缺失股票 (%d) ===\n" % len(cross_result["missing_60m"]))
        for s in sorted(cross_result["missing_60m"]):
            f.write(s + "\n")

    print("\n  报告已保存: %s" % report_path)


if __name__ == "__main__":
    main()
