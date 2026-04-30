# -*- coding: utf-8 -*-
"""
数据交叉检查与补充工具

对 data/cn/ 目录下的全量数据进行系统性完整性检查，发现问题后补充下载。

Usage:
    python scripts/validate_and_supplement_data.py                    # 仅检查（dry-run）
    python scripts/validate_and_supplement_data.py --supplement      # 检查并补充缺失数据
    python scripts/validate_and_supplement_data.py --checks stock_pool,date_range,continuity,5min,fundamentals
    python scripts/validate_and_supplement_data.py --parallel 4
    python scripts/validate_and_supplement_data.py --report-file validation_report.json
"""

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.fetchers.baostock_5min_fetcher import BaoStock5MinFetcher
from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.resilience.baostock_rate_limiter import get_baostock_rate_limiter

# ── 路径 ─────────────────────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data" / "cn"

# ── A 股过滤 ──────────────────────────────────────────────────────────────
A_SHARE_PREFIXES = {
    "600", "601", "603", "605", "688", "689",  # 上海
    "000", "001", "002", "003", "300", "301",  # 深圳
}
INDEX_CODES = {
    *{"%06d.SS" % i for i in range(1, 1000)},
    "000300.SS", "000016.SS", "000905.SS",
    "399001.SZ", "399006.SZ", "399005.SZ", "399300.SZ",
    "000688.SS",
}


def is_a_share(code: str) -> bool:
    if code in INDEX_CODES:
        return False
    prefix = code.split(".")[0][:3]
    return prefix in A_SHARE_PREFIXES


# ── 预期字段 ──────────────────────────────────────────────────────────────
STOCKS_REQUIRED_FIELDS = {"date", "open", "high", "low", "close", "preclose", "volume", "money", "high_limit", "low_limit"}
STOCKS_5M_REQUIRED_FIELDS = {"open", "high", "low", "close", "volume", "money"}
EXRIGHTS_REQUIRED_FIELDS = {"date", "exer_forward_a", "exer_forward_b", "bonus_ps", "dividend", "allotted_ps", "rationed_ps"}
FUNDAMENTALS_REQUIRED_FIELDS = {"code", "publ_date", "end_date"}
VALUATION_REQUIRED_FIELDS = {"date", "pe_ttm", "pb", "ps_ttm", "pcf", "turnover_rate"}

# 预期季度
EXPECTED_QUARTERS = []
for year in range(2023, 2027):
    for q in range(1, 5):
        if year == 2026 and q > 1:
            break
        EXPECTED_QUARTERS.append(f"{year}Q{q}")


# ══════════════════════════════════════════════════════════════════════════
# 检查函数
# ══════════════════════════════════════════════════════════════════════════

def get_parquet_stems(directory: Path) -> set:
    if not directory.exists():
        return set()
    return {f.stem for f in directory.glob("*.parquet")}


def check_stock_pool_consistency() -> dict:
    """交叉比对各目录的股票列表"""
    print("\n" + "=" * 60)
    print("1. 股票池一致性检查")
    print("=" * 60)

    stocks_dir = DATA_DIR / "stocks"
    stocks_5m_dir = DATA_DIR / "stocks_5m"
    fundamentals_dir = DATA_DIR / "fundamentals"
    valuation_dir = DATA_DIR / "valuation"
    exrights_dir = DATA_DIR / "exrights"

    stocks = get_parquet_stems(stocks_dir)
    stocks_5m = get_parquet_stems(stocks_5m_dir)
    fundamentals = get_parquet_stems(fundamentals_dir)
    valuation = get_parquet_stems(valuation_dir)
    exrights = get_parquet_stems(exrights_dir)

    # 过滤 A 股
    stocks_a = {s for s in stocks if is_a_share(s)}
    stocks_5m_a = {s for s in stocks_5m if is_a_share(s)}
    fundamentals_a = {s for s in fundamentals if is_a_share(s)}
    valuation_a = {s for s in valuation if is_a_share(s)}
    exrights_a = {s for s in exrights if is_a_share(s)}

    print(f"  stocks/:        {len(stocks_a)} 只")
    print(f"  stocks_5m/:     {len(stocks_5m_a)} 只")
    print(f"  fundamentals/:  {len(fundamentals_a)} 只")
    print(f"  valuation/:     {len(valuation_a)} 只")
    print(f"  exrights/:      {len(exrights_a)} 只")

    # 基准：stocks_5m 有 5293 只
    reference = stocks_5m_a

    result = {}

    # fundamentals 缺失
    missing_fund = reference - fundamentals_a
    print(f"\n  fundamentals/ 缺失: {len(missing_fund)} 只")
    if missing_fund:
        print(f"    示例: {sorted(missing_fund)[:10]}")
    result["missing_fundamentals"] = sorted(missing_fund)

    # valuation 缺失
    missing_val = reference - valuation_a
    print(f"\n  valuation/ 缺失: {len(missing_val)} 只")
    if missing_val:
        print(f"    示例: {sorted(missing_val)[:10]}")
    result["missing_valuation"] = sorted(missing_val)

    # exrights 多余（正常，只有576只）
    extra_exrights = exrights_a - reference
    print(f"\n  exrights/ 多余（无5分钟数据）: {len(extra_exrights)} 只")

    # fundamentals 多余
    extra_fund = fundamentals_a - reference
    print(f"  fundamentals/ 多余: {len(extra_fund)} 只")
    if extra_fund:
        result["extra_fundamentals"] = sorted(extra_fund)

    # 空文件检测
    empty_fund = []
    for f in fundamentals_dir.glob("*.parquet"):
        if f.stat().st_size < 100:
            empty_fund.append(f.stem)
    print(f"\n  fundamentals/ 空文件（<100B）: {len(empty_fund)} 只")
    if empty_fund:
        result["empty_fundamentals"] = sorted(empty_fund)

    return result


def load_trade_days() -> set:
    """加载交易日历"""
    cal_path = DATA_DIR / "metadata" / "trade_days.parquet"
    if not cal_path.exists():
        print("  警告: 交易日历不存在")
        return set()
    df = pd.read_parquet(cal_path)
    return set(pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d"))


def check_date_range_and_continuity(trade_days: set) -> dict:
    """日线日期范围和连续性检查"""
    print("\n" + "=" * 60)
    print("2. 日线日期范围和连续性检查")
    print("=" * 60)

    stocks_dir = DATA_DIR / "stocks"
    files = sorted(stocks_dir.glob("*.parquet"))
    total = len(files)
    print(f"  待检查文件: {total} 只")

    expected_trade_days_sorted = sorted(trade_days)
    min_expected = min(expected_trade_days_sorted)
    max_expected = max(expected_trade_days_sorted)
    print(f"  预期交易日范围: {min_expected} ~ {max_expected} ({len(trade_days)} 天)")

    # 分批处理避免内存问题
    gaps_by_stock = {}
    outside_range = {}
    empty_files = []

    for i, fp in enumerate(files):
        if (i + 1) % 500 == 0:
            print(f"  检查进度: {i+1}/{total}")

        sym = fp.stem
        if not is_a_share(sym):
            continue

        try:
            # 只读取日期列
            df = pd.read_parquet(fp, columns=["date"])
        except Exception as e:
            empty_files.append((sym, str(e)))
            continue

        if df.empty:
            empty_files.append((sym, "empty"))
            continue

        dates = set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"))
        if not dates:
            empty_files.append((sym, "no dates"))
            continue

        # 检查范围
        first, last = min(dates), max(dates)
        if first > min_expected or last < max_expected:
            outside_range[sym] = {"first": first, "last": last}

        # 检查连续性（与交易日历对比）
        expected = {d for d in trade_days if first <= d <= last}
        missing = expected - dates
        if missing:
            gaps_by_stock[sym] = sorted(missing)

    print(f"\n  检查完成: {total} 只")
    print(f"  超出范围的: {len(outside_range)} 只")
    if outside_range:
        for sym in sorted(outside_range.keys())[:5]:
            info = outside_range[sym]
            print(f"    {sym}: {info['first']} ~ {info['last']}")

    print(f"\n  有交易日缺口的: {len(gaps_by_stock)} 只")
    if gaps_by_stock:
        total_gap_days = sum(len(v) for v in gaps_by_stock.values())
        print(f"  总缺口天数: {total_gap_days}")
        for sym in sorted(gaps_by_stock.keys())[:5]:
            print(f"    {sym}: 缺 {len(gaps_by_stock[sym])} 天, 例如 {gaps_by_stock[sym][:5]}")

    print(f"  空/错文件: {len(empty_files)} 只")
    if empty_files:
        print(f"    示例: {empty_files[:5]}")

    result = {
        "stocks_with_gaps": len(gaps_by_stock),
        "stocks_outside_range": len(outside_range),
        "gap_samples": {k: v[:10] for k, v in list(gaps_by_stock.items())[:10]},
        "outside_range_samples": dict(list(outside_range.items())[:10]),
    }
    return result


def check_5min_integrity(trade_days: set) -> dict:
    """5分钟线完整性检查"""
    print("\n" + "=" * 60)
    print("3. 5分钟线完整性检查")
    print("=" * 60)

    stocks_5m_dir = DATA_DIR / "stocks_5m"
    files = sorted(stocks_5m_dir.glob("*.parquet"))
    total = len(files)
    print(f"  待检查文件: {total} 只")

    expected_trade_days_sorted = sorted(trade_days)
    min_expected = min(expected_trade_days_sorted)
    max_expected = max(expected_trade_days_sorted)

    incomplete_days_count = 0
    short_date_range = []
    empty_files = []
    low_bars_total = 0

    for i, fp in enumerate(files):
        if (i + 1) % 500 == 0:
            print(f"  检查进度: {i+1}/{total}")

        sym = fp.stem
        if not is_a_share(sym):
            continue

        try:
            # 读取datetime列和index
            pf = pd.read_parquet(fp)
        except Exception as e:
            empty_files.append((sym, str(e)))
            continue

        if pf.empty:
            empty_files.append((sym, "empty"))
            continue

        # 从index获取日期
        if isinstance(pf.index, pd.DatetimeIndex):
            dates = pf.index.strftime("%Y-%m-%d")
        elif "datetime" in pf.columns:
            dates = pd.to_datetime(pf["datetime"]).dt.strftime("%Y-%m-%d")
        elif "date" in pf.columns:
            dates = pd.to_datetime(pf["date"]).dt.strftime("%Y-%m-%d")
        else:
            empty_files.append((sym, "no date column"))
            continue

        pf_copy = pf.copy()
        pf_copy["_date"] = dates
        daily_counts = pf_copy.groupby("_date").size()

        # 检查日期范围
        unique_dates = sorted(daily_counts.index)
        if unique_dates:
            first, last = unique_dates[0], unique_dates[-1]
            if first > "2024-01-01":
                short_date_range.append((sym, first, last, len(unique_dates)))

        # 检查每日bar数量（应 >= 40）
        bad_days = daily_counts[daily_counts < 40]
        if len(bad_days) > 0:
            incomplete_days_count += 1
            low_bars_total += len(bad_days)

    print(f"\n  检查完成: {total} 只")
    print(f"  日期范围偏短: {len(short_date_range)} 只")
    if short_date_range:
        for sym, first, last, cnt in sorted(short_date_range, key=lambda x: x[1])[:5]:
            print(f"    {sym}: {first} ~ {last} ({cnt} 个交易日)")

    print(f"\n  有不完整交易日的: {incomplete_days_count} 只")
    print(f"  不完整交易日总数: {low_bars_total}")
    print(f"  空/错文件: {len(empty_files)} 只")
    if empty_files:
        print(f"    示例: {empty_files[:5]}")

    result = {
        "short_date_range_count": len(short_date_range),
        "short_date_range_samples": sorted(short_date_range, key=lambda x: x[1])[:10],
        "incomplete_days_count": incomplete_days_count,
        "low_bars_total": low_bars_total,
    }
    return result


def check_fundamentals_coverage() -> dict:
    """基本面覆盖检查"""
    print("\n" + "=" * 60)
    print("4. 基本面季度覆盖检查")
    print("=" * 60)

    fundamentals_dir = DATA_DIR / "fundamentals"
    files = sorted(fundamentals_dir.glob("*.parquet"))
    total = len(files)
    print(f"  待检查文件: {total} 只")
    print(f"  预期季度: {EXPECTED_QUARTERS}")

    stocks_incomplete = {}
    empty_files = []

    for i, fp in enumerate(files):
        if (i + 1) % 500 == 0:
            print(f"  检查进度: {i+1}/{total}")

        sym = fp.stem
        if not is_a_share(sym):
            continue

        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            empty_files.append((sym, str(e)))
            continue

        if df.empty:
            empty_files.append((sym, "empty"))
            continue

        if "end_date" not in df.columns:
            empty_files.append((sym, "no end_date"))
            continue

        # 提取季度
        df_dates = pd.to_datetime(df["end_date"])
        quarters = set()
        for dt in df_dates:
            y, q = dt.year, (dt.month - 1) // 3 + 1
            quarters.add(f"{y}Q{q}")

        missing_q = [q for q in EXPECTED_QUARTERS if q not in quarters]
        if missing_q:
            stocks_incomplete[sym] = {"found": sorted(quarters), "missing": missing_q}

    print(f"\n  检查完成: {total} 只")
    print(f"  季度不完整的: {len(stocks_incomplete)} 只")
    if stocks_incomplete:
        # 统计各季度缺失情况
        q_missing_count = Counter()
        for info in stocks_incomplete.values():
            for q in info["missing"]:
                q_missing_count[q] += 1
        print(f"\n  各季度缺失统计:")
        for q in EXPECTED_QUARTERS:
            if q_missing_count.get(q, 0) > 0:
                print(f"    {q}: {q_missing_count[q]} 只股票缺失")

        print(f"\n  示例（前10只）:")
        for sym in sorted(stocks_incomplete.keys())[:10]:
            info = stocks_incomplete[sym]
            print(f"    {sym}: 缺 {info['missing']}, 有 {info['found']}")

    print(f"\n  空/错文件: {len(empty_files)} 只")
    if empty_files:
        print(f"    示例: {empty_files[:5]}")

    result = {
        "incomplete_count": len(stocks_incomplete),
        "quarter_missing_stats": dict(q_missing_count),
        "samples": dict(list(stocks_incomplete.items())[:20]),
        "empty_files": empty_files[:20],
    }
    return result


def check_field_completeness() -> dict:
    """字段完整性检查"""
    print("\n" + "=" * 60)
    print("5. 字段完整性检查")
    print("=" * 60)

    checks = [
        ("stocks", DATA_DIR / "stocks", STOCKS_REQUIRED_FIELDS, ["date"]),
        ("stocks_5m", DATA_DIR / "stocks_5m", STOCKS_5M_REQUIRED_FIELDS, []),
        ("exrights", DATA_DIR / "exrights", EXRIGHTS_REQUIRED_FIELDS, ["date"]),
        ("fundamentals", DATA_DIR / "fundamentals", FUNDAMENTALS_REQUIRED_FIELDS, ["end_date"]),
        ("valuation", DATA_DIR / "valuation", VALUATION_REQUIRED_FIELDS, ["date"]),
    ]

    results = {}
    for name, dir_path, required_fields, date_cols in checks:
        print(f"\n  {name}/:")
        if not dir_path.exists():
            print(f"    目录不存在")
            results[name] = {"error": "directory not found"}
            continue

        files = list(dir_path.glob("*.parquet"))
        missing_fields = {}
        empty_files = []

        for fp in files:
            sym = fp.stem
            try:
                df_temp = pd.read_parquet(fp)
                cols = set(df_temp.columns)
                if isinstance(df_temp.index, pd.DatetimeIndex):
                    cols.add("(datetime_index)")
                missing = required_fields - cols
                if missing:
                    missing_fields[sym] = sorted(missing)
            except Exception as e:
                empty_files.append((sym, str(e)))

        print(f"    总文件: {len(files)}")
        print(f"    缺失字段: {len(missing_fields)} 只")
        print(f"    空/错文件: {len(empty_files)} 只")
        if missing_fields:
            print(f"    示例: {list(missing_fields.items())[:3]}")

        results[name] = {
            "missing_fields": missing_fields,
            "empty_files": empty_files[:20],
        }

    return results


# ══════════════════════════════════════════════════════════════════════════
# 补充下载函数
# ══════════════════════════════════════════════════════════════════════════

def supplement_missing_data(report: dict, dry_run: bool = True):
    """根据检查结果补充缺失数据"""
    print("\n" + "=" * 60)
    print("补充下载")
    print("=" * 60)

    if dry_run:
        print("  [DRY-RUN] 仅报告，不实际下载")
    else:
        print("  [LIVE] 实际下载数据")

    # 初始化 fetchers
    if not dry_run:
        print("\n  初始化 BaoStock 连接...")
        standard = UnifiedDataFetcher()
        min_fetcher = BaoStock5MinFetcher(timeout=60)
        standard.login()
        min_fetcher.login()
        rl = get_baostock_rate_limiter()
        print("  连接成功")

    # 1. fundamentals 缺失的股票
    missing_fund = report.get("stock_pool", {}).get("missing_fundamentals", [])
    if missing_fund:
        print(f"\n  fundamentals 缺失: {len(missing_fund)} 只")
        if not dry_run:
            for sym in missing_fund[:20]:
                print(f"    下载 {sym} 基本面...")
                for year in range(2023, 2027):
                    for q in range(1, 5):
                        if year == 2026 and q > 1:
                            break
                        try:
                            df = standard.fetch_quarterly_fundamentals(sym, year, q)
                            if df is not None and not df.empty:
                                fp = DATA_DIR / "fundamentals" / f"{sym}.parquet"
                                if fp.exists():
                                    existing = pd.read_parquet(fp)
                                    merged = pd.concat([existing, df], ignore_index=True).drop_duplicates()
                                else:
                                    merged = df
                                merged.to_parquet(fp, index=False)
                        except Exception as e:
                            print(f"      {sym} {year}Q{q} 失败: {e}")
                        rl.wait()

    # 2. valuation 缺失的股票
    missing_val = report.get("stock_pool", {}).get("missing_valuation", [])
    if missing_val:
        print(f"\n  valuation 缺失: {len(missing_val)} 只")
        if not dry_run:
            print("    (valuation 与 stocks 同源日线数据，缺失通常可忽略或重新下载 Phase 1)")

    # 3. fundamentals 空文件
    empty_fund = report.get("stock_pool", {}).get("empty_fundamentals", [])
    if empty_fund:
        print(f"\n  fundamentals 空文件: {len(empty_fund)} 只")
        if not dry_run:
            # 重新下载这些文件
            for sym in empty_fund[:20]:
                print(f"    重新下载 {sym} 基本面...")
                dfs = []
                for year in range(2023, 2027):
                    for q in range(1, 5):
                        if year == 2026 and q > 1:
                            break
                        try:
                            df = standard.fetch_quarterly_fundamentals(sym, year, q)
                            if df is not None and not df.empty:
                                dfs.append(df)
                        except:
                            pass
                        rl.wait()
                if dfs:
                    merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
                    fp = DATA_DIR / "fundamentals" / f"{sym}.parquet"
                    merged.to_parquet(fp, index=False)

    # 4. 基本面季度缺失
    fund_coverage = report.get("fundamentals_coverage", {})
    incomplete = fund_coverage.get("samples", {})
    if incomplete:
        print(f"\n  基本面季度缺失: {fund_coverage.get('incomplete_count', 0)} 只")
        if not dry_run:
            for sym, info in list(incomplete.items())[:30]:
                missing_q = info["missing"]
                print(f"    {sym}: 补充 {missing_q}")
                dfs = []
                for q_str in missing_q:
                    year, q = int(q_str[:4]), int(q_str[-1])
                    try:
                        df = standard.fetch_quarterly_fundamentals(sym, year, q)
                        if df is not None and not df.empty:
                            dfs.append(df)
                    except:
                        pass
                    rl.wait()
                if dfs:
                    fp = DATA_DIR / "fundamentals" / f"{sym}.parquet"
                    if fp.exists():
                        existing = pd.read_parquet(fp)
                        merged = pd.concat([existing] + dfs, ignore_index=True).drop_duplicates()
                    else:
                        merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
                    merged.to_parquet(fp, index=False)

    if not dry_run:
        standard.logout()
        min_fetcher.logout()
        print("\n  补充完成")


# ══════════════════════════════════════════════════════════════════════════
# 主函数
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="数据交叉检查与补充工具")
    parser.add_argument("--data-dir", type=str,
                        default=str(PROJECT_ROOT / "data" / "cn"),
                        help="数据目录路径")
    parser.add_argument("--checks", type=str,
                        default="all",
                        help="检查项: stock_pool,date_range,continuity,5min,fundamentals,fields 或 all")
    parser.add_argument("--supplement", action="store_true",
                        help="执行补充下载（默认仅dry-run报告）")
    parser.add_argument("--parallel", type=int, default=1,
                        help="并行worker数")
    parser.add_argument("--report-file", type=str, default="",
                        help="输出报告文件路径")
    args = parser.parse_args()

    global DATA_DIR
    DATA_DIR = Path(args.data_dir)

    print("=" * 60)
    print("数据交叉检查与补充")
    print("=" * 60)
    print(f"数据目录: {DATA_DIR}")
    print(f"时间: {datetime.now()}")

    # 加载交易日历
    trade_days = load_trade_days()
    print(f"交易日历: {len(trade_days)} 天")

    report = {}

    # 执行各项检查
    if args.checks in ("all", "stock_pool"):
        result = check_stock_pool_consistency()
        report["stock_pool"] = result

    if args.checks in ("all", "date_range", "continuity"):
        result = check_date_range_and_continuity(trade_days)
        report["date_range_continuity"] = result

    if args.checks in ("all", "5min"):
        result = check_5min_integrity(trade_days)
        report["five_min_integrity"] = result

    if args.checks in ("all", "fundamentals"):
        result = check_fundamentals_coverage()
        report["fundamentals_coverage"] = result

    if args.checks in ("all", "fields"):
        result = check_field_completeness()
        report["field_completeness"] = result

    # 汇总
    print("\n" + "=" * 60)
    print("检查汇总")
    print("=" * 60)
    print(f"  股票池 - fundamentals缺失: {len(report.get('stock_pool', {}).get('missing_fundamentals', []))}")
    print(f"  股票池 - valuation缺失: {len(report.get('stock_pool', {}).get('missing_valuation', []))}")
    print(f"  日期连续性 - 有缺口: {report.get('date_range_continuity', {}).get('stocks_with_gaps', '?')}")
    print(f"  5分钟线 - 不完整: {report.get('five_min_integrity', {}).get('incomplete_days_count', '?')}")
    print(f"  基本面 - 季度不完整: {report.get('fundamentals_coverage', {}).get('incomplete_count', '?')}")

    # 保存报告
    if args.report_file:
        with open(args.report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n报告已保存: {args.report_file}")

    # 补充数据
    if args.supplement:
        supplement_missing_data(report, dry_run=False)

    print("\n检查完成!")


if __name__ == "__main__":
    main()
