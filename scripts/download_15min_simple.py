# -*- coding: utf-8 -*-
"""
BaoStock 15分钟数据下载工具 (简化版)

直接使用 baostock API 下载，从已有日线数据目录获取股票列表
"""

import sys
import time
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd
import baostock as bs

BASE_DIR = Path(r"C:\Users\Admin\SynologyDrive\PtradeProjects\SimTradeLab\data\cn")
STOCKS_DIR = BASE_DIR / "stocks"
MIN15_DIR = BASE_DIR / "stocks_15m"

STOCKS_DIR.mkdir(parents=True, exist_ok=True)
MIN15_DIR.mkdir(parents=True, exist_ok=True)

INDEX_CODES = {"000300.SS", "000016.SS", "000905.SS", "000001.SS",
               "399001.SZ", "399006.SZ", "000688.SS"}


def get_stock_codes_from_daily():
    all_files = list(STOCKS_DIR.glob("*.parquet"))
    codes = []
    for f in all_files:
        code = f.stem
        if code not in INDEX_CODES:
            codes.append(code)
    return sorted(codes)


def download_15min_all(stock_codes=None):
    print("\n" + "=" * 60)
    print("下载15分钟K线数据 (全部A股)")
    print("=" * 60)

    if stock_codes is None:
        stock_codes = get_stock_codes_from_daily()

    total = len(stock_codes)
    print(f"共 {total} 只股票")
    print(f"目标目录: {MIN15_DIR}")
    print(f"时间范围: 2025-01-01 ~ 今天")
    print()

    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return {"success": 0, "no_data": 0, "failed": 0}

    print("登录成功")
    end_date = datetime.now().strftime("%Y-%m-%d")

    results = {"success": 0, "no_data": 0, "failed": 0}
    start_time = time.time()
    batch_start = time.time()

    for i, code in enumerate(stock_codes):
        output_path = MIN15_DIR / f"{code}.parquet"

        try:
            if code.endswith(".SZ"):
                bs_code = f"sz.{code[:-3]}"
            elif code.endswith(".SS"):
                bs_code = f"sh.{code[:-3]}"
            else:
                results["failed"] += 1
                continue

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,time,open,high,low,close,volume,amount",
                start_date="2025-01-01",
                end_date=end_date,
                frequency="15",
                adjustflag="2",
            )

            if rs is None:
                results["no_data"] += 1
                continue

            data = []
            while rs.next():
                data.append(rs.get_row_data())

            if len(data) > 10:
                df = pd.DataFrame(data, columns=rs.fields)
                for col in df.columns:
                    if col not in ("date", "time"):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df.to_parquet(output_path, index=False)
                results["success"] += 1
            else:
                results["no_data"] += 1

            time.sleep(0.03)

        except Exception as e:
            results["failed"] += 1
            if results["failed"] <= 5:
                print(f"  失败 {code}: {e}")

        if (i + 1) % 200 == 0:
            elapsed = time.time() - batch_start
            speed = 200 / elapsed * 60 if elapsed > 0 else 0
            eta = (total - i - 1) / speed if speed > 0 else 0
            print(f"  进度: {i+1}/{total} | 成功={results['success']} 无数据={results['no_data']} 失败={results['failed']} | 速度={speed:.0f}只/分钟 | ETA={eta:.0f}分钟")
            batch_start = time.time()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"下载完成!")
    print(f"  成功: {results['success']} 只")
    print(f"  无数据: {results['no_data']} 只")
    print(f"  失败: {results['failed']} 只")
    print(f"  耗时: {total_time:.0f}秒 ({total_time/60:.1f}分钟)")
    print(f"{'='*60}")

    bs.logout()
    return results


def download_missing_indices():
    print("\n" + "=" * 60)
    print("补充缺失的指数日线数据")
    print("=" * 60)

    MISSING = {
        "sz.399001": ("399001.SZ", "深证成指"),
        "sz.399006": ("399006.SZ", "创业板指"),
        "sh.000688": ("000688.SS", "科创50"),
    }

    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return

    print("登录成功")
    end_date = datetime.now().strftime("%Y-%m-%d")

    for bs_code, (pt_code, name) in MISSING.items():
        output_path = STOCKS_DIR / f"{pt_code}.parquet"

        if output_path.exists():
            df = pd.read_parquet(output_path)
            print(f"  跳过 {name} ({pt_code}): 已有 {len(df)} 条")
            continue

        print(f"  下载 {name} ({bs_code} -> {pt_code})...")

        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date="2024-01-01",
                end_date=end_date,
                frequency="d",
                adjustflag="2",
            )

            if rs is None:
                print(f"    无数据 (rs=None)")
                continue

            data = []
            while rs.next():
                data.append(rs.get_row_data())

            if len(data) > 0:
                df = pd.DataFrame(data, columns=rs.fields)
                for col in df.columns:
                    if col != "date":
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df.to_parquet(output_path, index=False)
                sz = output_path.stat().st_size / 1024
                print(f"    成功! {len(data)}条, {sz:.1f}KB")
                print(f"    close范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}")
            else:
                print(f"    无数据")

        except Exception as e:
            print(f"    失败: {e}")

    bs.logout()


def check_data():
    print("\n" + "=" * 60)
    print("数据完整性检查")
    print("=" * 60)

    print("\n[指数日线数据]")
    index_codes = {
        "000300.SS": "沪深300", "000016.SS": "上证50", "000905.SS": "中证500",
        "000001.SS": "上证指数", "399001.SZ": "深证成指", "399006.SZ": "创业板指",
        "000688.SS": "科创50",
    }
    ok = 0
    for code, name in index_codes.items():
        path = STOCKS_DIR / f"{code}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            print(f"  OK {code} ({name}): {len(df)}条")
            ok += 1
        else:
            print(f"  缺失 {code} ({name})")
    print(f"  指数数据: {ok}/7")

    print(f"\n[日线数据]")
    daily_files = list(STOCKS_DIR.glob("*.parquet"))
    print(f"  共 {len(daily_files)} 个文件")

    print(f"\n[15分钟数据]")
    min15_files = list(MIN15_DIR.glob("*.parquet"))
    if min15_files:
        total_size = sum(f.stat().st_size for f in min15_files) / (1024 * 1024)
        print(f"  共 {len(min15_files)} 只股票, {total_size:.1f}MB")
    else:
        print(f"  无15分钟数据")


if __name__ == "__main__":
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    download_missing_indices()
    download_15min_all()
    check_data()

    print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
