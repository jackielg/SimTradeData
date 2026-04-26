# -*- coding: utf-8 -*-
"""
交叉比对日线/5分钟线数据，补齐缺失股票

扫描 data/cn/stocks/ 和 data/cn/stocks_5m/ 目录，
找出仅存在于一侧的股票代码，从 BaoStock 下载缺失数据并导出为 Parquet。

Usage:
    python scripts/fill_data_gaps.py --data-dir data/cn --delay 1.5
    python scripts/fill_data_gaps.py --dry-run           # 仅显示差异，不下载
    python scripts/fill_data_gaps.py --only 5min          # 仅补齐5分钟线缺失
    python scripts/fill_data_gaps.py --only daily         # 仅补齐日线缺失
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from tqdm import tqdm

from simtradedata.fetchers.baostock_5min_fetcher import BaoStock5MinFetcher
from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.utils.code_utils import convert_from_ptrade_code

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            Path(__file__).parent.parent / "data" / "fill_gaps.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# BaoStock 5分钟数据字段（不复权）
MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"

# 限流配置
DEFAULT_DELAY = 1.5          # 每次请求间隔（秒）
MAX_RETRIES = 3              # 单只股票最大重试次数
CONSECUTIVE_FAIL_WAIT = 60   # 连续失败后等待时间（秒）
CONSECUTIVE_FAIL_THRESHOLD = 10


def scan_gaps(data_dir: Path):
    """扫描日线和5分钟线目录，返回各自的缺失列表"""
    stocks_dir = data_dir / "stocks"
    stocks_5m_dir = data_dir / "stocks_5m"

    daily_set = set(f.stem for f in stocks_dir.glob("*.parquet"))
    min5_set = set(f.stem for f in stocks_5m_dir.glob("*.parquet"))

    missing_5m = sorted(daily_set - min5_set)   # 有日线但缺5分钟线
    missing_daily = sorted(min5_set - daily_set)  # 有5分钟线但缺日线

    return daily_set, min5_set, missing_5m, missing_daily


def get_5min_date_range(data_dir: Path, sample_count: int = 10):
    """从已有5分钟parquet中采样，确定日期范围"""
    stocks_5m_dir = data_dir / "stocks_5m"
    files = sorted(stocks_5m_dir.glob("*.parquet"))

    if not files:
        return None, None

    # 采样一些大文件（更可能是活跃股票，数据完整）
    files_by_size = sorted(files, key=lambda f: f.stat().st_size, reverse=True)
    sample = files_by_size[:sample_count]

    min_dt = None
    max_dt = None
    for fp in sample:
        try:
            df = pd.read_parquet(fp)
            if df.empty:
                continue
            idx_min = df.index.min()
            idx_max = df.index.max()
            if min_dt is None or idx_min < min_dt:
                min_dt = idx_min
            if max_dt is None or idx_max > max_dt:
                max_dt = idx_max
        except Exception:
            continue

    return min_dt, max_dt


def get_daily_date_range(data_dir: Path, sample_count: int = 10):
    """从已有日parquet中采样，确定日期范围"""
    stocks_dir = data_dir / "stocks"
    files = sorted(stocks_dir.glob("*.parquet"))

    if not files:
        return None, None

    # 采样大文件
    files_by_size = sorted(files, key=lambda f: f.stat().st_size, reverse=True)
    sample = files_by_size[:sample_count]

    min_dt = None
    max_dt = None
    for fp in sample:
        try:
            df = pd.read_parquet(fp)
            if df.empty:
                continue
            if "date" in df.columns:
                dates = pd.to_datetime(df["date"])
            else:
                dates = pd.to_datetime(df.index)
            d_min = dates.min()
            d_max = dates.max()
            if min_dt is None or d_min < min_dt:
                min_dt = d_min
            if max_dt is None or d_max > max_dt:
                max_dt = d_max
        except Exception:
            continue

    return min_dt, max_dt


def download_5min_for_stocks(symbols: list, data_dir: Path, delay: float):
    """为缺失的股票下载5分钟K线数据并写入parquet"""
    if not symbols:
        logger.info("无5分钟线缺失，跳过")
        return

    stocks_5m_dir = data_dir / "stocks_5m"
    stocks_5m_dir.mkdir(parents=True, exist_ok=True)

    # 确定日期范围：使用已有5分钟数据的范围
    min_dt, max_dt = get_5min_date_range(data_dir)
    if min_dt is None:
        logger.error("无法确定5分钟数据日期范围，请先检查已有数据")
        return

    start_date = min_dt.strftime("%Y-%m-%d")
    end_date = max_dt.strftime("%Y-%m-%d")
    logger.info(f"5分钟数据日期范围: {start_date} ~ {end_date}")
    logger.info(f"待下载: {len(symbols)} 只股票")

    fetcher = BaoStock5MinFetcher(timeout=30)
    success_count = 0
    fail_count = 0
    no_data_count = 0
    consecutive_fails = 0

    try:
        fetcher.login()

        for i, symbol in enumerate(tqdm(symbols, desc="下载5分钟线", ncols=100)):
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = fetcher.fetch_5min_bars(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        adjustflag="3",  # 不复权
                    )

                    if df.empty:
                        logger.debug(f"  {symbol}: 该时间段无5分钟数据")
                        no_data_count += 1
                        break

                    # 确保列名匹配已有parquet格式
                    out_df = df[["open", "high", "low", "close", "volume", "money"]].copy()
                    out_file = stocks_5m_dir / f"{symbol}.parquet"
                    out_df.to_parquet(out_file)

                    logger.info(f"  [{i+1}/{len(symbols)}] {symbol}: "
                                f"{len(out_df)} 条记录 -> {out_file.name}")
                    success_count += 1
                    consecutive_fails = 0
                    break

                except Exception as e:
                    is_network = any(kw in str(e).lower()
                                     for kw in ["timeout", "网络", "连接", "connection"])
                    if attempt < MAX_RETRIES:
                        wait = (2 ** attempt) * delay
                        logger.warning(f"  {symbol}: 重试 {attempt}/{MAX_RETRIES} - {e}")
                        time.sleep(wait)
                    else:
                        logger.error(f"  {symbol}: 下载失败 - {e}")
                        fail_count += 1
                        consecutive_fails += 1

            # 连续失败过多时，长等待
            if consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
                logger.warning(f"连续失败 {consecutive_fails} 次，等待 {CONSECUTIVE_FAIL_WAIT}s...")
                time.sleep(CONSECUTIVE_FAIL_WAIT)
                consecutive_fails = 0

            # 限流：每次请求后等待
            if delay > 0:
                time.sleep(delay)

    finally:
        fetcher.logout()

    logger.info(f"5分钟线下载完成: 成功 {success_count}, 无数据 {no_data_count}, "
                f"失败 {fail_count}")


def download_daily_for_stocks(symbols: list, data_dir: Path, delay: float):
    """为缺失的股票下载日线数据并写入parquet（基础字段）"""
    if not symbols:
        logger.info("无日线缺失，跳过")
        return

    stocks_dir = data_dir / "stocks"
    stocks_dir.mkdir(parents=True, exist_ok=True)

    # 确定日期范围
    min_dt, max_dt = get_daily_date_range(data_dir)
    if min_dt is None:
        logger.error("无法确定日线数据日期范围")
        return

    start_date = min_dt.strftime("%Y-%m-%d")
    end_date = max_dt.strftime("%Y-%m-%d")
    logger.info(f"日线数据日期范围: {start_date} ~ {end_date}")
    logger.info(f"待下载: {len(symbols)} 只股票")

    fetcher = UnifiedDataFetcher()
    success_count = 0
    fail_count = 0
    consecutive_fails = 0

    try:
        fetcher.login()

        for i, symbol in enumerate(tqdm(symbols, desc="下载日线", ncols=100)):
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = fetcher.fetch_unified_daily_data(
                        symbol, start_date, end_date
                    )

                    if df.empty:
                        logger.debug(f"  {symbol}: 无日线数据")
                        break

                    # 转为已有parquet格式
                    out = df.rename(columns={"amount": "money"}).copy()
                    if "date" not in out.columns:
                        out["date"] = out.index
                    keep = [c for c in ["date", "open", "close", "high", "low",
                                        "volume", "money"]
                            if c in out.columns]
                    out = out[keep].reset_index(drop=True)

                    out_file = stocks_dir / f"{symbol}.parquet"
                    out.to_parquet(out_file)

                    logger.info(f"  [{i+1}/{len(symbols)}] {symbol}: "
                                f"{len(out)} 条记录 -> {out_file.name}")
                    success_count += 1
                    consecutive_fails = 0
                    break

                except Exception as e:
                    if attempt < MAX_RETRIES:
                        wait = (2 ** attempt) * delay
                        logger.warning(f"  {symbol}: 重试 {attempt}/{MAX_RETRIES} - {e}")
                        time.sleep(wait)
                    else:
                        logger.error(f"  {symbol}: 下载失败 - {e}")
                        fail_count += 1
                        consecutive_fails += 1

            if consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
                logger.warning(f"连续失败 {consecutive_fails} 次，等待 {CONSECUTIVE_FAIL_WAIT}s...")
                time.sleep(CONSECUTIVE_FAIL_WAIT)
                consecutive_fails = 0

            if delay > 0:
                time.sleep(delay)

    finally:
        fetcher.logout()

    logger.info(f"日线下载完成: 成功 {success_count}, 失败 {fail_count}")


def main():
    parser = argparse.ArgumentParser(
        description="交叉比对日线/5分钟线，补齐缺失数据（不复权）"
    )
    parser.add_argument(
        "--data-dir", type=str, default="data/cn",
        help="数据根目录 (default: data/cn)"
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"请求间隔秒数 (default: {DEFAULT_DELAY})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅显示差异，不下载"
    )
    parser.add_argument(
        "--only", choices=["5min", "daily"], default=None,
        help="仅补齐指定类型 (5min 或 daily)"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"数据目录不存在: {data_dir}")
        sys.exit(1)

    # 扫描差异
    logger.info("=" * 60)
    logger.info("扫描数据目录...")
    daily_set, min5_set, missing_5m, missing_daily = scan_gaps(data_dir)

    print(f"\n日线 stocks/:    {len(daily_set)} 只")
    print(f"5分钟 stocks_5m/: {len(min5_set)} 只")
    print(f"缺5分钟线（仅有日线）: {len(missing_5m)} 只")
    print(f"缺日线（仅有5分钟线）: {len(missing_daily)} 只")

    if missing_5m:
        print(f"\n缺5分钟线前20只: {missing_5m[:20]}")
    if missing_daily:
        print(f"\n缺日线前20只: {missing_daily[:20]}")

    if args.dry_run:
        print("\n[dry-run] 仅显示差异，不下载")
        # 输出完整缺失列表到文件
        report_file = data_dir / "gap_report.txt"
        with open(report_file, "w") as f:
            f.write(f"日线: {len(daily_set)}, 5分钟线: {len(min5_set)}\n")
            f.write(f"\n缺5分钟线 ({len(missing_5m)}):\n")
            f.write("\n".join(missing_5m))
            f.write(f"\n\n缺日线 ({len(missing_daily)}):\n")
            f.write("\n".join(missing_daily))
        print(f"完整报告已保存: {report_file}")
        return

    # 下载补齐
    if args.only is None or args.only == "5min":
        download_5min_for_stocks(missing_5m, data_dir, args.delay)

    if args.only is None or args.only == "daily":
        download_daily_for_stocks(missing_daily, data_dir, args.delay)

    # 再次扫描确认
    _, _, remaining_5m, remaining_daily = scan_gaps(data_dir)
    print(f"\n补齐后状态:")
    print(f"  缺5分钟线: {len(remaining_5m)} 只 (原 {len(missing_5m)})")
    print(f"  缺日线:    {len(remaining_daily)} 只 (原 {len(missing_daily)})")


if __name__ == "__main__":
    main()
