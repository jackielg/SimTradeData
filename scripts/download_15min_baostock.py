# -*- coding: utf-8 -*-
"""
BaoStock 15分钟数据下载工具

下载A股15分钟K线数据并存储到DuckDB，支持导出为Parquet格式

Usage:
    python scripts/download_15min_baostock.py --start-date 2025-01-01 --end-date 2026-04-12
    python scripts/download_15min_baostock.py --stocks 000001.SZ,600000.SS --start-date 2025-01-01
    python scripts/download_15min_baostock.py --all-stocks --start-date 2025-01-01
    python scripts/download_15min_baostock.py --export-only --output-dir data/parquet/stocks_15m
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from simtradedata.fetchers.baostock_5min_fetcher import BaoStock5MinFetcher
from simtradedata.writers.duckdb_writer_5min import DuckDBWriter5Min

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            Path(__file__).parent.parent / "data" / "download_15min.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

DEFAULT_STOCKS = [
    "000001.SZ",
    "600000.SS",
    "000333.SZ",
    "600519.SS",
    "000858.SZ",
    "601318.SS",
    "600036.SS",
    "000002.SZ",
]

MAX_RETRIES = 3
BASE_WAIT_TIME = 2
NETWORK_ERROR_WAIT_MULTIPLIER = 5
CONSECUTIVE_FAILURES_THRESHOLD = 10
LONG_WAIT_TIME = 60


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download 15-minute stock data from BaoStock"
    )

    parser.add_argument(
        "--start-date", type=str, default="2025-01-01", help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--stocks",
        type=str,
        help='Comma-separated list of stock codes (e.g., "000001.SZ,600000.SS")',
    )
    parser.add_argument(
        "--all-stocks",
        action="store_true",
        help="Download all A-share stocks (Caution: Large data volume)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/simtradedata_15min.duckdb",
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay between requests in seconds",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of stocks to process before committing",
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Only export existing data to Parquet",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/parquet/stocks_15m",
        help="Output directory for Parquet files",
    )

    return parser.parse_args()


def download_data(args):
    """Download 15-minute data from BaoStock"""
    logger.info("=" * 60)
    logger.info("开始下载15分钟数据")
    logger.info("=" * 60)
    logger.info(f"开始日期: {args.start_date}")
    logger.info(f"结束日期: {args.end_date}")
    logger.info(f"数据库: {args.db_path}")

    fetcher = BaoStock5MinFetcher(timeout=30)
    writer = DuckDBWriter5Min(args.db_path)

    try:
        if args.all_stocks:
            logger.info("获取全部A股列表...")
            bs_codes = fetcher.get_stock_list()
            stocks = []
            for code in bs_codes:
                if code.startswith("sh."):
                    stocks.append(code[3:] + ".SS")
                elif code.startswith("sz."):
                    stocks.append(code[3:] + ".SZ")
            logger.info(f"共 {len(stocks)} 只股票")
        elif args.stocks:
            stocks = [s.strip() for s in args.stocks.split(",")]
        else:
            stocks = DEFAULT_STOCKS
            logger.info(f"使用默认股票列表: {stocks}")

        total_stocks = len(stocks)
        success_count = 0
        fail_count = 0
        total_records = 0
        retry_count = 0
        consecutive_network_errors = 0

        writer.begin()

        for i, symbol in enumerate(stocks):
            attempts = 0
            max_attempts = MAX_RETRIES

            while attempts < max_attempts:
                try:
                    logger.info(f"[{i+1}/{total_stocks}] 下载 {symbol}...")
                    df = fetcher.fetch_5min_bars(
                        symbol, args.start_date, args.end_date,
                        adjustflag="2", frequency="15"
                    )

                    if df is not None and not df.empty:
                        writer.write_15min_data(symbol, df)
                        success_count += 1
                        total_records += len(df)
                        logger.info(f"  成功: {len(df)} 条记录")
                    else:
                        logger.warning(f"  {symbol}: 无数据")
                        fail_count += 1

                    retry_count = 0
                    break

                except Exception as e:
                    attempts += 1
                    is_network_error = "network" in str(e).lower() or "timeout" in str(e).lower()

                    if is_network_error:
                        consecutive_network_errors += 1
                    else:
                        consecutive_network_errors = 0

                    if attempts < max_attempts:
                        wait_time = BASE_WAIT_TIME * (NETWORK_ERROR_WAIT_MULTIPLIER if is_network_error else 1)
                        logger.warning(f"  {symbol}: 下载失败 (尝试 {attempts}/{max_attempts}), 等待 {wait_time}秒...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"  {symbol}: 下载失败，已达最大重试次数 - {e}")
                        fail_count += 1

                    if retry_count >= CONSECUTIVE_FAILURES_THRESHOLD:
                        logger.warning(f"连续失败次数过多，等待 {LONG_WAIT_TIME} 秒...")
                        time.sleep(LONG_WAIT_TIME)
                        retry_count = 0

            if (i + 1) % args.batch_size == 0:
                writer.commit()
                writer.begin()
                logger.info(f"已提交 {i+1} 只股票的数据")

            if args.delay > 0 and i < total_stocks - 1:
                time.sleep(args.delay)

        writer.commit()

        logger.info("\n" + "=" * 60)
        logger.info("下载完成汇总")
        logger.info("=" * 60)
        logger.info(f"成功: {success_count} 只")
        logger.info(f"失败: {fail_count} 只")
        logger.info(f"总记录数: {total_records} 条")

        stats = writer.get_stats()
        logger.info(f"数据库统计:")
        logger.info(f"  总记录数: {stats['total_records']}")
        logger.info(f"  股票数量: {stats['unique_symbols']}")
        logger.info(f"  时间范围: {stats['min_datetime']} ~ {stats['max_datetime']}")

    finally:
        fetcher.logout()
        writer.close()

    return success_count, fail_count


def export_data(args):
    """Export data to parquet"""
    logger.info("=" * 60)
    logger.info("导出15分钟数据到Parquet")
    logger.info("=" * 60)
    logger.info(f"数据库: {args.db_path}")
    logger.info(f"输出目录: {args.output_dir}")

    writer = DuckDBWriter5Min(args.db_path)

    try:
        count = writer.export_to_parquet(args.output_dir)
        logger.info(f"成功导出 {count} 个文件")
    finally:
        writer.close()

    return count


def main():
    args = parse_args()

    Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).parent.mkdir(parents=True, exist_ok=True)

    if args.export_only:
        export_data(args)
    else:
        success, fail = download_data(args)

        if success > 0:
            logger.info("\n自动导出到Parquet...")
            export_data(args)

        sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
