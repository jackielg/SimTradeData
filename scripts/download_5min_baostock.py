# -*- coding: utf-8 -*-
"""
BaoStock 5分钟数据下载工具

下载A股5分钟K线数据并存储到DuckDB，支持导出为Parquet格式

Usage:
    python scripts/download_5min_baostock.py --start-date 2025-04-01 --end-date 2025-04-30
    python scripts/download_5min_baostock.py --stocks 000001.SZ,600000.SS --start-date 2025-04-01 --end-date 2025-04-30
    python scripts/download_5min_baostock.py --export-only --output-dir data/parquet/stocks_5m
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data/download_5min.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

DEFAULT_STOCKS = [
    "000001.SZ",  # 平安银行
    "600000.SS",  # 浦发银行
    "000333.SZ",  # 美的集团
    "600519.SS",  # 贵州茅台
    "000858.SZ",  # 五粮液
    "601318.SS",  # 中国平安
    "600036.SS",  # 招商银行
    "000002.SZ",  # 万科A
]


def parse_args():
    parser = argparse.ArgumentParser(description='Download 5-minute stock data from BaoStock')
    
    parser.add_argument(
        '--start-date',
        type=str,
        default='2025-04-01',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='2025-04-30',
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--stocks',
        type=str,
        help='Comma-separated list of stock codes (e.g., "000001.SZ,600000.SS")'
    )
    parser.add_argument(
        '--all-stocks',
        action='store_true',
        help='Download all A-share stocks ( caution: large data volume )'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default='data/simtradedata_5min.duckdb',
        help='DuckDB database path'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between requests (seconds)'
    )
    parser.add_argument(
        '--export-only',
        action='store_true',
        help='Only export existing data to parquet'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/parquet/stocks_5m',
        help='Output directory for parquet files'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of stocks to process before committing'
    )
    
    return parser.parse_args()


def download_data(args):
    """Download 5-minute data"""
    
    # Parse stock list
    if args.stocks:
        stocks = [s.strip() for s in args.stocks.split(',')]
    elif args.all_stocks:
        stocks = None  # Will fetch all stocks
    else:
        stocks = DEFAULT_STOCKS
    
    logger.info("=" * 60)
    logger.info("BaoStock 5分钟数据下载")
    logger.info("=" * 60)
    logger.info(f"时间范围: {args.start_date} ~ {args.end_date}")
    logger.info(f"数据库: {args.db_path}")
    logger.info(f"请求延迟: {args.delay}秒")
    logger.info("=" * 60)
    
    # Initialize fetcher and writer
    fetcher = BaoStock5MinFetcher()
    writer = DuckDBWriter5Min(args.db_path)
    
    try:
        fetcher.login()
        
        # Get stock list if downloading all
        if stocks is None:
            logger.info("获取所有A股列表...")
            # Use a known trading day to get stock list
            bs_stocks = fetcher.get_stock_list(date='2025-04-01')
            # Convert BaoStock format to PTrade format
            stocks = []
            for bs_code in bs_stocks:
                if bs_code.startswith('sh.'):
                    stocks.append(bs_code[3:] + '.SS')
                elif bs_code.startswith('sz.'):
                    stocks.append(bs_code[3:] + '.SZ')
            logger.info(f"共 {len(stocks)} 只股票")
        
        # Download data
        total_stocks = len(stocks)
        success_count = 0
        fail_count = 0
        total_records = 0
        
        writer.begin()
        
        for i, symbol in enumerate(stocks):
            logger.info(f"[{i+1}/{total_stocks}] 下载 {symbol}...")
            
            try:
                # Fetch data
                df = fetcher.fetch_5min_bars(
                    symbol=symbol,
                    start_date=args.start_date,
                    end_date=args.end_date
                )
                
                if df.empty:
                    logger.warning(f"  {symbol}: 无数据")
                    fail_count += 1
                    continue
                
                # Write to database
                records = writer.write_5min_data(symbol, df)
                total_records += records
                
                logger.info(f"  {symbol}: 写入 {records} 条记录")
                success_count += 1
                
                # Commit every batch
                if (i + 1) % args.batch_size == 0:
                    writer.commit()
                    writer.begin()
                    logger.info(f"已提交 {i+1} 只股票的数据")
                
                # Delay to avoid rate limiting
                if args.delay > 0 and i < total_stocks - 1:
                    time.sleep(args.delay)
                    
            except Exception as e:
                logger.error(f"  {symbol}: 下载失败 - {e}")
                fail_count += 1
                continue
        
        writer.commit()
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("下载完成汇总")
        logger.info("=" * 60)
        logger.info(f"成功: {success_count} 只")
        logger.info(f"失败: {fail_count} 只")
        logger.info(f"总记录数: {total_records} 条")
        
        # Show database stats
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
    logger.info("导出5分钟数据到Parquet")
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
    
    # Ensure data directory exists
    Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).parent.mkdir(parents=True, exist_ok=True)
    
    if args.export_only:
        export_data(args)
    else:
        success, fail = download_data(args)
        
        # Auto export after download
        if success > 0:
            logger.info("\n自动导出到Parquet...")
            export_data(args)
        
        sys.exit(0 if fail == 0 else 1)


if __name__ == '__main__':
    main()
